#!/usr/bin/env python3
"""BitJita データを取得して GeoJSON を出力するコア生成ロジック。

このモジュールは BitJita API から取得したエンパイア／塔データを受け取り、
チャンク単位の所有マップを作成し、Shapely を用いて所有領域のマージ、
隣接グラフ構築、貪欲着色を行い GeoJSON に変換する責務を持ちます。

主な役割:
- HTTP クライアントと簡易トークンバケット `RateLimiter` による API 取得
- 小座標 (SmallHexTile) -> チャンク変換、チャンク四隅の算出
- 各塔の 5x5 チャンク占有範囲の集計（watchtower extent）
- Shapely によるチャンクポリゴンの結合と隣接性判定
- 永続化された色ストア（YAML）を用いた貪欲着色と GeoJSON 生成

依存・前提:
- Python 3.10+
- `requests` — BitJita API 呼び出し
- `pyyaml` — 色ストアの読み書き（`scripts/color_store.py` を使用）
- `shapely` — ポリゴンのマージ・隣接判定（起動時に存在チェックを行います）

設計ノート:
- CLI は `scripts/generate_geojson.py` にあり、当該モジュールはロジックを提供します。
- 再利用性を高めるため、座標変換と色ストア I/O はそれぞれ
    `scripts/coords.py` / `scripts/color_store.py` に分割しています。

使用例:
    uv run generate

"""
from __future__ import annotations

import time
import threading
import random
from collections import defaultdict

try:
    from . import color_store as _color_store
    from . import coords as _coords
except Exception:
    import color_store as _color_store
    import coords as _coords
from typing import Any, Dict, List, Set, Tuple

import requests

try:
    from shapely.geometry import mapping as shapely_mapping
    from shapely.ops import unary_union
    HAS_SHAPELY = True
except Exception:
    shapely_mapping = None
    unary_union = None
    HAS_SHAPELY = False

BASE_URL = "https://bitjita.com"
DEFAULT_USER_AGENT = "Map_With_Empire (discord: hu_ja_ja_)"

DEFAULT_EMPIRE_COLOR = "#FF5500ff"
CONTESTED_COLOR = "#2d2d2d"
CONTESTED_FILL_OPACITY = 0.5
OWNER_FILL_OPACITY = 0.4

class RateLimiter:
    """トークンバケット方式の簡易レートリミッタ。

    rate_per_min: 分あたり許可するリクエスト数（最小値 1）
    capacity: バケットの最大トークン数（None の場合はデフォルト）

    acquire() を呼ぶとトークンが利用可能になるまでブロックします。
    これにより外部 API へ過負荷をかけないようにします。
    """
    def __init__(self, rate_per_min: int = 250, capacity: int | None = None):
        self.rate_per_min = max(1, int(rate_per_min))
        self.rate_per_sec = self.rate_per_min / 60.0
        default_capacity = min(self.rate_per_min, 10)
        self.capacity = capacity if capacity is not None else default_capacity
        self._tokens = 0.0
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._last = now
                return
            needed = tokens - self._tokens
            wait = needed / self.rate_per_sec
        time.sleep(wait + 0.01)


def _get_with_retries(session: requests.Session, url: str, limiter: RateLimiter, headers: dict, timeout: float = 10.0, max_retries: int = 4):
    """HTTP GET を行い、リトライ/バックオフとレート制御を行うヘルパー。

    - limiter.acquire() で事前にレート制御を行う
    - 例外/5xx/429 レスポンス時は指数バックオフでリトライする
    - 成功時は Response を返し、最終的に失敗した場合は例外を発生させる
    """
    backoff_base = 0.5
    for attempt in range(1, max_retries + 1):
        limiter.acquire()
        try:
            response = session.get(url, headers=headers, timeout=timeout)
        except requests.RequestException:
            if attempt == max_retries:
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        if response.status_code == 429:
            retry_after_header = response.headers.get("Retry-After")
            try:
                wait = float(retry_after_header) if retry_after_header is not None else (backoff_base * (2 ** (attempt - 1)))
            except Exception:
                wait = backoff_base * (2 ** (attempt - 1))
            time.sleep(wait + random.random() * 0.2)
            if attempt == max_retries:
                response.raise_for_status()
            continue

        if 500 <= response.status_code < 600:
            if attempt == max_retries:
                response.raise_for_status()
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        response.raise_for_status()
        return response


class BitJitaClient:
    def __init__(self, session: requests.Session, limiter: RateLimiter, user_agent: str):
        self.session = session
        self.limiter = limiter
        self.user_agent = user_agent

    def fetch_empires(self) -> List[dict]:
        url = f"{BASE_URL}/api/empires"
        try:
            response = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if response is None:
                return []
            return response.json().get("empires", [])
        except Exception:
            return []

    def fetch_towers(self, empire_id: int) -> List[dict]:
        url = f"{BASE_URL}/api/empires/{empire_id}/towers"
        try:
            response = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if response is None:
                return []
            return response.json()
        except Exception:
            return []

    def fetch_empire(self, empire_id: int) -> dict:
        """Fetch detailed empire info from /api/empires/{id}.

        Returns the inner empire object when the API returns a wrapper, or an
        empty dict on error.
        """
        url = f"{BASE_URL}/api/empires/{empire_id}"
        try:
            response = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if response is None:
                return {}
            data = response.json()
            if isinstance(data, dict) and data.get("empire") is not None:
                return data.get("empire") or {}
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}

    def fetch_claim(self, claim_id: int) -> dict:
        """Fetch a single claim by id from /api/claims/{id}.

        Returns claim dict or empty dict on error.
        """
        url = f"{BASE_URL}/api/claims/{claim_id}"
        try:
            response = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if response is None:
                return {}
            data = response.json()
            # API may return the claim object directly or wrapped; try to normalize
            if isinstance(data, dict) and data.get("claim") is not None:
                return data.get("claim") or {}
            if isinstance(data, dict) and data.get("entityId") is not None:
                return data
            return {}
        except Exception:
            return {}

    def fetch_claims_page(self, sort: str = "tier", limit: int = 100, page: int = 1) -> List[dict]:
        """Fetch a page of claims from /api/claims with sorting/paging.

        Returns list of claim dicts (may be empty on error).
        """
        url = f"{BASE_URL}/api/claims?sort={sort}&limit={limit}&page={page}"
        try:
            response = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if response is None:
                return []
            data = response.json()
            if isinstance(data, dict) and data.get("claims") is not None:
                return data.get("claims") or []
            if isinstance(data, list):
                return data
            return []
        except Exception:
            return []


smallhex_to_chunk = _coords.smallhex_to_chunk
chunk_bounds = _coords.chunk_bounds
_coords_to_feature_polygon = _coords.coords_to_feature_polygon


def build_features_from_chunkmap(chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]]) -> List[dict]:
    """chunkmap を受け取り、チャンク単位の GeoJSON Feature リストを返す。

    入力:
    chunkmap: {(chunk_x,chunk_y): set((eid, name), ...), ...}

        出力:
            GeoJSON Feature のリスト。各 Feature はチャンクの四角形ポリゴンで、
            所有者が複数なら 'Contested' 表示、単一なら COLOR_PALETTE から色を選ぶ。

    注意点:
      - chunkmap のキーは任意の整数チャンク座標を許す（負値も理論上は可能）
      - 所有者セットが空のチャンクは無視する
    """
    features: List[dict] = []
    for (chunk_x, chunk_y), owners in sorted(chunkmap.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        coords = chunk_bounds(chunk_x, chunk_y)
        coords_list = _coords_to_feature_polygon(coords)
        if len(owners) == 0:
            continue
        if len(owners) > 1:
            owner_names = ", ".join(sorted((n for (_, n) in owners)))
            props = {"popupText": f"Contested: {owner_names}", "color": CONTESTED_COLOR, "fillColor": CONTESTED_COLOR, "fillOpacity": CONTESTED_FILL_OPACITY}
        else:
            empire_id, empire_name = sorted(owners)[0]
            color = DEFAULT_EMPIRE_COLOR
            props = {"popupText": empire_name, "color": color, "fillColor": color, "fillOpacity": OWNER_FILL_OPACITY}
        feature = {"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": coords_list}}
        features.append(feature)
    return features


def process_empires_to_chunkmap(emps_to_process: List[Tuple[int, str]], client: BitJitaClient, args, throttle: float, log) -> Tuple[Dict[Tuple[int, int], Set[Tuple[int, str]]], List[dict]]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    """指定されたエンパイア一覧を処理してチャンクマップを構築する。

    入力:
      emps_to_process: [(eid, name), ...] — 処理対象のエンパイア一覧
      client: BitJitaClient — API 呼び出し用クライアント
      args: argparse.Namespace — CLI 引数（workers, max_towers_per_empire などを参照）
      throttle: float — 各エンパイア処理後に待機する秒数
      log: callable — ログ出力関数

    出力:
      (chunkmap, siege_points)
    - chunkmap: {(chunk_x,chunk_y): set((eid,name), ...)} — チャンクごとの所有者集合
      - siege_points: 将来的な利用を想定した位置情報リスト（現状は未使用）

    挙動:
      - 並列に各エンパイアの塔を取得し、各塔が占有する 5x5 チャンク範囲を chunkmap に追加する
      - towers の 'active' が False のものは無視する
      - locationX / locationZ が無い、あるいは範囲外 (<=0 または >23040) は無視する
      - max_features / max_towers_per_empire が設定されている場合は早期停止する
    """
    chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]] = defaultdict(set)
    siege_points: List[dict] = []

    def fetch_emp(emp: Tuple[int, str]):
        empire_id, empire_name = emp
        try:
            if getattr(args, "verbose", False):
                log(f"Fetching towers for empire {empire_id} ({empire_name})")
            fetch_start = time.perf_counter()
            towers = client.fetch_towers(empire_id)
            fetch_end = time.perf_counter()
            if getattr(args, "verbose", False):
                log(f"Fetched {len(towers)} towers for {empire_id} in {fetch_end - fetch_start:.2f}s")
        except Exception as exc:
            log(f"Failed to fetch towers for {empire_id}: {exc}")
            towers = []

        local_chunks: List[Tuple[int, int]] = []
        local_siege: List[dict] = []
        towers_handled = 0
        for tower in towers:
            if args.max_towers_per_empire > 0 and towers_handled >= args.max_towers_per_empire:
                break
            if not tower.get("active", True):
                continue
            location_x = tower.get("locationX")
            location_y = tower.get("locationZ")
            if location_x is None or location_y is None:
                continue
            try:
                if location_x <= 0 or location_y <= 0 or location_x > 23040 or location_y > 23040:
                    continue
            except Exception:
                continue
            try:
                small_x = int(location_x)
                small_y = int(location_y)
            except Exception:
                continue
            for (cx, cy) in _coords.tower_covered_chunks(small_x, small_y, radius_chunks=2):
                local_chunks.append((cx, cy))
            towers_handled += 1

        return (empire_id, empire_name, local_chunks, local_siege, towers_handled)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_emp, emp): emp for emp in emps_to_process}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as exc:
                log(f"Worker failed: {exc}")
                continue
            if res:
                results.append(res)
            time.sleep(throttle)

    for empire_id, empire_name, local_chunks, local_siege, towers_handled in sorted(results, key=lambda r: (r[0], r[1])):
        for cx, cy in sorted(set(local_chunks), key=lambda c: (c[0], c[1])):
            chunkmap[(cx, cy)].add((empire_id, empire_name))
        siege_points.extend(local_siege)
        if getattr(args, "verbose", False):
            try:
                log(f"Processed empire {empire_id} ({empire_name}), towers handled: {towers_handled}")
            except Exception:
                pass

    return chunkmap, siege_points


def build_owner_and_contested_polys(chunkmap, log):
    """chunkmap から Shapely ポリゴンを作り、所有者ごとと競合ポリゴンに分類して返す。

    出力:
      - owner_polys: {(eid,name): [Polygon, ...]}
      - contested_polys: [Polygon, ...]

    注意:
      - Shapely が利用不可な場合は空を返す
      - chunk_bounds の順序 (四隅) をそのまま Polygon に渡している
    """
    owner_polys: Dict[Tuple[int, str], List] = defaultdict(list)
    contested_polys: List = []
    if not HAS_SHAPELY:
        return owner_polys, contested_polys
    from shapely.geometry import Polygon as _Polygon
    for (chunk_x, chunk_y), owners in sorted(chunkmap.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        coords = chunk_bounds(chunk_x, chunk_y)
        try:
            polygon = _Polygon(coords)
        except Exception:
            continue
        if len(owners) == 1:
            empire_id, empire_name = sorted(owners)[0]
            owner_polys[(empire_id, empire_name)].append(polygon)
        elif len(owners) > 1:
            contested_polys.append(polygon)
    return owner_polys, contested_polys


def merge_owner_geometries(owner_polys, log):
    merged_owner_geoms: Dict[Tuple[int, str], Any] = {}
    if not HAS_SHAPELY:
        return merged_owner_geoms
    t_merge_start = time.perf_counter()
    for (empire_id, empire_name), polys in owner_polys.items():
        try:
            from shapely.ops import unary_union as _unary_union
        except Exception:
            _unary_union = None
        if _unary_union is None:
            merged = None
        else:
            try:
                merged = _unary_union(polys)
            except Exception:
                merged = None
        if merged is not None:
            merged_owner_geoms[(empire_id, empire_name)] = merged
    t_merge_end = time.perf_counter()
    log(f"Merged owner polygons: {len(merged_owner_geoms)} owners (took {t_merge_end - t_merge_start:.2f}s)")
    return merged_owner_geoms


def apply_colors_from_store(nodes, log, verbose: bool, color_store_path: str | None = None):
    """YAMLストアから色を適用する。ストアにない場合はデフォルト色を使用する。

    - 既存の色があればそれを優先して使う。
    - ストアにない場合はデフォルト色(#FF5500ff)を使用し、ストアに保存する。
    """
    if color_store_path:
        try:
            store = _color_store.load_color_store(color_store_path)
            log(f"Loaded color store from {color_store_path} ({len(store)} entries)")
        except Exception as exc:
            log(f"Failed to load color store {color_store_path}: {exc}; continuing with empty store")
            store = {}
    else:
        store = {}

    assigned_color: Dict[Tuple[int, str], str] = {}
    default_color = DEFAULT_EMPIRE_COLOR
    store_updated = False

    for node_key in nodes:
        empire_id, empire_name = node_key
        try:
            eid_key = int(empire_id)
        except Exception:
            eid_key = empire_id

        persisted = store.get(eid_key)
        color_val = persisted.get("color") if persisted else None
        if color_val:
            assigned_color[node_key] = color_val
            # 名前情報の更新
            if persisted and persisted.get("name") != empire_name:
                persisted["name"] = empire_name
                store_updated = True
            if verbose:
                log(f"Using stored color for {node_key}: {color_val}")
        else:
            # ストアにない、または色が未定義の場合
            assigned_color[node_key] = default_color
            if verbose:
                log(f"Using default color for {node_key}: {default_color}")

            # ストアに新規追加
            if persisted is None:
                store[eid_key] = {"name": empire_name, "color": default_color}
                store_updated = True
            elif not persisted.get("color"):
                persisted["color"] = default_color
                persisted["name"] = empire_name
                store_updated = True

    if color_store_path and store_updated:
        try:
            _color_store.save_color_store(color_store_path, store)
            log(f"Updated color store to {color_store_path} ({len(store)} entries)")
        except Exception as exc:
            log(f"Failed to save color store {color_store_path}: {exc}")

    return assigned_color


def emit_owner_features(merged_owner_geoms, assigned_color, empire_info: dict | None = None, claims_map: dict | None = None):
    """マージ済みジオメトリと色割り当てから GeoJSON Feature を生成する。

    入力:
      merged_owner_geoms: {(eid,name): shapely_geom}
      assigned_color: {(eid,name): color_hex}

    出力:
      GeoJSON Feature リスト。各 Feature は mapping() によって GeoJSON 互換の dict に変換される。

    注意:
      - Shapely が利用不可なら空リストを返す
      - mapping() 呼び出しが失敗するジオメトリはスキップされる
    """
    features: List[dict] = []
    if not HAS_SHAPELY:
        return features
    for owner_key, geom in sorted(merged_owner_geoms.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        try:
            from shapely.geometry import mapping as _mapping
        except Exception:
            _mapping = None
        if _mapping is None:
            continue
        try:
            geom_json = _mapping(geom)
        except Exception:
            continue
        empire_id, empire_name = owner_key
        color = assigned_color.get(owner_key, DEFAULT_EMPIRE_COLOR)

        popup = [empire_name, "", "", "", ""]
        info = None
        if empire_info is not None:
            try:
                info = empire_info.get(int(empire_id))
            except Exception:
                info = empire_info.get(str(empire_id))
        if info and isinstance(info, dict) and info.get("empire") is not None:
            info = info.get("empire")

        if info:
            cap_name = info.get("capitalClaimName")
            cap_region = info.get("capitalRegionId")

            # Tier: try to lookup via claims_map using capitalClaimId
            tier_val = None
            try:
                cap_id = info.get("capitalClaimId")
                if cap_id is not None and claims_map is not None:
                    try:
                        claim_key = int(cap_id)
                    except Exception:
                        claim_key = str(cap_id)
                    claim = claims_map.get(claim_key)
                    if not claim and isinstance(claim_key, str):
                        try:
                            claim = claims_map.get(int(claim_key))
                        except Exception:
                            claim = None
                    if claim and isinstance(claim, dict):
                        tier_val = claim.get("tier")
            except Exception:
                tier_val = None

            # Capital line: include tier if available as (T{tier})
            try:
                if cap_name:
                    if tier_val is not None:
                        popup[2] = f"Capital : {cap_name} (T{tier_val})"
                    else:
                        popup[2] = f"Capital : {cap_name}"
                else:
                    popup[2] = ""
            except Exception:
                popup[2] = ""

            # Region line
            try:
                if cap_region is not None:
                    popup[3] = f"Region : {cap_region}"
                else:
                    popup[3] = ""
            except Exception:
                popup[3] = ""

            # Location line
            lx = info.get("locationX")
            lz = info.get("locationZ")
            try:
                if lx is not None and lz is not None:
                    e = round(float(lx) / 3.0)
                    n = round(float(lz) / 3.0)
                    popup[4] = f"Location : N {n} E {e}"
                else:
                    popup[4] = ""
            except Exception:
                popup[4] = ""
        else:
            popup = [empire_name]

        props = {"popupText": popup, "color": color, "fillColor": color, "fillOpacity": OWNER_FILL_OPACITY}
        features.append({"type": "Feature", "properties": props, "geometry": geom_json})
    return features
