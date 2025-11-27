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
- Python 3.12+
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
    try:
        from shapely.strtree import STRtree
    except Exception:
        STRtree = None
    HAS_SHAPELY = True
except Exception:
    shapely_mapping = None
    unary_union = None
    STRtree = None
    HAS_SHAPELY = False

BASE_URL = "https://bitjita.com"
DEFAULT_USER_AGENT = "Map_With_Empire (discord: hu_ja_ja_)"

COLOR_PALETTE = [
    "#FF5500ff",
    "#AAFF00ff",
    "#00FFAAff",
    "#0088FFff",
    "#6600FFff",
    "#FF0099ff",
]

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


# delegate chunk/coords helpers to scripts/coords.py
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
            color = COLOR_PALETTE[empire_id % len(COLOR_PALETTE)]
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
            log(f"Fetching towers for empire {empire_id} ({empire_name})")
            fetch_start = time.perf_counter()
            towers = client.fetch_towers(empire_id)
            fetch_end = time.perf_counter()
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
        print(f"Processed empire {empire_id} ({empire_name}), towers handled: {towers_handled}")

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


def build_adjacency(merged_owner_geoms, args, log):
    """マージ済みオーナー幾何を受け取り、隣接グラフを返す。

    入力:
      merged_owner_geoms: {(eid,name): shapely_geom}
      args: CLI 引数（force_pairwise を参照）

    出力:
      adjacency: {node: set(neighbor_nodes)}

    最適化:
      - オブジェクト数が十分に多い（>50）の場合、STRtree を使って候補を検索する。
      - STRtree の query 結果は環境によって型が異なる（geom オブジェクトや index など）ため
        保守的にハンドリングしている。
    """
    nodes = sorted(list(merged_owner_geoms.keys()), key=lambda k: (k[0], k[1]))
    adjacency: Dict[Tuple[int, str], Set[Tuple[int, str]]] = {n: set() for n in nodes}
    log(f"Building adjacency graph for {len(nodes)} owner geometries...")
    t_adj_start = time.perf_counter()

    if STRtree is not None and len(nodes) > 50 and not getattr(args, "force_pairwise", False):
        geom_list = [merged_owner_geoms[n] for n in nodes]
        try:
            tree = STRtree(geom_list)
            geom_id_to_index = {id(g): idx for idx, g in enumerate(geom_list)}
            for idx_i, geom_i in enumerate(geom_list):
                owner_key = nodes[idx_i]
                try:
                    candidates = tree.query(geom_i)
                except Exception:
                    candidates = []
                for candidate in candidates:
                    idx_j = None
                    if isinstance(candidate, int):
                        idx_j = candidate
                    else:
                        idx_j = geom_id_to_index.get(id(candidate))
                        if idx_j is None:
                            try:
                                idx_j = geom_list.index(candidate)
                            except Exception:
                                try:
                                    idx_j = int(candidate)
                                except Exception:
                                    idx_j = None
                    if idx_j is None or idx_j == idx_i:
                        continue
                    other = nodes[idx_j]
                    try:
                        if isinstance(candidate, int):
                            candidate_geom = geom_list[idx_j]
                        else:
                            candidate_geom = candidate if hasattr(candidate, "geom_type") else geom_list[idx_j]
                        if geom_i.intersects(candidate_geom) or geom_i.touches(candidate_geom):
                            adjacency[owner_key].add(other)
                            adjacency[other].add(owner_key)
                    except Exception:
                        continue
        except Exception:
            pass
    else:
        for idx_i, node_a in enumerate(nodes):
            geom_a = merged_owner_geoms[node_a]
            for node_b in nodes[idx_i + 1 :]:
                geom_b = merged_owner_geoms[node_b]
                try:
                    if geom_a.intersects(geom_b) or geom_a.touches(geom_b):
                        adjacency[node_a].add(node_b)
                        adjacency[node_b].add(node_a)
                except Exception:
                    continue
    t_adj_end = time.perf_counter()
    edge_count = sum(len(s) for s in adjacency.values()) // 2
    log(f"Adjacency graph built: nodes={len(nodes)}, edges={edge_count} (took {t_adj_end - t_adj_start:.2f}s)")
    return adjacency


def greedy_coloring(adjacency, palette, log, verbose: bool, color_store_path: str | None = None):
    """隣接グラフに対して貪欲着色を行い、色情報を `scripts.color_store` 経由で永続化する。

    - 既存の色があればそれを優先して使う（上書きしない）。
    - 毎回 `name` フィールドは更新する。
    - `color_store_path` が指定されればロード/セーブを行う。
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

    nodes = list(adjacency.keys())
    sorted_nodes = sorted(nodes, key=lambda n: len(adjacency[n]), reverse=True)
    assigned_color: Dict[Tuple[int, str], str] = {}

    for eid, meta in list(store.items()):
        if meta.get("color"):
            pass

    if verbose:
        log(f"Coloring order (top 20): {sorted_nodes[:20]}")

    for node_key in sorted_nodes:
        empire_id, empire_name = node_key
        try:
            eid_key = int(empire_id)
        except Exception:
            eid_key = empire_id

        persisted = store.get(eid_key)
        if persisted and persisted.get("color") is not None:
            color_val = persisted.get("color")
            if color_val:
                assigned_color[node_key] = color_val
            persisted["name"] = empire_name
            if verbose:
                log(f"Reused stored color for {node_key}: {color_val}")
            continue

        used = {assigned_color[n] for n in adjacency[node_key] if n in assigned_color}
        pick = next((c for c in palette if c not in used), palette[0])
        assigned_color[node_key] = pick

        existing = store.get(eid_key)
        if existing is None:
            store[eid_key] = {"name": empire_name, "color": pick}
        else:
            existing.setdefault("name", empire_name)
            if not existing.get("color"):
                existing["color"] = pick
        if verbose:
            log(f"Assigned color for {node_key}: {pick} (used around it: {used})")

    if color_store_path:
        try:
            _color_store.save_color_store(color_store_path, store)
            log(f"Saved color store to {color_store_path} ({len(store)} entries)")
        except Exception as exc:
            log(f"Failed to save color store {color_store_path}: {exc}")

    return assigned_color


def emit_owner_features(merged_owner_geoms, assigned_color):
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
        color = assigned_color.get(owner_key, COLOR_PALETTE[empire_id % len(COLOR_PALETTE)])
        props = {"popupText": empire_name, "color": color, "fillColor": color, "fillOpacity": OWNER_FILL_OPACITY}
        features.append({"type": "Feature", "properties": props, "geometry": geom_json})
    return features
