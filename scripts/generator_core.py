#!/usr/bin/env python3
"""Core generator logic for BitJita -> GeoJSON.

このモジュールは BitJita API から取得したデータを GeoJSON に変換する
主要なロジック（API クライアント、レートリミッタ、座標変換、チャンクマップ構築、
Shapely を使ったポリゴン結合や隣接グラフ、色付けなど）を含みます。

`generate_geojson.py` はこのモジュールを薄くラップして CLI を提供します。
"""
from __future__ import annotations

import time
import threading
import random
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import requests
import os

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

# Shared palette for deterministic and greedy coloring
PALETTE = [
    "#FF5500ff",
    "#AAFF00ff",
    "#00FFAAff",
    "#0088FFff",
    "#6600FFff",
    "#FF0099ff",
]


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
            r = session.get(url, headers=headers, timeout=timeout)
        except requests.RequestException:
            if attempt == max_retries:
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            try:
                wait = float(ra) if ra is not None else (backoff_base * (2 ** (attempt - 1)))
            except Exception:
                wait = backoff_base * (2 ** (attempt - 1))
            time.sleep(wait + random.random() * 0.2)
            if attempt == max_retries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            if attempt == max_retries:
                r.raise_for_status()
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        r.raise_for_status()
        return r


class BitJitaClient:
    def __init__(self, session: requests.Session, limiter: RateLimiter, user_agent: str):
        self.session = session
        self.limiter = limiter
        self.user_agent = user_agent

    def fetch_empires(self) -> List[dict]:
        # /api/empires からエンパイア一覧を取得し、JSON の 'empires' を返す
        url = f"{BASE_URL}/api/empires"
        try:
            r = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if r is None:
                return []
            return r.json().get("empires", [])
        except Exception:
            # 呼び出しに失敗した場合は空リストを返して呼び出し側で扱う
            return []

    def fetch_towers(self, empire_id: int) -> List[dict]:
        # 指定エンパイアの塔情報を取得する（通常はリストを返す）
        url = f"{BASE_URL}/api/empires/{empire_id}/towers"
        try:
            r = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if r is None:
                return []
            return r.json()
        except Exception:
            return []


def small_to_chunk(x: int, y: int) -> Tuple[int, int]:
    # SmallHexTile (ゲーム内部座標) をチャンク座標に変換する
    # 仕様：チャンク = floor(座標 / 96)
    return (x // 96, y // 96)


def chunk_bounds(cx: int, cy: int) -> List[Tuple[int, int]]:
    x0 = cx * 96
    y0 = cy * 96
    x1 = (cx + 1) * 96
    y1 = (cy + 1) * 96
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def _coords_to_feature_polygon(coords: List[Tuple[int, int]]) -> List[List[List[int]]]:
    """ヘルパー: chunk_bounds の出力を GeoJSON の Polygon 座標形式に変換する。

    返り値の形: [[[x0,y0],[x1,y0],...]] のような 1 要素のリスト（外殻のみ）。
    この関数は内部利用のみで、明示的に GeoJSON 形式に整形する役割を持つ。
    """
    return [[list(p) for p in coords]]


def build_features_from_chunkmap(chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]]) -> List[dict]:
    """chunkmap を受け取り、チャンク単位の GeoJSON Feature リストを返す。

    入力:
      chunkmap: {(cx,cy): set((eid, name), ...), ...}

    出力:
      GeoJSON Feature のリスト。各 Feature はチャンクの四角形ポリゴンで、
      所有者が複数なら 'Contested' 表示、単一なら PALETTE から色を選ぶ。

    注意点:
      - chunkmap のキーは任意の整数チャンク座標を許す（負値も理論上は可能）
      - 所有者セットが空のチャンクは無視する
    """
    features: List[dict] = []
    for (cx, cy), owners in chunkmap.items():
        coords = chunk_bounds(cx, cy)
        coords_list = _coords_to_feature_polygon(coords)
        if len(owners) == 0:
            # 所有者なしはスキップ
            continue
        if len(owners) > 1:
            # 複数オーナー -> 競合表示
            owner_names = ", ".join(sorted(n for (_, n) in owners))
            props = {"popupText": f"Contested: {owner_names}", "color": "#888888", "fillColor": "#888888", "fillOpacity": 0.2}
        else:
            # 単一オーナー -> PALETTE から色を選ぶ（deterministic）
            eid, name = next(iter(owners))
            color = PALETTE[eid % len(PALETTE)]
            props = {"popupText": name, "color": color, "fillColor": color, "fillOpacity": 0.4}
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
      - chunkmap: {(cx,cy): set((eid,name), ...)} — チャンクごとの所有者集合
      - siege_points: 将来的な利用を想定した位置情報リスト（現状は未使用）

    挙動:
      - 並列に各エンパイアの塔を取得し、各塔が占有する 5x5 チャンク範囲を chunkmap に追加する
      - towers の 'active' が False のものは無視する
      - locationX / locationZ が無い、あるいは範囲外 (<=0 または >23040) は無視する
      - max_features / max_towers_per_empire が設定されている場合は早期停止する
    """
    chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]] = defaultdict(set)
    # siege_points は将来的なアイコンやポイント表現用の保持場所（現状未使用）
    siege_points: List[dict] = []

    def fetch_emp(emp: Tuple[int, str]):
        eid, name = emp
        try:
            log(f"Fetching towers for empire {eid} ({name})")
            t0 = time.perf_counter()
            towers = client.fetch_towers(eid)
            t1 = time.perf_counter()
            log(f"Fetched {len(towers)} towers for {eid} in {t1 - t0:.2f}s")
            return eid, name, towers
        except Exception as exc:
            log(f"Failed to fetch towers for {eid}: {exc}")
            return eid, name, []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_emp, emp): emp for emp in emps_to_process}
        for fut in as_completed(futures):
            eid, name, towers = fut.result()
            towers_handled = 0
            for t in towers:
                if args.max_towers_per_empire > 0 and towers_handled >= args.max_towers_per_empire:
                    break
                if not t.get("active", True):
                    continue
                # BitJita のレスポンスにおける内部座標
                # docs により locationX が X、locationZ が Y（Z が Y 軸扱い）
                x = t.get("locationX")
                y = t.get("locationZ")
                if x is None or y is None:
                    continue
                if x <= 0 or y <= 0 or x > 23040 or y > 23040:
                    continue
                try:
                    xi = int(x)
                    yi = int(y)
                except Exception:
                    continue
                # 各塔は watchtower の影響範囲として中心チャンクから 5x5 チャンクを占有する
                cx, cy = small_to_chunk(xi, yi)
                for dx in range(-2, 3):
                    for dy in range(-2, 3):
                        chunkmap[(cx + dx, cy + dy)].add((eid, name))
                towers_handled += 1
                approx_features = len(chunkmap) + len(siege_points)
                if args.max_features > 0 and approx_features >= args.max_features:
                    log(f"Reached max-features={args.max_features}, stopping early")
                    break
            print(f"Processed empire {eid} ({name}), towers handled: {towers_handled}")
            time.sleep(throttle)

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
    # local import to satisfy static analysis and ensure availability at runtime
    from shapely.geometry import Polygon as _Polygon
    for (cx, cy), owners in chunkmap.items():
        coords = chunk_bounds(cx, cy)
        try:
            poly = _Polygon(coords)
        except Exception:
            # 座標が不正などで Polygon が作れない場合はスキップ
            continue
        if len(owners) == 1:
            eid, name = next(iter(owners))
            owner_polys[(eid, name)].append(poly)
        elif len(owners) > 1:
            contested_polys.append(poly)
    return owner_polys, contested_polys


def merge_owner_geometries(owner_polys, log):
    merged_owner_geoms: Dict[Tuple[int, str], Any] = {}
    if not HAS_SHAPELY:
        return merged_owner_geoms
    # 同一オーナーの複数チャンクポリゴンを unary_union で結合する
    t_merge_start = time.perf_counter()
    for (eid, name), polys in owner_polys.items():
        # local import to avoid module-level None issues and appease static checkers
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
            merged_owner_geoms[(eid, name)] = merged
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
    nodes = list(merged_owner_geoms.keys())
    adjacency: Dict[Tuple[int, str], Set[Tuple[int, str]]] = {n: set() for n in nodes}
    log(f"Building adjacency graph for {len(nodes)} owner geometries...")
    t_adj_start = time.perf_counter()

    # 隣接グラフを作る。ポリゴン同士が intersects/touches する場合に辺を張る。
    # ノード数が多い場合は STRtree を使って候補検索を行い高速化する。
    if STRtree is not None and len(nodes) > 50 and not getattr(args, "force_pairwise", False):
        geom_list = [merged_owner_geoms[n] for n in nodes]
        try:
            tree = STRtree(geom_list)
            geom_id_to_index = {id(g): i for i, g in enumerate(geom_list)}
            for i, g in enumerate(geom_list):
                owner_key = nodes[i]
                try:
                    candidates = tree.query(g)
                except Exception:
                    candidates = []
                for c in candidates:
                    j = None
                    if isinstance(c, int):
                        j = c
                    else:
                        j = geom_id_to_index.get(id(c))
                        if j is None:
                            try:
                                j = geom_list.index(c)
                            except Exception:
                                try:
                                    j = int(c)
                                except Exception:
                                    j = None
                    if j is None or j == i:
                        continue
                    other = nodes[j]
                    try:
                        if isinstance(c, int):
                            cgeom = geom_list[j]
                        else:
                            cgeom = c if hasattr(c, "geom_type") else geom_list[j]
                        if g.intersects(cgeom) or g.touches(cgeom):
                            adjacency[owner_key].add(other)
                            adjacency[other].add(owner_key)
                    except Exception:
                        continue
        except Exception:
            pass
    else:
        for i, a in enumerate(nodes):
            ga = merged_owner_geoms[a]
            for b in nodes[i + 1 :]:
                gb = merged_owner_geoms[b]
                try:
                    if ga.intersects(gb) or ga.touches(gb):
                        adjacency[a].add(b)
                        adjacency[b].add(a)
                except Exception:
                    continue
    t_adj_end = time.perf_counter()
    edge_count = sum(len(s) for s in adjacency.values()) // 2
    log(f"Adjacency graph built: nodes={len(nodes)}, edges={edge_count} (took {t_adj_end - t_adj_start:.2f}s)")
    return adjacency


def greedy_coloring(adjacency, palette, log, verbose: bool, color_store_path: str | None = None):
    """隣接グラフに貪欲着色を行う。

    戦略:
      - 隣接度の高い順でノードを並べ（降順）、利用可能なパレット色を割り当てる
      - パレットに空きが無い場合はノードの eid に基づくデフォルト色を割り当てる

    出力:
      {node: color_hex}
    """
    nodes = list(adjacency.keys())
    sorted_nodes = sorted(nodes, key=lambda n: len(adjacency[n]), reverse=True)
    assigned_color: Dict[Tuple[int, str], str] = {}

    # Load existing color store (entityId -> color) if path provided.
    color_by_eid: Dict[int, str] = {}
    if color_store_path:
        # Only try to load the YAML file if it exists. If it doesn't, continue
        # with an empty in-memory store (we'll create parent dir on save).
        if os.path.exists(color_store_path):
            # PyYAML is required — fail fast if not available
            try:
                import yaml
            except Exception:
                msg = (
                    "PyYAML is required for color store support. This repository uses uv; run:"
                    "\n    uv sync"
                )
                log(msg)
                raise RuntimeError(msg)

            try:
                with open(color_store_path, "r", encoding="utf-8") as f:
                    loaded = yaml.safe_load(f) or {}
            except Exception as exc:
                log(f"Failed to read color store {color_store_path}: {exc}; continuing with empty store")
                loaded = {}

            for k, v in (loaded.items() if isinstance(loaded, dict) else []):
                try:
                    color_by_eid[int(k)] = v
                except Exception:
                    # keep key as-is if it isn't an int-like
                    color_by_eid[k] = v
            log(f"Loaded color store from {color_store_path} ({len(color_by_eid)} entries)")
        else:
            log(f"Color store not found at {color_store_path}; continuing with empty store")

    if verbose:
        log(f"Coloring order (top 20): {sorted_nodes[:20]}")

    for n in sorted_nodes:
        eid = n[0]
        # If we have a persisted color for this entityId, reuse it.
        stored = None
        try:
            stored = color_by_eid.get(int(eid))
        except Exception:
            stored = color_by_eid.get(eid)
        if stored is not None:
            assigned_color[n] = stored
            if verbose:
                log(f"Reused stored color for {n}: {stored}")
            continue

        used = {assigned_color[nb] for nb in adjacency[n] if nb in assigned_color}
        pick = next((c for c in palette if c not in used), None)
        if pick is None:
            # パレット枯渇時のフォールバック（決定論的）
            pick = PALETTE[eid % len(PALETTE)]
        assigned_color[n] = pick
        # persist this assignment into the in-memory map so subsequent nodes can reuse it
        try:
            color_by_eid[int(eid)] = pick
        except Exception:
            color_by_eid[eid] = pick
        if verbose:
            log(f"Assigned color for {n}: {pick} (used around it: {used})")

    # Save updated color store back to disk if requested
    if color_store_path:
        # PyYAML is required for saving as well — ensure available
        try:
            import yaml
        except Exception:
            msg = (
                "PyYAML is required to save the color store. This repository uses uv; run:"
                "\n    uv sync"
            )
            log(msg)
            raise RuntimeError(msg)

        # Ensure parent directory exists before writing
        dirpath = os.path.dirname(color_store_path)
        if dirpath:
            try:
                os.makedirs(dirpath, exist_ok=True)
            except Exception as exc:
                log(f"Failed to create directory for color store {dirpath}: {exc}")

        # write keys preserving numeric entityId types when possible
        dumpable = {k: v for k, v in color_by_eid.items()}
        try:
            with open(color_store_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(dumpable, f, allow_unicode=True)
            log(f"Saved color store to {color_store_path} ({len(dumpable)} entries)")
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
    for owner_key, geom in merged_owner_geoms.items():
        # do a local import for mapping to avoid module-level None issues
        try:
            from shapely.geometry import mapping as _mapping
        except Exception:
            _mapping = None
        if _mapping is None:
            continue
        try:
            geom_json = _mapping(geom)
        except Exception:
            # ジオメトリの変換に失敗した場合はスキップ
            continue
        eid, name = owner_key
        color = assigned_color.get(owner_key, PALETTE[eid % len(PALETTE)])
        props = {"popupText": name, "color": color, "fillColor": color, "fillOpacity": 0.2}
        features.append({"type": "Feature", "properties": props, "geometry": geom_json})
    return features
