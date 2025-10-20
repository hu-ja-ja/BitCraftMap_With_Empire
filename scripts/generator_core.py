#!/usr/bin/env python3
"""Core generator logic for BitJita -> GeoJSON.

This module contains reusable classes and functions extracted from the original
`generate_geojson.py` to make the code modular and easier to test.
"""
from __future__ import annotations

import time
import threading
import random
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import requests

try:
    from shapely.geometry import Polygon as ShapelyPolygon, mapping as shapely_mapping
    from shapely.ops import unary_union
    try:
        from shapely.strtree import STRtree
    except Exception:
        STRtree = None
    HAS_SHAPELY = True
except Exception:
    ShapelyPolygon = None
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
        url = f"{BASE_URL}/api/empires"
        try:
            r = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if r is None:
                return []
            return r.json().get("empires", [])
        except Exception:
            return []

    def fetch_towers(self, empire_id: int) -> List[dict]:
        url = f"{BASE_URL}/api/empires/{empire_id}/towers"
        try:
            r = _get_with_retries(self.session, url, self.limiter, headers={"User-Agent": self.user_agent})
            if r is None:
                return []
            return r.json()
        except Exception:
            return []


def small_to_chunk(x: int, y: int) -> Tuple[int, int]:
    return (x // 96, y // 96)


def chunk_bounds(cx: int, cy: int) -> List[Tuple[int, int]]:
    x0 = cx * 96
    y0 = cy * 96
    x1 = (cx + 1) * 96
    y1 = (cy + 1) * 96
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def build_features_from_chunkmap(chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]]) -> List[dict]:
    features: List[dict] = []
    for (cx, cy), owners in chunkmap.items():
        coords = chunk_bounds(cx, cy)
        coords_list = [[list(p) for p in coords]]
        if len(owners) == 0:
            continue
        if len(owners) > 1:
            owner_names = ", ".join(sorted(n for (_id, n) in owners))
            props = {"popupText": f"Contested: {owner_names}", "color": "#888888", "fillColor": "#888888", "fillOpacity": 0.2}
        else:
            eid, name = next(iter(owners))
            props = {"popupText": name, "color": PALETTE[eid % len(PALETTE)], "fillColor": PALETTE[eid % len(PALETTE)], "fillOpacity": 0.4}
        feature = {"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": coords_list}}
        features.append(feature)
    return features


def process_empires_to_chunkmap(emps_to_process: List[Tuple[int, str]], client: BitJitaClient, args, throttle: float, log) -> Tuple[Dict[Tuple[int, int], Set[Tuple[int, str]]], List[dict]]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]] = defaultdict(set)
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
    nodes = list(merged_owner_geoms.keys())
    adjacency: Dict[Tuple[int, str], Set[Tuple[int, str]]] = {n: set() for n in nodes}
    log(f"Building adjacency graph for {len(nodes)} owner geometries...")
    t_adj_start = time.perf_counter()
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


def greedy_coloring(adjacency, palette, log, verbose: bool):
    nodes = list(adjacency.keys())
    sorted_nodes = sorted(nodes, key=lambda n: len(adjacency[n]), reverse=True)
    assigned_color: Dict[Tuple[int, str], str] = {}
    if verbose:
        log(f"Coloring order (top 20): {sorted_nodes[:20]}")
    for n in sorted_nodes:
        used = {assigned_color[nb] for nb in adjacency[n] if nb in assigned_color}
        pick = next((c for c in palette if c not in used), None)
        if pick is None:
            pick = PALETTE[n[0] % len(PALETTE)]
        assigned_color[n] = pick
        if verbose:
            log(f"Assigned color for {n}: {pick} (used around it: {used})")
    return assigned_color


def emit_owner_features(merged_owner_geoms, assigned_color, log):
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
            continue
        eid, name = owner_key
        color = assigned_color.get(owner_key, PALETTE[eid % len(PALETTE)])
        props = {"popupText": name, "color": color, "fillColor": color, "fillOpacity": 0.2}
        features.append({"type": "Feature", "properties": props, "geometry": geom_json})
    return features
