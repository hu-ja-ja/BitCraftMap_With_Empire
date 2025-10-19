#!/usr/bin/env python3
"""Generate GeoJSON from BitJita API (dry-run).

Produces `Resource/generated.geojson` by default. Respects User-Agent and a simple throttle
to avoid hitting the 250 req/min limit. This is a minimal, safe runner for local testing.
"""
from __future__ import annotations

import argparse
import json
import time
import threading
import random
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# shapely is optional; if available we'll merge chunks into unified polygons per-empire
try:
    from shapely.geometry import Polygon as ShapelyPolygon, mapping as shapely_mapping
    from shapely.ops import unary_union
    # optional spatial index for faster adjacency (Shapely >= 1.8/2.x)
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
DEFAULT_USER_AGENT = "BitJita (Map_With_Empire)"


class RateLimiter:
    """Token-bucket rate limiter (thread-safe).

    rate_per_min: allowed requests per minute. Allows gentle pacing and bursts up to capacity.
    """

    def __init__(self, rate_per_min: int = 250, capacity: int | None = None):
        # rate_per_min controls the long-term average. To avoid large startup bursts
        # we choose a conservative default capacity (max burst size) and start with
        # zero tokens so work ramps up to the configured rate instead of sending
        # many requests immediately at startup.
        self.rate_per_min = max(1, int(rate_per_min))
        self.rate_per_sec = self.rate_per_min / 60.0
        # default capacity capped to a small number to prevent large bursts
        default_capacity = min(self.rate_per_min, 10)
        self.capacity = capacity if capacity is not None else default_capacity
        # start with zero tokens to prevent startup burst
        self._tokens = 0.0
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            # Refill
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._last = now
                return
            # Need to wait
            needed = tokens - self._tokens
            wait = needed / self.rate_per_sec
        # sleep outside lock
        time.sleep(wait + 0.01)


def _get_with_retries(session: requests.Session, url: str, limiter: RateLimiter, headers: dict, timeout: float = 10.0, max_retries: int = 4):
    """GET with retries, honoring rate limiter and Retry-After on 429."""
    backoff_base = 0.5
    for attempt in range(1, max_retries + 1):
        limiter.acquire()
        try:
            r = session.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            if attempt == max_retries:
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        if r.status_code == 429:
            # obey Retry-After if provided
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
            # server error, retry
            if attempt == max_retries:
                r.raise_for_status()
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            time.sleep(sleep_for)
            continue

        # success or 4xx other than 429
        r.raise_for_status()
        return r


def fetch_empires(session: requests.Session, limiter: RateLimiter, user_agent: str, timeout: float = 10.0) -> List[dict]:
    url = f"{BASE_URL}/api/empires"
    try:
        r = _get_with_retries(session, url, limiter, headers={"User-Agent": user_agent}, timeout=timeout)
        if r is None:
            return []
        return r.json().get("empires", [])
    except Exception as exc:
        print(f"Error fetching empires: {exc}", flush=True)
        return []


def fetch_towers(session: requests.Session, limiter: RateLimiter, empire_id: int, user_agent: str, timeout: float = 10.0) -> List[dict]:
    url = f"{BASE_URL}/api/empires/{empire_id}/towers"
    try:
        r = _get_with_retries(session, url, limiter, headers={"User-Agent": user_agent}, timeout=timeout)
        if r is None:
            return []
        return r.json()
    except Exception as exc:
        print(f"Error fetching towers for {empire_id}: {exc}", flush=True)
        return []


def small_to_chunk(x: int, y: int) -> Tuple[int, int]:
    return (x // 96, y // 96)


def chunk_bounds(cx: int, cy: int) -> List[Tuple[int, int]]:
    # Return polygon coordinates for single chunk (cx,cy) in map coordinate space.
    x0 = cx * 96
    y0 = cy * 96
    x1 = (cx + 1) * 96
    y1 = (cy + 1) * 96
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def color_for_empire(empire_id: int) -> str:
    # Deterministic palette pick from id
    palette = [
        "#FF5500ff",
        "#AAFF00ff",
        "#00FFAAff",
        "#0088FFff",
        "#6600FFff",
        "#FF0099ff",
    ]
    return palette[empire_id % len(palette)]


def build_features_from_chunkmap(
    chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]]
) -> List[dict]:
    features: List[dict] = []
    for (cx, cy), owners in chunkmap.items():
        coords = chunk_bounds(cx, cy)
        coords_list = [[list(p) for p in coords]]
        if len(owners) == 0:
            continue
        if len(owners) > 1:
            owner_names = ", ".join(sorted(n for (_id, n) in owners))
            # Contested: gray fill, slightly more visible
            props = {"popupText": f"Contested: {owner_names}", "color": "#888888", "fillColor": "#888888", "fillOpacity": 0.2}
        else:
            eid, name = next(iter(owners))
            # Owned: outline color, fill same color with increased visibility
            props = {"popupText": name, "color": color_for_empire(eid), "fillColor": color_for_empire(eid), "fillOpacity": 0.4}

        feature = {"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": coords_list}}
        features.append(feature)
    return features


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Resource/generated.geojson", help="Output GeoJSON path")
    ap.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    ap.add_argument("--throttle-ms", type=int, default=250, help="Minimum ms between API calls (throttle)")
    ap.add_argument("--limit-empires", type=int, default=50, help="Limit number of empires to process (dry-run) -0 for all")
    ap.add_argument("--max-features", type=int, default=0, help="Stop after this many output features (0 = no limit)")
    ap.add_argument("--max-towers-per-empire", type=int, default=0, help="Limit towers processed per empire (0 = no limit)")
    ap.add_argument("--rate-per-min", type=int, default=250, help="Allowed API requests per minute (token-bucket)")
    ap.add_argument("--workers", type=int, default=8, help="Number of threads to use for parallel tower fetching")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose logging for debugging and progress")
    ap.add_argument("--force-pairwise", action="store_true", help="Disable STRtree and force pairwise adjacency checks (debug)")
    args = ap.parse_args()

    ua = args.user_agent
    throttle = args.throttle_ms / 1000.0
    verbose = args.verbose

    def log(msg: str) -> None:
        if verbose:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"[{ts}] {msg}", flush=True)

    # create a single session and a rate limiter to be gentle to the API
    session = requests.Session()
    limiter = RateLimiter(rate_per_min=args.rate_per_min)

    log("Fetching empires...")
    t_fetch_start = time.perf_counter()
    empires = fetch_empires(session, limiter, ua)
    t_fetch_end = time.perf_counter()
    log(f"Fetched empires: {len(empires)} (took {t_fetch_end - t_fetch_start:.2f}s)")

    chunkmap: Dict[Tuple[int, int], Set[Tuple[int, str]]] = defaultdict(set)
    siege_points: List[dict] = []

    # Parallel fetch of towers to hide network latency and improve throughput.
    processed = 0
    try:
        # prepare list of empires to fetch
        emps_to_process: List[Tuple[int, str]] = []
        for e in empires:
            if args.limit_empires > 0 and len(emps_to_process) >= args.limit_empires:
                break
            eid = e.get("entityId")
            if eid is None:
                continue
            try:
                eid = int(eid)
            except Exception:
                continue
            name = e.get("name", f"empire-{eid}")
            emps_to_process.append((eid, name))

        log(f"Processing {len(emps_to_process)} empires with {args.workers} workers")

        def fetch_emp(emp: Tuple[int, str]):
            eid, name = emp
            try:
                log(f"Fetching towers for empire {eid} ({name})")
                t0 = time.perf_counter()
                towers = fetch_towers(session, limiter, eid, ua)
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
                    # Validate bounds
                    if x <= 0 or y <= 0 or x > 23040 or y > 23040:
                        continue

                    try:
                        xi = int(x)
                        yi = int(y)
                    except Exception:
                        continue
                    cx, cy = small_to_chunk(xi, yi)

                    # mark 5x5 chunks centered on (cx,cy)
                    for dx in range(-2, 3):
                        for dy in range(-2, 3):
                            chunkmap[(cx + dx, cy + dy)].add((eid, name))

                    towers_handled += 1

                    # check global feature limit (approx: number of chunks + sieges)
                    approx_features = len(chunkmap) + len(siege_points)
                    if args.max_features > 0 and approx_features >= args.max_features:
                        log(f"Reached max-features={args.max_features}, stopping early")
                        break

                processed += 1
                print(f"Processed empire {eid} ({name}), towers handled: {towers_handled}")
                time.sleep(throttle)
    except KeyboardInterrupt:
        print("Interrupted by user - will write partial results", flush=True)
    features: List[dict] = []

    if HAS_SHAPELY and ShapelyPolygon is not None and unary_union is not None and shapely_mapping is not None:
        # Build per-owner shapely polygons and a contested polygon set
        owner_polys: Dict[Tuple[int, str], List] = defaultdict(list)
        contested_polys: List = []

        # Populate shapely polygons per owner from chunkmap
        for (cx, cy), owners in chunkmap.items():
            coords = chunk_bounds(cx, cy)
            # Shapely expects (x,y) tuples
            try:
                poly = ShapelyPolygon(coords)
            except Exception:
                continue
            if len(owners) == 1:
                eid, name = next(iter(owners))
                owner_polys[(eid, name)].append(poly)
            elif len(owners) > 1:
                contested_polys.append(poly)

        # Build per-owner shapely geometries and compute adjacency + coloring
        try:
            log("Merging owner polygons (this can be slow for many empires)...")
            t_merge_start = time.perf_counter()
            merged_owner_geoms: Dict[Tuple[int, str], Any] = {}
            for (eid, name), polys in owner_polys.items():
                try:
                    merged = unary_union(polys)
                except Exception:
                    merged = None
                if merged is not None:
                    merged_owner_geoms[(eid, name)] = merged
            t_merge_end = time.perf_counter()
            log(f"Merged owner polygons: {len(merged_owner_geoms)} owners (took {t_merge_end - t_merge_start:.2f}s)")

            # Palette for coloring (6 colors)
            palette = [
                "#FF5500ff",
                "#AAFF00ff",
                "#00FFAAff",
                "#0088FFff",
                "#6600FFff",
                "#FF0099ff",
            ]

            nodes = list(merged_owner_geoms.keys())
            adjacency: Dict[Tuple[int, str], Set[Tuple[int, str]]] = {n: set() for n in nodes}
            log(f"Building adjacency graph for {len(nodes)} owner geometries...")
            t_adj_start = time.perf_counter()
            if STRtree is not None and len(nodes) > 50 and not args.force_pairwise:
                # use spatial index for larger datasets
                geom_list = [merged_owner_geoms[n] for n in nodes]
                try:
                    tree = STRtree(geom_list)
                    # id -> index mapping for fast lookup of candidates
                    geom_id_to_index = {id(g): i for i, g in enumerate(geom_list)}
                    for i, g in enumerate(geom_list):
                        owner_key = nodes[i]
                        try:
                            candidates = tree.query(g)
                        except Exception:
                            candidates = []
                        for c in candidates:
                            j = None
                            # STRtree may return integer indices (Shapely 2.x) or geometry objects
                            if isinstance(c, int):
                                j = c
                            else:
                                j = geom_id_to_index.get(id(c))
                                if j is None:
                                    # fallback: try to find in list (equality-based)
                                    try:
                                        j = geom_list.index(c)
                                    except Exception:
                                        # last resort: try to coerce to int index
                                        try:
                                            j = int(c)
                                        except Exception:
                                            j = None
                            if j is None:
                                continue
                            if j == i:
                                continue
                            other = nodes[j]
                            try:
                                # choose candidate geometry object for spatial test
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
                    # fallback to pairwise
                    pass
            else:
                # small sets: pairwise check is acceptable
                for i, a in enumerate(nodes):
                    ga = merged_owner_geoms[a]
                    for b in nodes[i + 1 :]:
                        gb = merged_owner_geoms[b]
                        try:
                            if ga.intersects(gb) or ga.touches(gb):
                                adjacency[a].add(b)
                                adjacency[b].add(a)
                        except Exception:
                            # ignore geometry errors for pair
                            continue
            t_adj_end = time.perf_counter()
            edge_count = sum(len(s) for s in adjacency.values()) // 2
            log(f"Adjacency graph built: nodes={len(nodes)}, edges={edge_count} (took {t_adj_end - t_adj_start:.2f}s)")

            if verbose:
                log("Adjacency sizes (sample up to 10):")
                for n in nodes[:10]:
                    log(f"  {n}: {len(adjacency[n])} neighbors")

            # Greedy coloring (order by degree desc)
            sorted_nodes = sorted(nodes, key=lambda n: len(adjacency[n]), reverse=True)
            assigned_color: Dict[Tuple[int, str], str] = {}
            if verbose:
                log(f"Coloring order (top 20): {sorted_nodes[:20]}")
            for n in sorted_nodes:
                used = {assigned_color[nb] for nb in adjacency[n] if nb in assigned_color}
                pick = next((c for c in palette if c not in used), None)
                if pick is None:
                    pick = color_for_empire(n[0])
                assigned_color[n] = pick
                if verbose:
                    log(f"Assigned color for {n}: {pick} (used around it: {used})")

            # Emit colored features for owners
            for owner_key, geom in merged_owner_geoms.items():
                try:
                    geom_json = shapely_mapping(geom)
                except Exception:
                    # skip geometry if mapping fails
                    continue
                eid, name = owner_key
                color = assigned_color.get(owner_key, color_for_empire(eid))
                props = {"popupText": name, "color": color, "fillColor": color, "fillOpacity": 0.2}
                features.append({"type": "Feature", "properties": props, "geometry": geom_json})
        except Exception as exc:
            log(f"ERROR during shapely merge/adjacency/coloring: {exc}")
            import traceback

            traceback.print_exc()

        # Merge contested
        if contested_polys:
            try:
                merged_contested = unary_union(contested_polys)
            except Exception:
                merged_contested = None
            if merged_contested is not None:
                geom = shapely_mapping(merged_contested)
                props = {"popupText": "Contested", "color": "#888888", "fillColor": "#888888", "fillOpacity": 0.4}
                features.append({"type": "Feature", "properties": props, "geometry": geom})
    else:
        # No shapely: fall back to per-chunk polygons
        print("Warning: shapely not installed — output will contain one polygon per chunk (no merging). Install shapely for merged polygons.")
        features.extend(build_features_from_chunkmap(chunkmap))

    # Siege output disabled: do not append siege_points to final features
    # features.extend(siege_points)

    # Insert a single LayerOFF control object at the top of the FeatureCollection
    layer_off = {
        "type": "LayerOFF",
        "properties": {"turnLayerOff": ["ruinedLayer", "treesLayer", "templesLayer"]},
    }
    fc = {"type": "FeatureCollection", "features": [layer_off] + features}
    out = args.out
    with open(out, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(features)} features to {out}")


if __name__ == "__main__":
    main()
