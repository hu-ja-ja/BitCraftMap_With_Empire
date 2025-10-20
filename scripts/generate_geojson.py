#!/usr/bin/env python3
"""CLI wrapper for generator_core.

Thin script that parses CLI args and delegates to `generator_core` functions.
"""
from __future__ import annotations

import argparse
import json
import time
import os
import sys

# ensure we can import sibling module 'generator_core'
sys.path.insert(0, os.path.dirname(__file__))
import generator_core


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Resource/generated.geojson", help="Output GeoJSON path")
    ap.add_argument("--user-agent", default=generator_core.DEFAULT_USER_AGENT)
    ap.add_argument("--throttle-ms", type=int, default=120, help="Minimum ms between API calls (throttle)")
    ap.add_argument("--limit-empires", type=int, default=0, help="Limit number of empires to process (dry-run) -0 for all")
    ap.add_argument("--max-features", type=int, default=0, help="Stop after this many output features (0 = no limit)")
    ap.add_argument("--max-towers-per-empire", type=int, default=0, help="Limit towers processed per empire (0 = no limit)")
    ap.add_argument("--rate-per-min", type=int, default=100, help="Allowed API requests per minute (token-bucket)")
    ap.add_argument("--workers", type=int, default=8, help="Number of threads to use for parallel tower fetching")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose logging for debugging and progress")
    ap.add_argument("--force-pairwise", action="store_true", help="Disable STRtree and force pairwise adjacency checks (debug)")
    args = ap.parse_args()

    throttle = args.throttle_ms / 1000.0

    def log(msg: str) -> None:
        if args.verbose:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"[{ts}] {msg}", flush=True)

    session = generator_core.requests.Session()
    limiter = generator_core.RateLimiter(rate_per_min=args.rate_per_min)
    client = generator_core.BitJitaClient(session, limiter, args.user_agent)

    log("Fetching empires...")
    t_fetch_start = time.perf_counter()
    empires = client.fetch_empires()
    t_fetch_end = time.perf_counter()
    log(f"Fetched empires: {len(empires)} (took {t_fetch_end - t_fetch_start:.2f}s)")

    # prepare list of empires to process
    emps_to_process = []
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

    chunkmap, siege_points = generator_core.process_empires_to_chunkmap(emps_to_process, client, args, throttle, log)

    features = []
    if generator_core.HAS_SHAPELY:
        owner_polys, contested_polys = generator_core.build_owner_and_contested_polys(chunkmap, log)
        merged = generator_core.merge_owner_geometries(owner_polys, log)
        adjacency = generator_core.build_adjacency(merged, args, log)
        assigned = generator_core.greedy_coloring(adjacency, generator_core.PALETTE, log, args.verbose)
        features.extend(generator_core.emit_owner_features(merged, assigned, log))
        if contested_polys:
            try:
                merged_contested = generator_core.unary_union(contested_polys)
            except Exception:
                merged_contested = None
            if merged_contested is not None:
                geom = generator_core.shapely_mapping(merged_contested)
                props = {"popupText": "Contested", "color": "#888888", "fillColor": "#888888", "fillOpacity": 0.4}
                features.append({"type": "Feature", "properties": props, "geometry": geom})
    else:
        print("Warning: shapely not available — output will contain one polygon per chunk (no merging). Install shapely for merged polygons.")
        features.extend(generator_core.build_features_from_chunkmap(chunkmap))

    layer_off = {
        "type": "Feature",
        "properties": {
            "popupText": "^-^",
            "turnLayerOff": ["ruinedLayer", "treesLayer", "templesLayer"]
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-10000, -10000]
        }
    }
    fc = {"type": "FeatureCollection", "features": [layer_off] + features}
    out = args.out
    with open(out, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(features)} features to {out}")


if __name__ == "__main__":
    main()
