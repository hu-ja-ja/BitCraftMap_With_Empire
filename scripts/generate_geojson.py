#!/usr/bin/env python3
"""BitCraftMap ジェネレータの CLI エントリポイント。

このスクリプトは軽量なコマンドラインラッパーであり、引数を解析して
`generator_core.py` の実装に処理を委譲します。

主な役割:
- CLI オプションの解析（出力パス、スロットル、ワーカー数、上限など）
- HTTP セッションとトークンバケット方式の `RateLimiter` の初期化
- エンパイア一覧の取得と並列での塔取得を行い、
    `generator_core.process_empires_to_chunkmap` を用いてチャンク所有マップを構築
- Shapely が利用可能な場合はチャンクポリゴンを結合し、隣接性を計算して色付けし、
    GeoJSON フィーチャを出力。利用不可の場合はチャンク単位のポリゴンを出力

依存・前提:
- Python 3.12+
- `requests` — BitJita API 呼び出し
- `pyyaml` — 色ストアの読み書き（`scripts/color_store.py` を使用）
- `shapely` — ポリゴンのマージ・隣接判定（起動時に存在チェックを行います）

使用例:
    uv run generate

備考:
このファイルは CLI のみを担当し、主要ロジックは `scripts/generator_core.py` に移譲しています。
"""
from __future__ import annotations

import argparse
import json
import time
import os
import sys

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
    ap.add_argument("--workers", type=int, default=4, help="Number of threads to use for parallel tower fetching")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose logging for debugging and progress")
    ap.add_argument("--force-pairwise", action="store_true", help="Disable STRtree and force pairwise adjacency checks (debug)")
    ap.add_argument("--color-store", default="Resource/color_map.yaml", help="Path to YAML color store for entityId->color mapping")
    args = ap.parse_args()

    throttle = args.throttle_ms / 1000.0

    if not generator_core.HAS_SHAPELY:
        print("Error: Shapely is required for this tool.", file=sys.stderr)
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except Exception:
        print("Error: PyYAML is required for color store support.", file=sys.stderr)
        sys.exit(1)

    def log(msg: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[{ts}] {msg}", flush=True)

    def debug(msg: str) -> None:
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

    emps_to_process = []
    for empire_entry in empires:
        if args.limit_empires > 0 and len(emps_to_process) >= args.limit_empires:
            break
        eid = empire_entry.get("entityId")
        if eid is None:
            continue
        try:
            eid = int(eid)
        except Exception:
            continue
        name = empire_entry.get("name", f"empire-{eid}")
        emps_to_process.append((eid, name))

    log(f"Processing {len(emps_to_process)} empires with {args.workers} workers")

    chunkmap, siege_points = generator_core.process_empires_to_chunkmap(emps_to_process, client, args, throttle, log)

    empire_info = {}
    owner_ids = set()
    for owners in chunkmap.values():
        for owner in owners:
            try:
                owner_ids.add(int(owner[0]))
            except Exception:
                try:
                    owner_ids.add(owner[0])
                except Exception:
                    continue
    if owner_ids:
        total = len(owner_ids)
        debug(f"Fetching details for {total} owning empires...")
        success = 0
        failed = 0
        sorted_ids = sorted(owner_ids, key=lambda x: int(x) if isinstance(x, (int, str)) and str(x).isdigit() else str(x))
        for idx, eid in enumerate(sorted_ids, start=1):
            debug(f"Fetching empire details ({idx}/{total}): {eid}")
            try:
                detail = client.fetch_empire(eid)
            except Exception as exc:
                debug(f"Failed to fetch empire {eid}: {exc}")
                failed += 1
                detail = {}

            if not isinstance(detail, dict) or not detail:
                log(f"No details returned for empire {eid}")
                failed += 1
                time.sleep(throttle)
                continue

            if detail.get("empire") is not None:
                detail = detail.get("empire") or {}

            try:
                key = int(eid)
            except Exception:
                key = str(eid)
            empire_info[key] = detail
            success += 1
            cap = detail.get("capitalClaimName") or "(no capital)"
            debug(f"Fetched ({idx}/{total}) {eid} -> capital: {cap}")
            time.sleep(throttle)

        debug(f"Finished fetching empire details: success={success}, failed={failed}")

    # --- Prefetch top-tier claims (200) to reduce per-claim API calls ---
    claims_map = {}
    try:
        prefetch_pages = [1, 2, 3]
        fetched = 0
        debug("Prefetching top-tier claims pages...")
        for p in prefetch_pages:
            claims_page = client.fetch_claims_page(sort="tier", limit=100, page=p)
            if not isinstance(claims_page, list):
                continue
            for c in claims_page:
                eid_raw = c.get("entityId")
                if eid_raw is None:
                    continue
                try:
                    cid = int(eid_raw)
                except Exception:
                    cid = str(eid_raw)
                claims_map[cid] = c
                fetched += 1
            time.sleep(throttle)
        debug(f"Prefetched {fetched} claims from top-tier pages")
    except Exception:
        debug("Failed to prefetch top-tier claims")

    # Collect capitalClaimIds that need lookup, then fetch missing ones individually
    capital_ids_needed = set()
    for info in empire_info.values():
        try:
            capid = info.get("capitalClaimId")
        except Exception:
            capid = None
        if capid:
            try:
                capital_ids_needed.add(int(capid))
            except Exception:
                capital_ids_needed.add(capid)

    # Remove those already found in prefetch
    remaining = [cid for cid in capital_ids_needed if cid not in claims_map]
    if remaining:
        debug(f"Fetching {len(remaining)} individual claim(s) not covered by prefetch...")
        for idx, cid in enumerate(sorted(remaining, key=lambda x: int(x) if isinstance(x, (int, str)) and str(x).isdigit() else str(x)), start=1):
            debug(f"Fetching claim ({idx}/{len(remaining)}): {cid}")
            try:
                claim = client.fetch_claim(cid)
            except Exception as exc:
                debug(f"Failed to fetch claim {cid}: {exc}")
                claim = {}
            if claim and isinstance(claim, dict):
                try:
                    key = int(cid)
                except Exception:
                    key = str(cid)
                claims_map[key] = claim
            time.sleep(throttle)
        debug(f"Finished fetching individual claims: now have {len(claims_map)} claims in map")

    features = []
    if generator_core.HAS_SHAPELY:
        owner_polys, contested_polys = generator_core.build_owner_and_contested_polys(chunkmap, log)
        merged = generator_core.merge_owner_geometries(owner_polys, log)
        adjacency = generator_core.build_adjacency(merged, args, log)
        assigned = generator_core.greedy_coloring(adjacency, generator_core.COLOR_PALETTE, log, args.verbose, args.color_store)
        features.extend(generator_core.emit_owner_features(merged, assigned, empire_info, claims_map))
        if contested_polys:
            try:
                merged_contested = generator_core.unary_union(contested_polys)
            except Exception:
                merged_contested = None
            if merged_contested is not None:
                geom = generator_core.shapely_mapping(merged_contested)
                props = {"popupText": "Contested", "color": generator_core.CONTESTED_COLOR, "fillColor": generator_core.CONTESTED_COLOR, "fillOpacity": generator_core.CONTESTED_FILL_OPACITY}
                features.append({"type": "Feature", "properties": props, "geometry": geom})
    else:
        print("Warning: shapely not available — output will contain one polygon per chunk (no merging). Install shapely for merged polygons.")
        features.extend(generator_core.build_features_from_chunkmap(chunkmap))

    layer_off = {
        "type": "Feature",
        "properties": {
            "popupText": ["Repository is here.", "https://github.com/hu-ja-ja/BitCraftMap_With_Empire"],
            "iconName": "Hex_Logo",
            "turnLayerOff": ["ruinedLayer", "treesLayer", "templesLayer"]
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-5000, -5000]
        }
    }
    fc = {"type": "FeatureCollection", "features": [layer_off] + features}
    out = args.out
    with open(out, "w", encoding="utf-8") as outfile:
        json.dump(fc, outfile, ensure_ascii=False, indent=2)

    print(f"Wrote {len(features)} features to {out}")


if __name__ == "__main__":
    main()
