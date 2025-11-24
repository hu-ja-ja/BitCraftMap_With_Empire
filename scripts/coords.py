"""座標ユーティリティ（SmallHexTile ⇄ チャンク変換など）。

このモジュールはゲーム内部座標（SmallHexTile）の扱いに関する軽量なヘルパー関数を提供します。

主な役割:
- 小座標（SmallHexTile）の (X,Y) からチャンク座標への変換
- チャンクの四隅座標（チャンクの境界）を算出
- GeoJSON 用のポリゴン座標形式への変換補助
- 見張り塔（watchtower）が占有する 5x5 チャンクブロックを列挙

関数一覧:
- `smallhex_to_chunk(small_x, small_y)` — SmallHexTile -> (chunk_x, chunk_y)
- `chunk_bounds(chunk_x, chunk_y)` — チャンクの四隅 [(x0,y0), ...]
- `coords_to_feature_polygon(coords)` — GeoJSON の Polygon 座標形式に整形
- `tower_covered_chunks(small_x, small_y, radius_chunks=2)` — watchtower が占有するチャンク列挙

このモジュールは外部依存を持たない軽量ユーティリティなので、
テストや他モジュールからの呼び出しに適しています。
"""

from __future__ import annotations

from typing import Iterable, List, Tuple


def smallhex_to_chunk(small_x: int, small_y: int) -> Tuple[int, int]:
    return (small_x // 96, small_y // 96)


def chunk_bounds(chunk_x: int, chunk_y: int) -> List[Tuple[int, int]]:
    x0 = chunk_x * 96
    y0 = chunk_y * 96
    x1 = (chunk_x + 1) * 96
    y1 = (chunk_y + 1) * 96
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def coords_to_feature_polygon(coords: List[Tuple[int, int]]):
    return [[list(p) for p in coords]]


def tower_covered_chunks(small_x: int, small_y: int, radius_chunks: int = 2) -> Iterable[Tuple[int, int]]:
    """Return chunk coords covered by a watchtower centered at smallhex coords.

    Default radius_chunks=2 produces 5x5 block centered on tower chunk.
    """
    cx, cy = smallhex_to_chunk(small_x, small_y)
    for dx in range(-radius_chunks, radius_chunks + 1):
        for dy in range(-radius_chunks, radius_chunks + 1):
            yield (cx + dx, cy + dy)
