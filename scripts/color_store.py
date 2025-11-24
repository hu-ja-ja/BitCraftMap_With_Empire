"""色ストアの入出力ユーティリティ（YAML）。

このモジュールは永続化された色マップを YAML で読み書きする機能を提供します。
フォーマットは次のようにエンティティ ID をトップレベルキーにしたマッピングです::

  40:
    name: The Ottoadman Empire
    color: '#AAFF00ff'

各エントリは `name`（帝国名）と `color`（先頭に `#` を含むカラー文字列）を持ちます。

主な役割:
- `load_color_store(path)` — YAML を読み込み、{int: { 'name': str|None, 'color': str|None }} を返す
- `save_color_store(path, mapping)` — マッピングを YAML に保存（`name` を先に、`color` を次に出力）
- 色文字列の正規化（先頭 `#` を保証）

前提:
- `pyyaml` が必要です（無ければ RuntimeError を投げます）。
"""

from __future__ import annotations

import os
from typing import Dict, Optional


def _ensure_yaml_available():
    try:
        import yaml
        return yaml
    except Exception as exc:
        raise RuntimeError("PyYAML is required for color store support. Run: uv sync") from exc


def normalize_color_for_runtime(color: Optional[str]) -> Optional[str]:
    if color is None:
        return None
    if not isinstance(color, str):
        return None
    return color if color.startswith("#") else f"#{color}"


def normalize_color_for_store(color: Optional[str]) -> Optional[str]:
    # Save colors including leading '#'; rely on YAML quoting when needed.
    if color is None:
        return None
    if not isinstance(color, str):
        return None
    return color if color.startswith("#") else f"#{color}"


def load_color_store(path: str) -> Dict[int, Dict[str, Optional[str]]]:
    """Load color store from YAML file.

    Returns mapping: {entityId: {"name": str|None, "color": str|None}}
    """
    yaml = _ensure_yaml_available()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as infile:
            loaded = yaml.safe_load(infile) or {}
    except Exception:
        return {}
    out: Dict[int, Dict[str, Optional[str]]] = {}
    if not isinstance(loaded, dict):
        return out
    for key, val in loaded.items():
        try:
            eid = int(key)
        except Exception:
            continue
        if not isinstance(val, dict):
            continue
        name = val.get("name")
        color = val.get("color")
        color = normalize_color_for_runtime(color)
        out[eid] = {"name": name, "color": color}
    return out


def save_color_store(path: str, mapping: Dict[int, Dict[str, Optional[str]]]) -> None:
    """Save mapping to YAML file.

    Expects mapping: {entityId: {"name": str|None, "color": str|None}}
    """
    yaml = _ensure_yaml_available()
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    dumpable = {}
    for eid, val in mapping.items():
        name = val.get("name") if isinstance(val, dict) else None
        color = val.get("color") if isinstance(val, dict) else val
        color = normalize_color_for_store(color)
        dumpable[eid] = {"name": name, "color": color}
    with open(path, "w", encoding="utf-8") as outfile:
        yaml.safe_dump(dumpable, outfile, allow_unicode=True, sort_keys=False)
