from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def read_json(path: str | Path) -> Any:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(
    path: str | Path,
    obj: Any,
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
) -> None:
    p = Path(path)
    if p.parent and str(p.parent) not in (".", ""):
        p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, indent=indent, ensure_ascii=ensure_ascii)
        f.write("\n")


def write_json_atomic(
    path: str | Path,
    obj: Any,
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, indent=indent, ensure_ascii=ensure_ascii)
        f.write("\n")
    os.replace(tmp, p)


def load_json_best_effort(
    relative_or_absolute_path: str | Path,
    default: Any,
    *,
    base_dir: str | Path | None = None,
) -> Any:
    try:
        p = Path(relative_or_absolute_path)
        if base_dir is not None:
            p = Path(base_dir) / p
        if not p.exists():
            return default
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_best_effort(
    relative_or_absolute_path: str | Path,
    obj: Any,
    *,
    base_dir: str | Path | None = None,
) -> None:
    try:
        p = Path(relative_or_absolute_path)
        if base_dir is not None:
            p = Path(base_dir) / p
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
            f.write("\n")
    except Exception:
        pass
