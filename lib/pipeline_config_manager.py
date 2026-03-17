from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple


JsonDict = Dict[str, Any]


def _read_json(path: Path) -> JsonDict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, data: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp_path, path)


def deep_merge(base: Any, override: Any) -> Any:
    """Recursively merge dictionaries.

    - If both values are dicts, merge by key.
    - Otherwise, the override wins (including lists).
    """

    if isinstance(base, dict) and isinstance(override, dict):
        merged: JsonDict = dict(base)
        for k, v in override.items():
            if k in merged:
                merged[k] = deep_merge(merged[k], v)
            else:
                merged[k] = v
        return merged

    return override


def strip_unknown_keys(override: Any, schema: Any) -> Any:
    """Remove keys from override that aren't present in schema (recursively).

    For non-dict schema, the override is accepted as-is.
    """

    if not isinstance(override, dict) or not isinstance(schema, dict):
        return override

    cleaned: JsonDict = {}
    for k, v in override.items():
        if k not in schema:
            continue
        cleaned[k] = strip_unknown_keys(v, schema[k])
    return cleaned


def compute_overrides(effective: Any, defaults: Any) -> Any:
    """Compute a minimal override object such that deep_merge(defaults, overrides) == effective."""

    if isinstance(effective, dict) and isinstance(defaults, dict):
        out: JsonDict = {}
        for k, eff_v in effective.items():
            if k not in defaults:
                # If defaults don't have it, skip to keep override schema-aligned.
                continue
            child = compute_overrides(eff_v, defaults[k])
            if child is None:
                continue
            out[k] = child
        return out or None

    # For non-dicts (including lists), include only if different.
    if effective != defaults:
        return effective
    return None


def get_config_paths(base_dir: Path) -> Tuple[Path, Path, Path]:
    cfg_dir = base_dir / "config"
    default_path = cfg_dir / "pipeline_config.default.json"
    user_path = cfg_dir / "pipeline_config.user.json"
    effective_path = cfg_dir / "pipeline_config.json"
    return default_path, user_path, effective_path


def load_defaults(base_dir: Path) -> JsonDict:
    default_path, _, _ = get_config_paths(base_dir)
    if not default_path.exists():
        return {}
    return _read_json(default_path)


def load_user_overrides(base_dir: Path, defaults: JsonDict) -> JsonDict:
    _, user_path, _ = get_config_paths(base_dir)
    if not user_path.exists():
        return {}
    try:
        overrides = _read_json(user_path)
    except Exception:
        # If user file is corrupt, ignore rather than breaking startup.
        return {}
    if not isinstance(overrides, dict):
        return {}
    return strip_unknown_keys(overrides, defaults)


def ensure_effective_config(base_dir: Path) -> JsonDict:
    """Ensure pipeline_config.json exists and is the merged output of default+user."""

    defaults = load_defaults(base_dir)
    user_overrides = load_user_overrides(base_dir, defaults)

    # Persist cleaned overrides (migration: strip unknown keys)
    # Also ensure the user overrides file exists (create empty {} if missing).
    _, user_path, effective_path = get_config_paths(base_dir)
    if user_path.exists() or user_overrides != {}:
        _write_json_atomic(user_path, user_overrides)
    else:
        _write_json_atomic(user_path, {})

    effective = deep_merge(defaults, user_overrides)
    _write_json_atomic(effective_path, effective)
    return effective


def save_effective_from_updates(base_dir: Path, updates: JsonDict) -> JsonDict:
    """Apply updates to the effective config, persist user overrides, regenerate effective."""

    defaults = load_defaults(base_dir)
    user_overrides = load_user_overrides(base_dir, defaults)
    effective = deep_merge(defaults, user_overrides)

    effective = deep_merge(effective, strip_unknown_keys(updates, defaults))

    overrides = compute_overrides(effective, defaults)
    if overrides is None:
        overrides = {}

    _, user_path, effective_path = get_config_paths(base_dir)
    _write_json_atomic(user_path, overrides)
    _write_json_atomic(effective_path, effective)
    return effective
