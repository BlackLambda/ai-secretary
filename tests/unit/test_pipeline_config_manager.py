"""Unit tests for lib/pipeline_config_manager.py"""

import json
import sys
from pathlib import Path

import pytest

# Ensure repo root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from lib.pipeline_config_manager import (
    deep_merge,
    strip_unknown_keys,
    ensure_effective_config,
    save_effective_from_updates,
    get_config_paths,
)


def test_deep_merge_basic():
    base = {"a": 1, "b": {"x": 10, "y": 20}}
    override = {"b": {"y": 99}, "c": 3}
    result = deep_merge(base, override)
    assert result == {"a": 1, "b": {"x": 10, "y": 99}, "c": 3}


def test_deep_merge_list_override():
    base = {"a": [1, 2, 3]}
    override = {"a": [4, 5]}
    assert deep_merge(base, override)["a"] == [4, 5]


def test_strip_unknown_keys():
    schema = {"a": 1, "b": {"x": 0}}
    override = {"a": 5, "b": {"x": 99, "unknown": "drop"}, "extra": "drop"}
    result = strip_unknown_keys(override, schema)
    assert result == {"a": 5, "b": {"x": 99}}


def test_get_config_paths(tmp_path):
    default_path, user_path, effective_path = get_config_paths(tmp_path)
    assert default_path == tmp_path / "config" / "pipeline_config.default.json"
    assert user_path == tmp_path / "config" / "pipeline_config.user.json"
    assert effective_path == tmp_path / "config" / "pipeline_config.json"


def test_ensure_effective_config_creates_files(tmp_path):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    default = {"ai_backend": "azure", "timeout": 20}
    (cfg_dir / "pipeline_config.default.json").write_text(json.dumps(default))

    result = ensure_effective_config(tmp_path)
    assert result["ai_backend"] == "azure"
    assert (cfg_dir / "pipeline_config.json").exists()


def test_ensure_effective_config_merges_user_override(tmp_path):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    default = {"ai_backend": "azure", "timeout": 20}
    user = {"timeout": 60}
    (cfg_dir / "pipeline_config.default.json").write_text(json.dumps(default))
    (cfg_dir / "pipeline_config.user.json").write_text(json.dumps(user))

    result = ensure_effective_config(tmp_path)
    assert result["timeout"] == 60
    assert result["ai_backend"] == "azure"


def test_save_effective_from_updates(tmp_path):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    default = {"ai_backend": "azure", "timeout": 20}
    (cfg_dir / "pipeline_config.default.json").write_text(json.dumps(default))

    updated = save_effective_from_updates(tmp_path, {"timeout": 45})
    assert updated["timeout"] == 45
    assert (cfg_dir / "pipeline_config.user.json").exists()
