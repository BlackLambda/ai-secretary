from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def default_recent_focus_path(project_root: Path) -> Path:
    return (project_root / "incremental_data" / "output" / "recent_focus.json").resolve()


def load_recent_focus_report(path: Path) -> dict[str, Any] | None:
    try:
        if not path or not isinstance(path, Path):
            return None
        if not path.exists() or not path.is_file():
            return None
        with path.open("r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _topic_name(topic: Any) -> str:
    if not isinstance(topic, dict):
        return ""
    name = topic.get("name")
    if not isinstance(name, str) or not name.strip():
        name = topic.get("topic")
    return str(name or "").strip()


def extract_active_projects_from_recent_focus(
    report: dict[str, Any] | None,
    *,
    max_topics: int = 6,
    min_confidence: float | None = None,
) -> list[str]:
    """Return a compact list of project/topic names from a recent focus report.

    This is the supported source of project context.
    """
    if not isinstance(report, dict):
        return []

    focus = report.get("focus")
    if not isinstance(focus, dict):
        return []

    topics = focus.get("topics")
    if not isinstance(topics, list):
        return []

    scored: list[tuple[float, int, str]] = []
    for i, t in enumerate(topics):
        if not isinstance(t, dict):
            continue
        name = _topic_name(t)
        if not name:
            continue

        conf = t.get("confidence")
        if min_confidence is not None:
            try:
                if not isinstance(conf, (int, float)) or float(conf) < float(min_confidence):
                    continue
            except Exception:
                continue

        score = t.get("score")
        score_val = 0.0
        if isinstance(score, (int, float)):
            score_val = float(score)

        scored.append((score_val, i, name))

    # Sort by score desc, then original order.
    scored.sort(key=lambda x: (-x[0], x[1]))

    out: list[str] = []
    seen: set[str] = set()
    for _, _, name in scored:
        k = name.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(name)
        if len(out) >= max_topics:
            break

    return out


def resolve_effective_active_projects(
    *,
    recent_focus_path: Path | None,
    user_profile: dict[str, Any] | None,
    max_topics: int = 6,
    min_confidence: float | None = None,
) -> list[str]:
    report = load_recent_focus_report(recent_focus_path) if isinstance(recent_focus_path, Path) else None
    focus_projects = extract_active_projects_from_recent_focus(report, max_topics=max_topics, min_confidence=min_confidence)
    # Recent focus is the sole supported source of project context.
    return focus_projects
