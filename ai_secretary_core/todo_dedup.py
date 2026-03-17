import re
from typing import Any, Dict, List, Optional, Tuple


_ws_re = re.compile(r"\s+")
_non_alnum_re = re.compile(r"[^a-z0-9\s]")


def _item_text(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("task") or item.get("description") or "").strip()


def normalize_todo_text(text: str) -> str:
    s = str(text or "").strip().lower()
    s = _ws_re.sub(" ", s)
    s = _non_alnum_re.sub("", s)
    s = _ws_re.sub(" ", s).strip()
    return s


def _info_score(item: Dict[str, Any]) -> int:
    fields = [
        "task",
        "description",
        "rationale",
        "assignment_reason",
        "user_role",
        "deadline",
        "original_quote",
        "original_quote_timestamp",
        "related_outlook_event",
        "connection_reason",
        "last_updated",
    ]
    score = 0
    for f in fields:
        v = item.get(f)
        if isinstance(v, str):
            if v.strip():
                score += 1
        elif v is not None:
            score += 1
    return score


def dedup_todos(items: Any) -> Tuple[List[Dict[str, Any]], int]:
    """Deduplicate todo-like dicts by normalized text.

    - Stable: keeps first occurrence order.
    - If duplicates exist, keeps the item with more filled fields.

    Returns (deduped_items, removed_count).
    """
    if not isinstance(items, list) or not items:
        return ([], 0)

    seen: Dict[str, int] = {}
    out: List[Dict[str, Any]] = []
    removed = 0

    for it in items:
        if not isinstance(it, dict):
            # Keep non-dict entries but don't attempt to dedup.
            out.append(it)  # type: ignore[list-item]
            continue

        text = _item_text(it)
        key = normalize_todo_text(text)
        if not key:
            out.append(it)
            continue

        if key not in seen:
            seen[key] = len(out)
            out.append(it)
            continue

        # Duplicate: keep "better" one.
        existing_idx = seen[key]
        existing = out[existing_idx]
        if isinstance(existing, dict) and _info_score(it) > _info_score(existing):
            out[existing_idx] = it
        removed += 1

    # Filter type to list of dicts for callers; keep only dicts.
    dicts_only = [x for x in out if isinstance(x, dict)]
    return (dicts_only, removed)
