from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


_TOKEN_RE = re.compile(r"[a-zA-Z0-9]{2,}")


def _norm_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _iter_str_list(x: Any) -> List[str]:
    if not isinstance(x, list):
        return []
    out: List[str] = []
    for v in x:
        t = _norm_text(v)
        if t:
            out.append(t)
    return out


def _item_text(item: Dict[str, Any]) -> str:
    od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
    desc = _norm_text(item.get("description") or item.get("task") or od.get("task") or od.get("description"))
    if not desc:
        return ""

    parts = [desc]

    deadline = _norm_text(od.get("deadline") or item.get("deadline"))
    if deadline:
        parts.append(f"deadline: {deadline}")

    owner = _norm_text(od.get("owner") or item.get("owner"))
    if owner:
        parts.append(f"owner: {owner}")

    assignees = od.get("assignees") if isinstance(od.get("assignees"), list) else item.get("assignees")
    if isinstance(assignees, list):
        a = ", ".join([_norm_text(x) for x in assignees if _norm_text(x)])
        if a:
            parts.append(f"assignees: {a}")

    rationale = _norm_text(od.get("rationale") or item.get("rationale"))
    if rationale:
        parts.append(f"rationale: {rationale}")

    return " | ".join(parts)


def extract_card_text(card: Dict[str, Any]) -> str:
    """Convert an Outlook/Teams card into a single 'task-level' text.

    This is intentionally card-level (task) rather than per-action item.
    """
    card_type = _norm_text(card.get("type"))
    data = card.get("data") if isinstance(card.get("data"), dict) else {}

    parts: List[str] = []
    parts.append(f"source: {card_type}")

    if card_type.lower() == "outlook":
        event_name = _norm_text(data.get("event_name") or data.get("subject") or data.get("title"))
        if event_name:
            parts.append(f"title: {event_name}")

        event_type = _norm_text(data.get("event_type"))
        if event_type:
            parts.append(f"event_type: {event_type}")

        summary = _norm_text(data.get("executive_summary") or data.get("summary") or data.get("description") or data.get("action_summary"))
        if summary:
            parts.append(f"summary: {summary}")

        participants = _iter_str_list(data.get("key_participants") or data.get("participants") or data.get("attendees"))
        if participants:
            parts.append("participants: " + ", ".join(participants[:30]))

        outcomes = _iter_str_list(data.get("key_outcomes"))
        if outcomes:
            parts.append("outcomes: " + " | ".join(outcomes[:30]))

        items: List[Dict[str, Any]] = []
        for bucket_key in ("todos", "recommendations"):
            v = data.get(bucket_key)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        items.append(it)

        item_lines: List[str] = []
        for it in items:
            t = _item_text(it)
            if t:
                item_lines.append(t)

        if item_lines:
            parts.append("items:\n" + "\n".join(item_lines[:200]))

    elif card_type.lower() == "teams":
        conv = data.get("conversation") if isinstance(data.get("conversation"), dict) else {}

        chat_name = _norm_text(conv.get("chat_name") or conv.get("topic") or conv.get("chatTitle"))
        if chat_name:
            parts.append(f"title: {chat_name}")

        summary = conv.get("summary") if isinstance(conv.get("summary"), dict) else {}
        topic = _norm_text(summary.get("topic"))
        if topic:
            parts.append(f"topic: {topic}")

        action_summary = _norm_text(summary.get("action_summary"))
        if action_summary:
            parts.append(f"summary: {action_summary}")

        key_points = _iter_str_list(summary.get("key_points"))
        if key_points:
            parts.append("key_points: " + " | ".join(key_points[:50]))

        decisions = _iter_str_list(summary.get("decisions_made"))
        if decisions:
            parts.append("decisions: " + " | ".join(decisions[:50]))

        top_participants = _iter_str_list(data.get("top_participants"))
        if top_participants:
            parts.append("participants: " + ", ".join(top_participants[:30]))

        items: List[Dict[str, Any]] = []
        for bucket_key in ("linked_items", "unlinked_items"):
            v = data.get(bucket_key)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        items.append(it)

        # Some pipelines store extracted tasks under conversation.tasks
        conv_tasks = conv.get("tasks")
        if isinstance(conv_tasks, list):
            for it in conv_tasks:
                if isinstance(it, dict):
                    items.append(it)

        item_lines: List[str] = []
        for it in items:
            t = _item_text(it)
            if t:
                item_lines.append(t)

        if item_lines:
            parts.append("items:\n" + "\n".join(item_lines[:200]))

    text = "\n".join([p for p in parts if p])
    return text


def text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_embedding(text: str, dim: int = 512) -> List[float]:
    """Deterministic, dependency-free embedding.

    Uses a hashed bag-of-words with signed buckets + L2 normalization.
    """
    if dim <= 0:
        raise ValueError("dim must be > 0")

    vec = [0.0] * dim
    tokens = [t.lower() for t in _TOKEN_RE.findall(text)]
    if not tokens:
        return vec

    # Term frequency
    tf: Dict[str, int] = {}
    for t in tokens:
        tf[t] = tf.get(t, 0) + 1

    for tok, c in tf.items():
        h = int(hashlib.sha256(tok.encode("utf-8")).hexdigest(), 16)
        idx = h % dim
        sign = -1.0 if ((h >> 1) & 1) else 1.0
        # sublinear TF
        vec[idx] += sign * (1.0 + math.log(1.0 + c))

    # L2 normalize
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def dot(a: List[float], b: List[float]) -> float:
    if len(a) != len(b):
        raise ValueError("vector length mismatch")
    return float(sum(x * y for x, y in zip(a, b)))


def add_scaled(dst: List[float], src: List[float], scale: float) -> None:
    if len(dst) != len(src):
        raise ValueError("vector length mismatch")
    for i in range(len(dst)):
        dst[i] += scale * src[i]


def l2_normalize(v: List[float]) -> List[float]:
    norm = math.sqrt(sum(x * x for x in v))
    if norm <= 0:
        return v
    return [x / norm for x in v]


def compute_focus_vector(
    vectors_by_id: Dict[str, Dict[str, Any]],
    feedback_by_id: Dict[str, str],
    dim: int = 512,
) -> Tuple[List[float], Dict[str, int]]:
    """DEPRECATED: order-independent focus vector from like/dislike feedback.

    This remains for legacy compatibility with stored card feedback data.
    focus = normalize( sum( +v_card for likes ) + sum( -v_card for dislikes ) )
    """
    u = [0.0] * dim
    likes = 0
    dislikes = 0

    for card_id, fb in feedback_by_id.items():
        fb_norm = str(fb or "").lower().strip()
        if fb_norm not in ("like", "dislike"):
            continue

        rec = vectors_by_id.get(card_id)
        vec = rec.get("vector") if isinstance(rec, dict) else None
        if not isinstance(vec, list) or len(vec) != dim:
            continue

        if fb_norm == "like":
            add_scaled(u, vec, 1.0)
            likes += 1
        else:
            add_scaled(u, vec, -1.0)
            dislikes += 1

    u = l2_normalize(u)
    return u, {"likes": likes, "dislikes": dislikes}
