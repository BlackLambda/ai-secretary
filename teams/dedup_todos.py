import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def load_json(filepath: str):
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data: Any, filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _iter_conversations(data: Any):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("results", [])
    return []


def _item_text(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("task") or item.get("description") or "").strip()


def _collect_ai_removed(conv: dict) -> list[dict]:
    removed: list[dict] = []
    for key in ("removed_tasks", "removed_recommended_actions"):
        arr = conv.get(key)
        if not isinstance(arr, list):
            continue
        for it in arr:
            if isinstance(it, dict):
                removed.append(it)
    return removed


def _priority_value(item: dict) -> float:
    raw = item.get("priority_score")
    try:
        return float(raw)
    except Exception:
        return 0.0


def _enforce_max_todos(items: list[dict], *, max_todos: int) -> tuple[list[dict], int]:
    if max_todos <= 0:
        return ([], len(items))
    if len(items) <= max_todos:
        return (items, 0)

    # Keep the highest priority_score items. If missing, treat as 0.
    # Preserve stable ordering among equal scores.
    indexed = list(enumerate(items))
    indexed.sort(key=lambda x: (_priority_value(x[1]), -x[0]), reverse=True)
    keep = indexed[:max_todos]
    keep_indices = {i for i, _ in keep}
    kept = [it for i, it in enumerate(items) if i in keep_indices]
    removed = len(items) - len(kept)
    return (kept, removed)


def _check_max_todos(items: list[dict], *, max_todos: int) -> bool:
    return len(items) <= max_todos


def _resolve_conversations_dir(input_path: Path, explicit: str | None) -> Path | None:
    if explicit:
        p = Path(explicit)
        return p if p.exists() and p.is_dir() else None

    try:
        p = input_path.resolve()
    except Exception:
        p = input_path

    parent = p.parent
    cand = parent / "master_teams_conversations"
    if cand.exists() and cand.is_dir():
        return cand

    if parent.name.lower() == "teams_analysis":
        cand2 = parent.parent / "teams_conversations"
        if cand2.exists() and cand2.is_dir():
            return cand2

    return None


def _load_conversation_messages(conversations_dir: Path | None, conv: dict, *, max_messages: int) -> list[dict]:
    if not conversations_dir:
        return []
    filename = str(conv.get("conversation_file") or "").strip()
    if not filename:
        return []
    path = conversations_dir / filename
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return []

    msgs = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(msgs, list):
        return []

    out: list[dict] = []
    for m in msgs[-max_messages:]:
        if not isinstance(m, dict):
            continue
        out.append(
            {
                "timestamp": m.get("timestamp"),
                "sender": m.get("sender_name") or m.get("sender") or m.get("from"),
                "subject": m.get("subject"),
                "content": str(m.get("content") or "")[:800],
            }
        )
    return out


def _ai_dedup_items(*, conv: dict, messages: list[dict], items: list[dict], client: object) -> tuple[list[dict], int]:
    # Local import so "fast mode" doesn't require AI deps/config.
    from lib.ai_utils import ai_chat_json

    items_for_model = []
    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        items_for_model.append(
            {
                "id": idx,
                "description": _item_text(it),
                "user_role": it.get("user_role"),
                "priority_score": it.get("priority_score"),
                "original_quote": it.get("original_quote"),
                "original_quote_timestamp": it.get("original_quote_timestamp"),
                "deadline": it.get("deadline"),
                "assigned_to": it.get("assigned_to"),
                "assigned_by": it.get("assigned_by"),
            }
        )

    system = (
        "You are deduplicating extracted TODO items for a single Microsoft Teams conversation. "
        "Output a clean list of CURRENT items only. Remove duplicates, items superseded by newer updates, "
        "and items already completed/resolved based on the conversation messages. "
        "If two items refer to the same underlying work, keep the more specific/outcome-focused one. "
        "Meta followups like confirm/receipt/ack often duplicate a concrete request; drop the meta followup unless truly distinct. "
        "Hard constraint: the final kept todo count MUST be <= 3. If there are more than 3 candidate items, keep the highest priority ones. "
        "Only output JSON."
    )

    user_obj = {
        "conversation": {
            "conversation_id": conv.get("conversation_id"),
            "chat_name": conv.get("chat_name"),
            "summary": conv.get("summary") or conv.get("conversation_summary"),
        },
        "messages": messages,
        "items": items_for_model,
        "already_removed": _collect_ai_removed(conv),
        "constraints": {
            "max_todos": 3,
        },
        "output_schema": {
            "keep_ids": [0],
            "remove": [{"id": 1, "reason": "duplicate/superseded/completed", "evidence": "quote"}],
        },
    }

    data = ai_chat_json(
        client,
        [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_obj, ensure_ascii=False)},
        ],
        temperature=0.0,
    )
    keep_ids = data.get("keep_ids")
    if not isinstance(keep_ids, list):
        raise ValueError("AI response missing keep_ids")

    keep_set: set[int] = set()
    for x in keep_ids:
        try:
            keep_set.add(int(x))
        except Exception:
            continue

    kept: list[dict] = []
    removed = 0
    for idx, it in enumerate(items):
        if idx in keep_set:
            kept.append(it)
        else:
            removed += 1

    return (kept, removed)


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate Teams todos using AI, with a hard max-todos cap.")
    parser.add_argument("--input", required=True, help="Path to Teams analysis summary JSON")
    parser.add_argument("--output", required=True, help="Path to write updated JSON")
    parser.add_argument(
        "--conversations-dir",
        required=False,
        default=None,
        help="Optional folder containing the conversation JSON files referenced by conversation_file.",
    )
    parser.add_argument(
        "--max-messages",
        required=False,
        type=int,
        default=40,
        help="Max recent messages to pass to AI for dedup decisions (default 40)",
    )
    parser.add_argument(
        "--max-todos",
        required=False,
        type=int,
        default=3,
        help="Max todos per conversation after dedup (default 3)",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        help="Disable AI and only apply the max-todos cap (not recommended).",
    )
    args = parser.parse_args()

    data = load_json(args.input)
    if data is None:
        return 2

    convs = _iter_conversations(data)
    if not isinstance(convs, list):
        print("[ERROR] Expected a list of conversations")
        return 2

    input_path = Path(args.input)
    conversations_dir = _resolve_conversations_dir(input_path, args.conversations_dir)

    total_removed = 0
    total_ai_removed = 0
    total_cap_removed = 0
    touched = 0

    # Create AI client once (outside the loop) to avoid re-init per conversation.
    ai_client = None
    need_ai = not args.no_ai and any(
        isinstance(c, dict) and (c.get('todos') or c.get('tasks') or c.get('recommended_actions'))
        for c in convs
    )
    if need_ai:
        from lib.ai_utils import get_azure_openai_client
        ai_client = get_azure_openai_client()

    for conv in convs:
        if not isinstance(conv, dict):
            continue

        todos_in = conv.get("todos") if isinstance(conv.get("todos"), list) else []
        tasks_in = conv.get("tasks") if isinstance(conv.get("tasks"), list) else []
        recs_in = conv.get("recommended_actions") if isinstance(conv.get("recommended_actions"), list) else []

        combined = [*todos_in, *tasks_in, *recs_in]
        combined_items = [it for it in combined if isinstance(it, dict)]

        deduped = combined_items
        ai_removed = 0
        cap_removed = 0

        # AI-only dedup.
        if not args.no_ai and deduped and ai_client is not None:
            msgs = _load_conversation_messages(
                conversations_dir,
                conv,
                max_messages=max(5, int(args.max_messages)),
            )
            try:
                deduped2, ai_removed2 = _ai_dedup_items(conv=conv, messages=msgs, items=deduped, client=ai_client)
                deduped = deduped2
                ai_removed += ai_removed2
            except Exception as e:
                print(f"[WARN] AI dedup failed for {conv.get('conversation_id','')}: {e}. Keeping original items.")

        # Hard cap (applies even if AI was disabled or failed).
        if isinstance(deduped, list) and deduped:
            deduped, cap_removed = _enforce_max_todos(deduped, max_todos=int(args.max_todos))
            if not _check_max_todos(deduped, max_todos=int(args.max_todos)):
                # Extremely defensive: should never happen.
                deduped = deduped[: int(args.max_todos)]

        conv["todos"] = deduped
        conv["tasks"] = []
        conv["recommended_actions"] = []
        conv["todo_dedup_stats"] = {
            "removed_by_ai": int(ai_removed),
            "removed_by_cap": int(cap_removed),
            "max_todos": int(args.max_todos),
            "kept": int(len(deduped) if isinstance(deduped, list) else 0),
        }

        if ai_removed or cap_removed:
            total_ai_removed += int(ai_removed)
            total_cap_removed += int(cap_removed)
            touched += 1

    if isinstance(data, dict):
        data["generated_at"] = datetime.now(timezone.utc).isoformat()

    save_json(data, args.output)
    print(
        f"[OK] Deduped todos in {touched} conversations (removed ai={total_ai_removed}, cap={total_cap_removed}, max={int(args.max_todos)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
