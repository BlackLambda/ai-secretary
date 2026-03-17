import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ai_secretary_core import json_io
from ai_secretary_core.paths import RepoPaths


BASE_DIR = Path(__file__).resolve().parent.parent
PATHS = RepoPaths(BASE_DIR)


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = " ".join(s.split())
    return s.strip().lower()


def _get_item_text(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
    return (item.get("task") or item.get("description") or od.get("task") or od.get("description") or "").strip()


def _fingerprint_payload(payload: dict) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _outlook_container_key(event: dict) -> str:
    name = _norm_text(event.get("event_name") or event.get("summary") or "")
    start = _norm_text(event.get("start_time") or "")
    end = _norm_text(event.get("end_time") or "")
    web = _norm_text(event.get("web_link") or event.get("weblink") or "")
    return "|".join([name, start, end, web])


def _teams_container_key(conversation: dict) -> str:
    chat_id = _norm_text(conversation.get("chat_id") or "")
    conv_id = _norm_text(conversation.get("conversation_id") or "")
    chat_name = _norm_text(conversation.get("chat_name") or "")
    return "|".join([chat_id, conv_id, chat_name])


def _find_action_context_by_ui_id(briefing_data: dict, ui_id: str) -> Optional[Tuple[str, str, str, dict, dict]]:
    """Returns (card_type, container_key, bucket, item_dict, extra_fields)."""
    cards = briefing_data.get("cards")
    if not isinstance(cards, list):
        return None

    for card in cards:
        if not isinstance(card, dict):
            continue
        ctype = card.get("type")
        data = card.get("data")
        if not isinstance(data, dict):
            continue

        if ctype == "Outlook":
            container_key = _outlook_container_key(data)
            for bucket in ("todos", "recommendations"):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict) and str(it.get("_ui_id")) == str(ui_id):
                        extra = {
                            "event_id": data.get("event_id"),
                            "event_name": data.get("event_name"),
                            "web_link": data.get("web_link") or data.get("weblink"),
                            "start_time": data.get("start_time"),
                            "end_time": data.get("end_time"),
                        }
                        return ("Outlook", container_key, bucket, it, extra)

        if ctype == "Teams":
            conv = data.get("conversation") if isinstance(data.get("conversation"), dict) else {}
            container_key = _teams_container_key(conv)
            for bucket in ("linked_items", "unlinked_items"):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict) and str(it.get("_ui_id")) == str(ui_id):
                        extra = {
                            "chat_id": conv.get("chat_id"),
                            "chat_name": conv.get("chat_name"),
                            "conversation_id": conv.get("conversation_id"),
                            "last_updated": conv.get("last_updated") or data.get("last_updated"),
                        }
                        return ("Teams", container_key, bucket, it, extra)

    return None


def _compute_fingerprint(card_type: str, container_key: str, item: dict, bucket: str, op: str) -> str:
    od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
    # Keep fingerprint compatible with server_react.py:
    # - does NOT include op
    # - uses v=1 schema
    base = {
        "v": 1,
        "source": _norm_text(card_type),
        "bucket": _norm_text(bucket),
        "container": _norm_text(container_key),
        "text": _norm_text(_get_item_text(item)),
        "deadline": _norm_text(
            (item.get("deadline") if isinstance(item, dict) else None)
            or od.get("deadline")
            or ""
        ),
        "owner": _norm_text((item.get("owner") if isinstance(item, dict) else None) or od.get("owner") or ""),
    }
    quote = _norm_text((item.get("original_quote") if isinstance(item, dict) else None) or od.get("original_quote") or "")
    if quote:
        base["quote"] = quote
    return _fingerprint_payload(base)


def _compute_fingerprint_minimal(ui_id: str, op: str) -> str:
    # Minimal fallback when we can't find the ui_id in briefing_data.
    # This is less stable across datasets, but ensures the store can be created.
    # Keep it op-agnostic (server fingerprints don't include op).
    return _fingerprint_payload({"v": 0, "ui_id": str(ui_id)})


def _rename_existing(path: str, backup_filename: str) -> Optional[str]:
    try:
        if not path or not os.path.exists(path):
            return None
        out_dir = os.path.dirname(path) or "."
        backup_path = os.path.join(out_dir, backup_filename)
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.replace(path, backup_path)
        return backup_path
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert incremental_data/user_operation.json into user_state/user_ops_store.json"
    )
    parser.add_argument(
        "--user-ops",
        default=str(PATHS.user_operation_file()),
        help="Path to incremental_data/user_operation.json",
    )
    parser.add_argument(
        "--briefing",
        default=str(PATHS.briefing_data_file()),
        help="Optional: briefing_data.json to enrich ops_store context (recommended)",
    )
    parser.add_argument(
        "--store-out",
        default=str(PATHS.user_ops_store_file()),
        help="Where to write user_ops_store.json",
    )
    parser.add_argument(
        "--include-ai",
        action="store_true",
        help="If set, also converts completed_ai/dismissed_ai lists (default: false).",
    )
    parser.add_argument(
        "--backup-existing-store",
        action="store_true",
        help="If set, renames existing store-out to user_ops_backup.json before writing.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.user_ops):
        print(f"[ERROR] user_operation.json not found: {args.user_ops}")
        return 2

    user_ops = json_io.read_json(args.user_ops)
    if not isinstance(user_ops, dict):
        print("[ERROR] user_operation.json must be an object")
        return 2

    briefing: Optional[dict] = None
    if args.briefing and os.path.exists(args.briefing):
        try:
            b = json_io.read_json(args.briefing)
            if isinstance(b, dict):
                briefing = b
        except Exception:
            briefing = None

    # Normalize lists
    completed = [str(x) for x in (user_ops.get("completed") or []) if x]
    dismissed = [str(x) for x in (user_ops.get("dismissed") or []) if x]
    promoted = [str(x) for x in (user_ops.get("promoted") or []) if x]
    if args.include_ai:
        completed += [str(x) for x in (user_ops.get("completed_ai") or []) if x]
        dismissed += [str(x) for x in (user_ops.get("dismissed_ai") or []) if x]

    items: List[Tuple[str, str]] = []
    items += [(ui_id, "complete") for ui_id in completed]
    items += [(ui_id, "dismiss") for ui_id in dismissed]
    items += [(ui_id, "promote") for ui_id in promoted]

    now = _utc_now_iso()

    ops_by_fp: Dict[str, Any] = {}
    collisions = 0
    enriched = 0
    minimal = 0

    for ui_id, op in items:
        ctx_payload: Dict[str, Any] = {"last_seen_ui_id": ui_id}

        fp = None
        if briefing is not None:
            found = _find_action_context_by_ui_id(briefing, ui_id)
            if found:
                card_type, container_key, bucket, item, extra = found
                od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
                ctx_payload = {
                    "card_type": card_type,
                    "bucket": bucket,
                    "container_key": container_key,
                    **(extra or {}),
                    "text": _get_item_text(item),
                    "deadline": (item.get("deadline") or od.get("deadline")),
                    "owner": (item.get("owner") or od.get("owner")),
                    "assignees": (
                        item.get("assignees")
                        if isinstance(item.get("assignees"), list)
                        else od.get("assignees")
                    ),
                    "original_quote": (item.get("original_quote") or od.get("original_quote")),
                    "last_seen_ui_id": ui_id,
                }
                fp = _compute_fingerprint(card_type, container_key, item, bucket, op)
                enriched += 1

        if fp is None:
            fp = _compute_fingerprint_minimal(ui_id, op)
            ctx_payload.setdefault("card_type", None)
            ctx_payload.setdefault("bucket", None)
            minimal += 1

        entry = {
            "op": op,
            "active": True,
            "first_seen": now,
            "last_updated": now,
            "last_seen_ui_id": ui_id,
            "context": ctx_payload,
        }

        # Ensure unique keys even in rare collisions
        if fp in ops_by_fp:
            collisions += 1
            fp = _fingerprint_payload({"fp": fp, "ui_id": ui_id, "op": op, "ts": time.time()})

        ops_by_fp[fp] = entry

    out_obj = {"version": 1, "ops_by_fingerprint": ops_by_fp}

    if args.backup_existing_store:
        backup = _rename_existing(args.store_out, "user_ops_backup.json")
        if backup:
            print(f"[STORE_BACKUP] Renamed existing store to: {backup}")

    json_io.write_json(args.store_out, out_obj)

    print("Done.")
    print(f"  Input user_ops: {args.user_ops}")
    print(f"  Briefing used: {args.briefing if briefing is not None else '(none)'}")
    print(f"  Output store: {args.store_out}")
    print(f"  Items converted: {len(items)}")
    print(f"  Enriched via briefing: {enriched}")
    print(f"  Minimal (no briefing match): {minimal}")
    print(f"  Fingerprint collisions handled: {collisions}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
