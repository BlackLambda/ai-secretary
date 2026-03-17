import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _sanitize_id_part(raw: Any) -> str:
    """Match server_react.py's stable card key sanitization."""
    s = str(raw or "").strip()
    if not s:
        return "unknown"
    s = "".join(ch if (ch.isalnum() or ch in "_-") else "-" for ch in s)
    while "--" in s:
        s = s.replace("--", "-")
    s = s.strip("-")
    return s[:120] if s else "unknown"


def _load_user_ops(incremental_dir: Path) -> Dict[str, Any]:
    ops = _load_json(incremental_dir / "user_operation.json", default={})
    return ops if isinstance(ops, dict) else {}


def _pinned_card_keys(user_ops: Dict[str, Any]) -> Dict[str, set]:
    """Return pinned keys grouped by type ('outlook', 'teams')."""
    pinned = user_ops.get("pinned_cards", [])
    if not isinstance(pinned, list):
        pinned = []
    by_type: Dict[str, set] = {"outlook": set(), "teams": set()}
    for raw in pinned:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if "|" not in s:
            continue
        t, k = s.split("|", 1)
        t = t.strip().lower()
        k = k.strip()
        if t in by_type and k:
            by_type[t].add(k)
    return by_type


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _parse_ts(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    try:
        # Normalize Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _thread_last_update(thread: Dict[str, Any]) -> Optional[datetime]:
    # Prefer newest message ReceivedDateTime (quote time).
    # Rationale: pipeline may update latest_received when re-processing, which would
    # incorrectly keep very old threads from being pruned.
    messages = thread.get("messages", []) or []
    best: Optional[datetime] = None
    for m in messages:
        md = _parse_ts(m.get("ReceivedDateTime"))
        if md and (best is None or md > best):
            best = md
    if best:
        return best

    # Fallback: latest_received
    return _parse_ts(thread.get("latest_received"))


def _teams_conv_last_update(conv: Dict[str, Any]) -> Optional[datetime]:
    # Prefer newest message timestamp (quote time).
    # Rationale: merge steps can bump last_message_time even without new content.
    messages = conv.get("messages", []) or []
    best: Optional[datetime] = None
    for m in messages:
        md = _parse_ts(m.get("timestamp"))
        if md and (best is None or md > best):
            best = md
    if best:
        return best

    # Fallback: last_message_time
    return _parse_ts(conv.get("last_message_time"))


def _event_last_update(event: Dict[str, Any]) -> Optional[datetime]:
    # Prefer start_time/end_time as a proxy for the underlying quote time.
    # last_updated is often the pipeline processing time and should not extend retention.
    for key in ("start_time", "end_time"):
        dt = _parse_ts(event.get(key))
        if dt:
            return dt
    return _parse_ts(event.get("last_updated"))


def prune_threads(
    master_threads_file: Path,
    cutoff: datetime,
    protected_thread_ids: Optional[set] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    threads = _load_json(master_threads_file, default=[])
    if not isinstance(threads, list):
        return [], []

    protected_thread_ids = protected_thread_ids or set()

    kept: List[Dict[str, Any]] = []
    pruned: List[Dict[str, Any]] = []
    for t in threads:
        tid = t.get("id")
        if tid and tid in protected_thread_ids:
            kept.append(t)
            continue
        ts = _thread_last_update(t)
        if ts and ts < cutoff:
            pruned.append(t)
        else:
            kept.append(t)

    _save_json(master_threads_file, kept)
    pruned_ids = [t.get("id") for t in pruned if t.get("id")]
    return pruned, pruned_ids


def prune_teams_conversation_messages(
    master_teams_dir: Path,
    cutoff: datetime,
    protected_team_keys: Optional[set] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Prune old messages inside conversation files.

    - Removes messages with timestamp < cutoff.
    - Recomputes first_message_time / last_message_time / message_count.
    - Deletes the whole conversation file only if no messages remain.
    """
    if not master_teams_dir.exists():
        return {"conversations_touched": 0, "messages_pruned": 0, "conversations_deleted": 0, "details": []}, []

    pruned_conversation_ids: List[str] = []
    summary: Dict[str, Any] = {"conversations_touched": 0, "messages_pruned": 0, "conversations_deleted": 0, "details": []}

    protected_team_keys = protected_team_keys or set()

    for file_path in master_teams_dir.glob("conversation_*.json"):
        data = _load_json(file_path, default=None)
        if not isinstance(data, dict):
            continue

        conv_id = data.get("conversation_id")
        conv_key = _sanitize_id_part(conv_id)
        if conv_key in protected_team_keys:
            # Pinned: never prune raw messages for this conversation.
            continue
        messages = data.get("messages", []) or []
        if not isinstance(messages, list) or not messages:
            continue

        kept_messages: List[Dict[str, Any]] = []
        pruned_messages: List[Dict[str, Any]] = []
        for m in messages:
            ts = _parse_ts(m.get("timestamp"))
            if ts and ts < cutoff:
                pruned_messages.append(m)
            else:
                kept_messages.append(m)

        if not pruned_messages:
            continue

        summary["conversations_touched"] += 1
        summary["messages_pruned"] += len(pruned_messages)

        if not kept_messages:
            summary["conversations_deleted"] += 1
            if conv_id:
                pruned_conversation_ids.append(conv_id)
            summary["details"].append({
                "conversation_id": conv_id,
                "file": str(file_path),
                "deleted": True,
                "pruned_messages": len(pruned_messages),
                "remaining_messages": 0,
            })
            try:
                file_path.unlink()
            except Exception:
                pass
            continue

        kept_messages.sort(key=lambda x: x.get("timestamp", ""))
        data["messages"] = kept_messages
        data["message_count"] = len(kept_messages)
        data["first_message_time"] = kept_messages[0].get("timestamp")
        data["last_message_time"] = kept_messages[-1].get("timestamp")
        _save_json(file_path, data)

        summary["details"].append({
            "conversation_id": conv_id,
            "file": str(file_path),
            "deleted": False,
            "pruned_messages": len(pruned_messages),
            "remaining_messages": len(kept_messages),
            "new_last_message_time": data.get("last_message_time"),
        })

    return summary, pruned_conversation_ids


def prune_teams_conversations(master_teams_dir: Path, cutoff: datetime) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Deprecated whole-file prune; kept for compatibility."""
    return [], []


def prune_outlook_events_by_threads(
    master_events_file: Path,
    cutoff: datetime,
    pruned_thread_ids: set,
    protected_event_ids: Optional[set] = None,
) -> Tuple[Dict[str, Any], List[str], Dict[str, Any]]:
    """Prune Outlook events based on related_thread_ids.

    Behavior:
    - Removes obsolete thread IDs from event.related_thread_ids if those threads were pruned.
    - Drops the event only if ALL its related_thread_ids were pruned (i.e., none remain).
    - Also supports time-based prune if an event has no related_thread_ids: uses last_updated < cutoff.
    """
    payload = _load_json(master_events_file, default={})
    if not isinstance(payload, dict):
        return {"events_dropped": 0, "events_touched": 0, "thread_refs_removed": 0, "details": []}, [], payload

    events = payload.get("events", [])
    if not isinstance(events, list):
        events = []

    kept: List[Dict[str, Any]] = []
    dropped_ids: List[str] = []
    summary: Dict[str, Any] = {"events_dropped": 0, "events_touched": 0, "thread_refs_removed": 0, "details": []}

    protected_event_ids = protected_event_ids or set()

    for e in events:
        eid = e.get("event_id")

        # Pinned: never consider obsolete/dropped.
        if eid and eid in protected_event_ids:
            kept.append(e)
            continue

        related = e.get("related_thread_ids", [])
        if isinstance(related, list) and related:
            related_set = {x for x in related if isinstance(x, str)}
            remaining = sorted(list(related_set - pruned_thread_ids))
            removed = sorted(list(related_set & pruned_thread_ids))
            if removed:
                summary["events_touched"] += 1
                summary["thread_refs_removed"] += len(removed)
                e["related_thread_ids"] = remaining
                summary["details"].append({
                    "event_id": eid,
                    "removed_thread_ids": removed,
                    "remaining_thread_ids": remaining,
                    "dropped": False,
                })

            if not remaining:
                summary["events_dropped"] += 1
                if eid:
                    dropped_ids.append(eid)
                # Mark as dropped (not kept)
                if removed or related_set:
                    summary["details"].append({
                        "event_id": eid,
                        "removed_thread_ids": removed,
                        "remaining_thread_ids": [],
                        "dropped": True,
                    })
                continue

            kept.append(e)
            continue

        # No related threads: fall back to last_updated cutoff
        ts = _event_last_update(e)
        if ts and ts < cutoff:
            summary["events_dropped"] += 1
            if eid:
                dropped_ids.append(eid)
            summary["details"].append({"event_id": eid, "dropped": True, "reason": "last_updated_before_cutoff"})
        else:
            kept.append(e)

    payload["events"] = kept
    _save_json(master_events_file, payload)
    return summary, dropped_ids, payload


def prune_teams_summary(master_summary_file: Path, pruned_conversation_ids: set) -> int:
    if not master_summary_file.exists():
        return 0

    summary = _load_json(master_summary_file, default=None)
    if summary is None:
        return 0

    # Handle either list or dict wrapper with results
    if isinstance(summary, list):
        before = len(summary)
        summary = [c for c in summary if c.get("conversation_id") not in pruned_conversation_ids]
        _save_json(master_summary_file, summary)
        return before - len(summary)

    if isinstance(summary, dict):
        results = summary.get("results", [])
        if not isinstance(results, list):
            return 0
        before = len(results)
        summary["results"] = [c for c in results if c.get("conversation_id") not in pruned_conversation_ids]
        _save_json(master_summary_file, summary)
        return before - len(summary["results"])

    return 0


def _prune_teams_analysis_items_in_conversation(conv: Dict[str, Any], cutoff: datetime) -> Dict[str, int]:
    """Prune action items inside a Teams analysis conversation entry.

    Newer summaries may include a unified `todos` list (tasks + recommended_actions). We prune that as well,
    while keeping legacy counters for backward compatibility in prune logs.
    """
    stats = {"tasks_pruned": 0, "recommended_actions_pruned": 0, "todos_pruned": 0}

    todos = conv.get("todos", [])
    if isinstance(todos, list) and todos:
        kept = []
        for t in todos:
            ts = _parse_ts((t or {}).get("last_updated") if isinstance(t, dict) else None)
            if ts and ts < cutoff:
                stats["todos_pruned"] += 1
            else:
                kept.append(t)
        conv["todos"] = kept

    tasks = conv.get("tasks", [])
    if isinstance(tasks, list) and tasks:
        kept = []
        for t in tasks:
            ts = _parse_ts(t.get("last_updated"))
            if ts and ts < cutoff:
                stats["tasks_pruned"] += 1
            else:
                kept.append(t)
        conv["tasks"] = kept

    actions = conv.get("recommended_actions", [])
    if isinstance(actions, list) and actions:
        kept = []
        for a in actions:
            ts = _parse_ts(a.get("last_updated"))
            if ts and ts < cutoff:
                stats["recommended_actions_pruned"] += 1
            else:
                kept.append(a)
        conv["recommended_actions"] = kept

    # Also update conv-level last_updated if it exists and is now older than cutoff.
    # We won't drop the conversation solely for this; instead, keep it but let its items be pruned.
    return stats


def prune_teams_analysis_summary_items(
    master_summary_file: Path,
    cutoff: datetime,
    pruned_conversation_ids: set,
    protected_team_keys: Optional[set] = None,
) -> Dict[str, Any]:
    """Prune analysis items for Teams conversations.

    - Removes whole conversation entries whose conversation_id is in pruned_conversation_ids.
    - For remaining conversations, prunes tasks/recommended_actions with last_updated < cutoff.
    """
    result = {
        "conversations_removed": 0,
        "conversations_touched": 0,
        "tasks_pruned": 0,
        "recommended_actions_pruned": 0,
        "todos_pruned": 0,
        "details": [],
    }

    if not master_summary_file.exists():
        return result

    data = _load_json(master_summary_file, default=None)
    if data is None:
        return result

    protected_team_keys = protected_team_keys or set()

    def _is_pinned_conv(conv_obj: Dict[str, Any]) -> bool:
        cid = conv_obj.get("conversation_id")
        chat_id = conv_obj.get("chat_id") or conv_obj.get("chatId")
        chat_name = conv_obj.get("chat_name")
        key = _sanitize_id_part(cid or chat_id or chat_name)
        return key in protected_team_keys

    if isinstance(data, list):
        new_list = []
        for conv in data:
            if not isinstance(conv, dict):
                continue
            cid = conv.get("conversation_id")
            if cid in pruned_conversation_ids:
                result["conversations_removed"] += 1
                continue

            if _is_pinned_conv(conv):
                # Pinned: never prune tasks/actions inside this conversation.
                new_list.append(conv)
                continue

            stats = _prune_teams_analysis_items_in_conversation(conv, cutoff)
            if stats["tasks_pruned"] or stats["recommended_actions_pruned"] or stats.get("todos_pruned"):
                result["conversations_touched"] += 1
                result["tasks_pruned"] += stats["tasks_pruned"]
                result["recommended_actions_pruned"] += stats["recommended_actions_pruned"]
                result["todos_pruned"] += stats.get("todos_pruned", 0)
                result["details"].append({
                    "conversation_id": cid,
                    **stats,
                })
            new_list.append(conv)

        _save_json(master_summary_file, new_list)
        return result

    if isinstance(data, dict):
        results = data.get("results", [])
        if not isinstance(results, list):
            return result

        new_results = []
        for conv in results:
            if not isinstance(conv, dict):
                continue
            cid = conv.get("conversation_id")
            if cid in pruned_conversation_ids:
                result["conversations_removed"] += 1
                continue

            if _is_pinned_conv(conv):
                new_results.append(conv)
                continue
            stats = _prune_teams_analysis_items_in_conversation(conv, cutoff)
            if stats["tasks_pruned"] or stats["recommended_actions_pruned"] or stats.get("todos_pruned"):
                result["conversations_touched"] += 1
                result["tasks_pruned"] += stats["tasks_pruned"]
                result["recommended_actions_pruned"] += stats["recommended_actions_pruned"]
                result["todos_pruned"] += stats.get("todos_pruned", 0)
                result["details"].append({"conversation_id": cid, **stats})
            new_results.append(conv)

        data["results"] = new_results
        _save_json(master_summary_file, data)
        return result

    return result


def write_prune_log(prune_log_file: Path, entry: Dict[str, Any]) -> None:
    log = _load_json(prune_log_file, default=[])
    if not isinstance(log, list):
        log = []
    log.append(entry)
    _save_json(prune_log_file, log)


YELLOW = "\033[93m"
RESET = "\033[0m"


def _yellow(text: str) -> str:
    # Match existing repo convention (see apply_filters_to_existing_data.py, outlook_v2/*)
    return f"{YELLOW}{text}{RESET}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Prune old master threads/events/teams conversations.")
    parser.add_argument("--incremental-dir", default="incremental_data", help="Base incremental_data directory")
    parser.add_argument("--user-alias", required=True, help="User alias used in master events filename")
    parser.add_argument("--days", type=int, default=3, help="Prune items older than N days")
    parser.add_argument(
        "--threads-file",
        help="Optional: prune this threads JSON file instead of the master (e.g., incremental_data/outlook/threads_{index}.json)",
    )
    parser.add_argument(
        "--teams-dir",
        help="Optional: prune conversation_*.json under this directory instead of the master teams directory",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute what would be pruned without modifying files")
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Do not append an entry to incremental_data/output/prune_log.json",
    )
    args = parser.parse_args()

    base = Path(args.incremental_dir)

    master_threads = Path(args.threads_file) if args.threads_file else (base / "outlook" / "master_threads.json")
    master_events = base / "outlook" / f"master_outlook_events_{args.user_alias}.json"
    master_teams_dir = Path(args.teams_dir) if args.teams_dir else (base / "teams" / "master_teams_conversations")
    master_teams_summary = base / "teams" / "master_teams_analysis_summary.json"

    prune_log = base / "output" / "prune_log.json"

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.days)

    user_ops = _load_user_ops(base)
    pinned_keys = _pinned_card_keys(user_ops)
    pinned_outlook_keys = pinned_keys.get("outlook", set())
    pinned_teams_keys = pinned_keys.get("teams", set())

    protected_thread_ids: set = set()
    protected_event_ids: set = set()

    # Map pinned Outlook card keys -> underlying event_id + related_thread_ids.
    if pinned_outlook_keys and master_events.exists():
        events_payload = _load_json(master_events, default={})
        events = events_payload.get("events", []) if isinstance(events_payload, dict) else []
        if isinstance(events, list):
            for e in events:
                if not isinstance(e, dict):
                    continue
                eid = e.get("event_id")
                if not eid:
                    continue
                ek = _sanitize_id_part(eid)
                if ek not in pinned_outlook_keys:
                    continue
                protected_event_ids.add(eid)
                rel = e.get("related_thread_ids", [])
                if isinstance(rel, list):
                    protected_thread_ids.update({x for x in rel if isinstance(x, str) and x})

    entry: Dict[str, Any] = {
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "cutoff": cutoff.isoformat().replace("+00:00", "Z"),
        "days": args.days,
        "dry_run": bool(args.dry_run),
        "pruned": {
            "threads": {"count": 0, "ids": []},
            "teams_conversations": {"count": 0, "ids": []},
            "teams_messages": {"conversations_touched": 0, "messages_pruned": 0, "conversations_deleted": 0},
            "outlook_events": {"count": 0, "ids": []},
            "outlook_event_thread_refs": {"events_touched": 0, "thread_refs_removed": 0, "events_dropped": 0},
            "teams_summary": {"removed_count": 0},
            "teams_summary_items": {"tasks_pruned": 0, "recommended_actions_pruned": 0},
        },
    }

    if args.dry_run:
        # Load and compute without saving
        threads = _load_json(master_threads, default=[])
        pruned_threads = (
            [
                t
                for t in threads
                if (
                    t.get("id") not in protected_thread_ids
                    and (_thread_last_update(t) and _thread_last_update(t) < cutoff)
                )
            ]
            if isinstance(threads, list)
            else []
        )

        conv_pruned_ids: List[str] = []
        conv_pruned_meta: List[Dict[str, Any]] = []
        if master_teams_dir.exists():
            for fp in master_teams_dir.glob("conversation_*.json"):
                data = _load_json(fp, default=None)
                if not isinstance(data, dict):
                    continue
                ts = _teams_conv_last_update(data)
                if ts and ts < cutoff:
                    cid = data.get("conversation_id")
                    if _sanitize_id_part(cid) in pinned_teams_keys:
                        continue
                    if cid:
                        conv_pruned_ids.append(cid)
                    conv_pruned_meta.append({"conversation_id": cid, "file": str(fp)})

        pruned_thread_ids_set = {t.get("id") for t in pruned_threads if t.get("id")}

        events_payload = _load_json(master_events, default={})
        events = events_payload.get("events", []) if isinstance(events_payload, dict) else []
        pruned_events: List[Dict[str, Any]] = []
        if isinstance(events, list):
            for e in events:
                eid = e.get("event_id")
                if eid and eid in protected_event_ids:
                    continue
                rel = e.get("related_thread_ids", [])
                if isinstance(rel, list) and rel:
                    remaining = [x for x in rel if x not in pruned_thread_ids_set]
                    if not remaining:
                        pruned_events.append(e)
                else:
                    ts = _event_last_update(e)
                    if ts and ts < cutoff:
                        pruned_events.append(e)

        entry["pruned"]["threads"] = {"count": len(pruned_threads), "ids": [t.get("id") for t in pruned_threads if t.get("id")]}
        entry["pruned"]["teams_conversations"] = {"count": len(conv_pruned_meta), "ids": conv_pruned_ids}
        entry["pruned"]["outlook_events"] = {"count": len(pruned_events), "ids": [e.get("event_id") for e in pruned_events if e.get("event_id")]}
        entry["pruned"]["teams_summary"] = {"removed_count": 0}
        entry["pruned"]["teams_summary_items"] = {"tasks_pruned": 0, "recommended_actions_pruned": 0}
        if not args.no_log:
            write_prune_log(prune_log, entry)
        print(json.dumps(entry, indent=2))
        return 0

    pruned_threads, pruned_thread_ids = (
        prune_threads(master_threads, cutoff, protected_thread_ids=protected_thread_ids)
        if master_threads.exists()
        else ([], [])
    )

    teams_msg_summary, pruned_conv_ids = prune_teams_conversation_messages(
        master_teams_dir,
        cutoff,
        protected_team_keys=pinned_teams_keys,
    )
    # Back-compat fields
    pruned_convs_meta: List[Dict[str, Any]] = []

    pruned_thread_ids_set = set(pruned_thread_ids)
    # Only prune master events if we're pruning the master threads file.
    # For delta threads pruning (threads_{index}.json), we should NOT mutate master events.
    is_master_threads_target = args.threads_file is None
    if is_master_threads_target and master_events.exists():
        event_summary, pruned_event_ids, _updated_payload = prune_outlook_events_by_threads(
            master_events,
            cutoff,
            pruned_thread_ids_set,
            protected_event_ids=protected_event_ids,
        )
    else:
        event_summary, pruned_event_ids = {"events_dropped": 0, "events_touched": 0, "thread_refs_removed": 0, "details": []}, []

    # Only prune master teams analysis summary when pruning the master teams directory.
    is_master_teams_target = args.teams_dir is None
    if is_master_teams_target:
        analysis_summary_stats = prune_teams_analysis_summary_items(
            master_teams_summary,
            cutoff,
            set(pruned_conv_ids),
            protected_team_keys=pinned_teams_keys,
        )
        removed_from_summary = analysis_summary_stats.get("conversations_removed", 0)
    else:
        analysis_summary_stats = {"conversations_removed": 0, "details": [], "tasks_pruned": 0, "recommended_actions_pruned": 0}
        removed_from_summary = 0

    entry["pruned"]["threads"] = {"count": len(pruned_threads), "ids": pruned_thread_ids}
    entry["pruned"]["teams_conversations"] = {"count": len(pruned_convs_meta), "ids": pruned_conv_ids}
    entry["pruned"]["teams_messages"] = {
        "conversations_touched": teams_msg_summary.get("conversations_touched", 0),
        "messages_pruned": teams_msg_summary.get("messages_pruned", 0),
        "conversations_deleted": teams_msg_summary.get("conversations_deleted", 0),
    }
    entry["pruned"]["outlook_events"] = {"count": len(pruned_event_ids), "ids": pruned_event_ids}
    entry["pruned"]["outlook_event_thread_refs"] = {
        "events_touched": event_summary.get("events_touched", 0),
        "thread_refs_removed": event_summary.get("thread_refs_removed", 0),
        "events_dropped": event_summary.get("events_dropped", 0),
    }
    entry["pruned"]["teams_summary"] = {"removed_count": removed_from_summary}
    entry["pruned"]["teams_summary_items"] = {
        "tasks_pruned": analysis_summary_stats.get("tasks_pruned", 0),
        "recommended_actions_pruned": analysis_summary_stats.get("recommended_actions_pruned", 0),
    }

    # Add details (kept reasonably sized)
    entry["details"] = {
        "teams": teams_msg_summary.get("details", [])[:200],
        "outlook_events": event_summary.get("details", [])[:200],
        "teams_summary": analysis_summary_stats.get("details", [])[:200],
    }

    if not args.no_log:
        write_prune_log(prune_log, entry)
    print(
        _yellow(
            f"[INFO] Prune complete. Threads={len(pruned_threads)}, TeamsMsgsPruned={teams_msg_summary.get('messages_pruned', 0)}, "
            f"TeamsDeleted={teams_msg_summary.get('conversations_deleted', 0)}, EventsDropped={len(pruned_event_ids)}"
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
