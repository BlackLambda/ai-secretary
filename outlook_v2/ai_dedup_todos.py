import argparse
import json
import os
from datetime import datetime, timezone

from ai_secretary_core.todo_dedup import dedup_todos


def load_json(filepath):
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate todos in Outlook events (deterministic).")
    parser.add_argument("--input", required=True, help="Path to Outlook events JSON (snapshot/master)")
    parser.add_argument("--output", required=True, help="Path to write updated JSON")
    args = parser.parse_args()

    data = load_json(args.input)
    if not isinstance(data, dict):
        print("[ERROR] Input JSON must be an object with an 'events' list")
        return 2

    events = data.get("events", [])
    if not isinstance(events, list):
        print("[ERROR] Input JSON missing 'events' list")
        return 2

    total_removed = 0
    touched = 0

    for e in events:
        if not isinstance(e, dict):
            continue

        # Enforce single bucket: merge recommendations into todos.
        recs = e.get("recommendations", [])
        if isinstance(recs, list) and recs:
            e.setdefault("todos", [])
            if isinstance(e.get("todos"), list):
                e["todos"].extend(recs)
            e["recommendations"] = []

        todos = e.get("todos", [])
        deduped, removed = dedup_todos(todos)
        if removed:
            e["todos"] = deduped
            total_removed += removed
            touched += 1

    data["events"] = events
    data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    save_json(data, args.output)
    print(f"[OK] Deduped todos in {touched} events (removed {total_removed} duplicates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
