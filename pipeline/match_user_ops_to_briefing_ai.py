import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.ai_utils import DEPLOYMENT_NAME, get_azure_openai_client, ai_chat_json
from lib.ai_utils import AZURE_OPENAI_TIMEOUT_SECONDS

from ai_secretary_core import json_io
from ai_secretary_core.paths import RepoPaths


BASE_DIR = Path(__file__).resolve().parent.parent
PATHS = RepoPaths(BASE_DIR)


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = " ".join(s.split())
    return s.strip()


def _clip(s: Any, limit: int = 280) -> str:
    s2 = _norm_text(s)
    if len(s2) <= limit:
        return s2
    return s2[: limit - 1] + "…"


def _fmt_conf(x: Any) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "0.00"


@dataclass
class Candidate:
    candidate_id: str
    ui_id: str
    card_index: int
    card_type: str  # Outlook | Teams
    bucket: str
    item_index: int
    title: str  # event_name or chat_name
    topic: str
    container_start_time: str
    container_end_time: str
    container_last_updated: str
    event_type: str
    labels: List[str]
    task_text: str
    deadline: str
    quote: str
    priority: str
    user_role: str
    owner: str
    assignees: str
    rationale: str
    assignment_reason: str

    def summary(self) -> Dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "source": self.card_type,
            "bucket": self.bucket,
            "title": _clip(self.title, 120),
            "topic": _clip(self.topic, 120),
            "container_time": {
                "start": _clip(self.container_start_time, 40),
                "end": _clip(self.container_end_time, 40),
                "last_updated": _clip(self.container_last_updated, 40),
            },
            "event_type": _clip(self.event_type, 40),
            "labels": [
                _clip(x, 32) for x in (self.labels or []) if isinstance(x, str) and x.strip()
            ][:8],
            "task_text": _clip(self.task_text, 200),
            "deadline": _clip(self.deadline, 60),
            "priority": _clip(self.priority, 20),
            "user_role": _clip(self.user_role, 20),
            "owner": _clip(self.owner, 60),
            "assignees": _clip(self.assignees, 120),
            "quote": _clip(self.quote, 260),
            "rationale": _clip(self.rationale, 260),
            "assignment_reason": _clip(self.assignment_reason, 260),
        }


def build_candidates(briefing: Dict[str, Any]) -> List[Candidate]:
    cards = briefing.get("cards")
    if not isinstance(cards, list):
        return []

    out: List[Candidate] = []
    seq = 0

    def next_id() -> str:
        nonlocal seq
        cid = f"C{seq:06d}"
        seq += 1
        return cid

    for card_idx, card in enumerate(cards):
        if not isinstance(card, dict):
            continue
        ctype = card.get("type")
        data = card.get("data")
        if not isinstance(data, dict):
            continue

        if ctype == "Outlook":
            event_name = _norm_text(data.get("event_name") or "")
            event_type = _norm_text(data.get("event_type") or "")
            labels_raw = data.get("labels")
            labels: List[str] = [
                _norm_text(x) for x in (labels_raw if isinstance(labels_raw, list) else []) if _norm_text(x)
            ]
            start_time = _norm_text(data.get("start_time") or "")
            end_time = _norm_text(data.get("end_time") or "")
            container_last_updated = _norm_text(data.get("last_updated") or "")
            for bucket, prefix in (("todos", "outlook-{card}-todo"), ("recommendations", "outlook-{card}-rec")):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for item_idx, item in enumerate(items):
                    if not isinstance(item, dict):
                        continue
                    # Prefer stable IDs when present (briefing_data.json); fall back to
                    # the synthetic IDs used by the frontend when IDs are missing (briefing_data_no_id.json).
                    ui_id_raw = item.get("_ui_id")
                    if isinstance(ui_id_raw, str) and ui_id_raw.strip():
                        ui_id = ui_id_raw.strip()
                    else:
                        # Mirrors frontend generateUiId(`outlook-${cardIdx}-todo`, idx)
                        ui_id = f"{prefix.format(card=card_idx)}-{item_idx}"
                    od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
                    task_text = _norm_text(
                        item.get("task")
                        or item.get("description")
                        or od.get("task")
                        or od.get("description")
                        or ""
                    )
                    deadline = _norm_text(item.get("deadline") or od.get("deadline") or "")
                    quote = _norm_text(item.get("original_quote") or od.get("original_quote") or "")

                    priority = _norm_text(item.get("priority") or od.get("priority") or "")
                    user_role = _norm_text(item.get("user_role") or od.get("user_role") or "")
                    owner = _norm_text(item.get("owner") or od.get("owner") or "")
                    assignees_val = item.get("assignees") if isinstance(item.get("assignees"), list) else od.get("assignees")
                    assignees = ", ".join([_norm_text(x) for x in (assignees_val if isinstance(assignees_val, list) else []) if _norm_text(x)])
                    rationale = _norm_text(item.get("rationale") or od.get("rationale") or "")
                    assignment_reason = _norm_text(item.get("assignment_reason") or od.get("assignment_reason") or "")

                    out.append(
                        Candidate(
                            candidate_id=next_id(),
                            ui_id=ui_id,
                            card_index=card_idx,
                            card_type="Outlook",
                            bucket=bucket,
                            item_index=item_idx,
                            title=event_name,
                            topic="",
                            container_start_time=start_time,
                            container_end_time=end_time,
                            container_last_updated=container_last_updated,
                            event_type=event_type,
                            labels=labels,
                            task_text=task_text,
                            deadline=deadline,
                            quote=quote,
                            priority=priority,
                            user_role=user_role,
                            owner=owner,
                            assignees=assignees,
                            rationale=rationale,
                            assignment_reason=assignment_reason,
                        )
                    )

        elif ctype == "Teams":
            conv = data.get("conversation") if isinstance(data.get("conversation"), dict) else {}
            chat_name = _norm_text(conv.get("chat_name") or "")
            summary = conv.get("summary") if isinstance(conv.get("summary"), dict) else {}
            topic = _norm_text(summary.get("topic") or "")
            container_last_updated = _norm_text(conv.get("last_updated") or data.get("last_updated") or "")

            for bucket, prefix_base in (("linked_items", "teams-{card}-linked"), ("unlinked_items", "teams-{card}-unlinked")):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for item_idx, item in enumerate(items):
                    if not isinstance(item, dict):
                        continue
                    item_type = _norm_text(item.get("type") or "")
                    suffix = "-rec" if item_type.lower() == "action" else "-task"
                    ui_id_raw = item.get("_ui_id")
                    if isinstance(ui_id_raw, str) and ui_id_raw.strip():
                        ui_id = ui_id_raw.strip()
                    else:
                        # Mirrors frontend generateUiId(`teams-${cardIdx}-linked${suffix}`, idx)
                        ui_id = f"{prefix_base.format(card=card_idx)}{suffix}-{item_idx}"

                    od = item.get("original_data") if isinstance(item.get("original_data"), dict) else {}
                    task_text = _norm_text(
                        item.get("description")
                        or item.get("task")
                        or od.get("description")
                        or od.get("task")
                        or ""
                    )
                    deadline = _norm_text(item.get("deadline") or od.get("deadline") or "")
                    quote = _norm_text(item.get("original_quote") or od.get("original_quote") or "")

                    priority = _norm_text(item.get("priority") or od.get("priority") or "")
                    user_role = _norm_text(item.get("user_role") or od.get("user_role") or "")
                    owner = _norm_text(item.get("owner") or od.get("owner") or "")
                    assignees_val = item.get("assignees") if isinstance(item.get("assignees"), list) else od.get("assignees")
                    assignees = ", ".join([_norm_text(x) for x in (assignees_val if isinstance(assignees_val, list) else []) if _norm_text(x)])
                    rationale = _norm_text(item.get("rationale") or od.get("rationale") or "")
                    assignment_reason = _norm_text(item.get("assignment_reason") or od.get("assignment_reason") or "")

                    out.append(
                        Candidate(
                            candidate_id=next_id(),
                            ui_id=ui_id,
                            card_index=card_idx,
                            card_type="Teams",
                            bucket=bucket,
                            item_index=item_idx,
                            title=chat_name,
                            topic=topic,
                            container_start_time="",
                            container_end_time="",
                            container_last_updated=container_last_updated,
                            event_type="",
                            labels=[],
                            task_text=task_text,
                            deadline=deadline,
                            quote=quote,
                            priority=priority,
                            user_role=user_role,
                            owner=owner,
                            assignees=assignees,
                            rationale=rationale,
                            assignment_reason=assignment_reason,
                        )
                    )

    return out


def _chat_json(client, messages: List[Dict[str, str]], temperature: float = 0.1) -> Dict[str, Any]:
    try:
        return ai_chat_json(client, messages, temperature=temperature)
    except Exception:
        return {}


SYSTEM_SHORTLIST = (
    "You are a careful entity-resolution assistant. "
    "You match ONE stored user operation (from a past run) to action items in the current briefing data. "
    "You MUST only choose from the provided candidates; never invent IDs. "
    "If none match confidently, return an empty shortlist."
)

SYSTEM_FINAL = (
    "You are a careful entity-resolution assistant. "
    "Pick the single best matching candidate for the stored user operation. "
    "You MUST only choose from the provided candidates; never invent IDs. "
    "If none match confidently, return null."
)

SYSTEM_BATCH = (
    "You are a careful entity-resolution assistant. "
    "For EACH stored user operation, pick the single best matching candidate from the provided list (or null). "
    "You MUST only choose from the provided candidates; never invent IDs. "
    "If none match confidently, return null for that op."
)


def _chunks(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        size = 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def llm_shortlist(
    client,
    op_context: Dict[str, Any],
    candidates: List[Candidate],
    batch_size: int,
    shortlist_per_batch: int,
    sleep_s: float,
) -> List[Dict[str, Any]]:
    all_hits: List[Dict[str, Any]] = []

    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        payload = {
            "op_context": {
                "card_type": op_context.get("card_type"),
                "bucket": op_context.get("bucket"),
                "text": op_context.get("text"),
                "deadline": op_context.get("deadline"),
                "event_name": op_context.get("event_name"),
                "chat_name": op_context.get("chat_name"),
                "original_quote": _clip(op_context.get("original_quote") or "", 300),
                "start_time": op_context.get("start_time"),
                "end_time": op_context.get("end_time"),
            },
            "candidates": [c.summary() for c in batch],
            "instructions": {
                "pick_up_to": shortlist_per_batch,
                "output_schema": {
                    "shortlist": [
                        {"candidate_id": "C000123", "confidence": 0.0, "why": "short reason"}
                    ]
                },
            },
        }

        messages = [
            {"role": "system", "content": SYSTEM_SHORTLIST},
            {
                "role": "user",
                "content": (
                    "Return JSON only.\n\n"
                    "Task: from this batch, pick the candidates that refer to the SAME real-world action as op_context.\n"
                    "Be strict: require strong alignment on title (event/chat), task text, deadline, and/or quote.\n"
                    "If nothing matches, return {\"shortlist\": []}.\n\n"
                    f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
                ),
            },
        ]

        out = _chat_json(client, messages)
        shortlist = out.get("shortlist") if isinstance(out, dict) else None
        if isinstance(shortlist, list):
            for hit in shortlist:
                if not isinstance(hit, dict):
                    continue
                cid = hit.get("candidate_id")
                if isinstance(cid, str) and cid.startswith("C"):
                    all_hits.append(hit)

        if sleep_s > 0:
            time.sleep(sleep_s)

    return all_hits


def llm_pick_best(client, op_context: Dict[str, Any], shortlist_candidates: List[Candidate]) -> Dict[str, Any]:
    payload = {
        "op_context": {
            "card_type": op_context.get("card_type"),
            "bucket": op_context.get("bucket"),
            "text": op_context.get("text"),
            "deadline": op_context.get("deadline"),
            "event_name": op_context.get("event_name"),
            "chat_name": op_context.get("chat_name"),
            "original_quote": _clip(op_context.get("original_quote") or "", 600),
            "start_time": op_context.get("start_time"),
            "end_time": op_context.get("end_time"),
        },
        "candidates": [c.summary() for c in shortlist_candidates],
        "output_schema": {"match": {"candidate_id": "C000123"}, "confidence": 0.0, "explanation": "short"},
        "rules": [
            "If no candidate clearly matches, set match to null and confidence <= 0.4.",
            "Never invent candidate_id.",
        ],
    }

    messages = [
        {"role": "system", "content": SYSTEM_FINAL},
        {
            "role": "user",
            "content": (
                "Return JSON only.\n\n"
                "Pick the single best match from the candidate list (or null).\n"
                f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
            ),
        },
    ]

    return _chat_json(client, messages)


def llm_match_ops_batch(
    client,
    op_contexts: List[Dict[str, Any]],
    candidates: List[Candidate],
) -> List[Dict[str, Any]]:
    payload = {
        "ops": [
            {
                "card_type": ctx.get("card_type"),
                "bucket": ctx.get("bucket"),
                "text": ctx.get("text"),
                "deadline": ctx.get("deadline"),
                "event_name": ctx.get("event_name"),
                "chat_name": ctx.get("chat_name"),
                "original_quote": _clip(ctx.get("original_quote") or "", 400),
                "start_time": ctx.get("start_time"),
                "end_time": ctx.get("end_time"),
            }
            for ctx in op_contexts
        ],
        "candidates": [c.summary() for c in candidates],
        "output_schema": {
            "matches": [
                {
                    "op_index": 0,
                    "candidate_id": "C000123",
                    "confidence": 0.0,
                    "explanation": "short",
                }
            ]
        },
        "rules": [
            "Return exactly one match object per op (same op_index).",
            "If no candidate clearly matches, set candidate_id to null and confidence <= 0.4.",
            "Never invent candidate_id.",
        ],
    }

    messages = [
        {"role": "system", "content": SYSTEM_BATCH},
        {
            "role": "user",
            "content": (
                "Return JSON only.\n\n"
                "For each op in ops[], pick the best matching candidate from candidates[] (or null).\n"
                "Be strict: require strong alignment on title (event/chat), task text, deadline, and/or quote.\n"
                "Use op_index to identify which op you are answering.\n\n"
                f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
            ),
        },
    ]

    out = _chat_json(client, messages)
    matches = out.get("matches") if isinstance(out, dict) else None
    return matches if isinstance(matches, list) else []


def _load_or_init_user_ops(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        try:
            data = json_io.read_json(path)
            if isinstance(data, dict):
                for k in ("completed", "dismissed", "promoted", "completed_ai", "dismissed_ai"):
                    if not isinstance(data.get(k), list):
                        data[k] = []
                return data
        except Exception:
            pass
    return {"completed": [], "dismissed": [], "promoted": [], "completed_ai": [], "dismissed_ai": []}


def apply_matches_to_user_ops(
    user_ops_path: str,
    matches: List[Dict[str, Any]],
) -> Dict[str, Any]:
    # Restore mode: always rebuild from scratch and write primary lists.
    ops = {"completed": [], "dismissed": [], "promoted": [], "completed_ai": [], "dismissed_ai": []}

    completed = set(str(x) for x in ops.get("completed", []) if x)
    dismissed = set(str(x) for x in ops.get("dismissed", []) if x)
    completed_ai = set(str(x) for x in ops.get("completed_ai", []) if x)
    dismissed_ai = set(str(x) for x in ops.get("dismissed_ai", []) if x)

    for m in matches:
        if not isinstance(m, dict):
            continue

        matched_ui_id = m.get("ui_id")
        last_seen_ui_id = m.get("last_seen_ui_id")

        chosen_id: Optional[str] = None
        if isinstance(matched_ui_id, str) and matched_ui_id.strip():
            chosen_id = matched_ui_id.strip()
        elif isinstance(last_seen_ui_id, str) and last_seen_ui_id.strip():
            chosen_id = last_seen_ui_id.strip()

        if not chosen_id:
            continue

        op_type = m.get("op")
        is_active = bool(m.get("active"))

        # Restore mode: only apply currently-active operations.
        if not is_active:
            continue

        if op_type == "dismiss":
            dismissed_ai.add(chosen_id)
            completed_ai.discard(chosen_id)
            dismissed.add(chosen_id)
            completed.discard(chosen_id)

        elif op_type == "complete":
            completed_ai.add(chosen_id)
            dismissed_ai.discard(chosen_id)
            completed.add(chosen_id)
            dismissed.discard(chosen_id)

    ops["completed"] = sorted(completed)
    ops["dismissed"] = sorted(dismissed)
    ops["completed_ai"] = sorted(completed_ai)
    ops["dismissed_ai"] = sorted(dismissed_ai)

    json_io.write_json(user_ops_path, ops)
    return ops


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "LLM-match persisted user ops (user_ops_store.json) onto briefing action items "
            "(briefing_data.json or briefing_data_no_id.json)."
        )
    )
    parser.add_argument(
        "--ops-store",
        default=str(PATHS.user_ops_store_file()),
        help="Path to user_ops_store.json (persistent store)",
    )
    parser.add_argument(
        "--briefing",
        default=str(PATHS.briefing_data_file()),
        help="Path to briefing_data.json (preferred) or briefing_data_no_id.json",
    )
    parser.add_argument(
        "--user-ops-out",
        default=str(PATHS.user_operation_file()),
        help="Where to write the derived user_operation.json",
    )
    parser.add_argument(
        "--write-user-ops",
        action="store_true",
        help="If set, writes incremental_data/user_operation.json based on matches.",
    )
    parser.add_argument(
        "--prune-unmatched-store",
        action="store_true",
        help=(
            "If set, removes ACTIVE stored ops from user_ops_store.json when they cannot be matched "
            "to any current briefing candidate with confidence >= threshold."
        ),
    )
    parser.add_argument(
        "--ops-batch-size",
        type=int,
        default=5,
        help="How many stored ops to send to the LLM per request (default: 5).",
    )
    # Note: this script is intentionally minimal for the app restore flow.
    # It always rebuilds user_operation.json from scratch and does not cache.

    args = parser.parse_args()

    store = json_io.read_json(args.ops_store)
    briefing = json_io.read_json(args.briefing)

    ops_by_fp = store.get("ops_by_fingerprint")
    if not isinstance(ops_by_fp, dict):
        print("[ERROR] ops_by_fingerprint missing or invalid")
        return 2

    candidates = build_candidates(briefing)
    if not candidates:
        print("[ERROR] No candidates found in briefing")
        return 2

    # Write an intermediate, sanitized file showing what we feed to the LLM.
    # This avoids leaking id-like fields and helps debugging restore behavior.
    restore_input_path = os.path.join("user_state", "op_restore_ai_input.json")
    # Write a separate debug report that can include real IDs (ui_id).
    # This file is NOT used as LLM input; it's purely for local debugging.
    restore_report_path = os.path.join("user_state", "op_restore_ai_report.json")

    client = get_azure_openai_client()

    fps = list(ops_by_fp.keys())

    matches: List[Dict[str, Any]] = []
    restore_inputs: List[Dict[str, Any]] = []
    per_op_debug: List[Dict[str, Any]] = []
    pruned_unmatched_fps: List[str] = []
    pruned_unmatched_active_fps: List[str] = []

    candidate_summaries = [c.summary() for c in candidates]
    candidate_id_map: List[Dict[str, Any]] = [
        {
            "candidate_id": c.candidate_id,
            "ui_id": c.ui_id,
            "card_type": c.card_type,
            "bucket": c.bucket,
            "card_index": c.card_index,
            "item_index": c.item_index,
            "title": _clip(c.title, 160),
            "task_text": _clip(c.task_text, 220),
            "deadline": _clip(c.deadline, 80),
        }
        for c in candidates
    ]

    # Preprocess ops and group by card_type so we can batch-match 5 ops per LLM call.
    ops_pre: List[Dict[str, Any]] = []
    for fp in fps:
        entry = ops_by_fp.get(fp)
        if not isinstance(entry, dict):
            continue

        op_type = entry.get("op")
        active = bool(entry.get("active"))
        ctx = entry.get("context") if isinstance(entry.get("context"), dict) else {}
        persisted_last_seen = entry.get("last_seen_ui_id")

        ctx_enriched = dict(ctx)
        if isinstance(persisted_last_seen, str) and persisted_last_seen.strip():
            ctx_enriched["last_seen_ui_id"] = persisted_last_seen.strip()
        elif isinstance(ctx_enriched.get("last_seen_ui_id"), str) and str(ctx_enriched.get("last_seen_ui_id")).strip():
            ctx_enriched["last_seen_ui_id"] = str(ctx_enriched.get("last_seen_ui_id")).strip()

        # Avoid ID-based matching: do not expose id-like fields to the LLM.
        ctx_for_llm = dict(ctx_enriched)
        for k in ("last_seen_ui_id", "event_id", "chat_id", "conversation_id"):
            ctx_for_llm.pop(k, None)

        restore_inputs.append(
            {
                "fingerprint": fp,
                "op": op_type,
                "active": active,
                "context_for_llm": ctx_for_llm,
            }
        )

        ctx_card_type = ctx_for_llm.get("card_type")
        group_key = ctx_card_type.strip() if isinstance(ctx_card_type, str) and ctx_card_type.strip() in ("Outlook", "Teams") else "ALL"

        ops_pre.append(
            {
                "fingerprint": fp,
                "op": op_type,
                "active": active,
                "persisted_last_seen_ui_id": persisted_last_seen if isinstance(persisted_last_seen, str) else ctx.get("last_seen_ui_id"),
                "context_for_llm": ctx_for_llm,
                "group": group_key,
            }
        )

    ops_by_group: Dict[str, List[Dict[str, Any]]] = {"Outlook": [], "Teams": [], "ALL": []}
    for it in ops_pre:
        ops_by_group.setdefault(it["group"], []).append(it)

    def _fallback_single_op(ctx_for_llm: Dict[str, Any], pool: List[Candidate]) -> Dict[str, Any]:
        hits = llm_shortlist(
            client,
            ctx_for_llm,
            pool,
            batch_size=40,
            shortlist_per_batch=2,
            sleep_s=0.0,
        )
        best_by_cid: Dict[str, float] = {}
        for h in hits:
            cid = h.get("candidate_id")
            conf = h.get("confidence")
            if not isinstance(cid, str):
                continue
            try:
                c = float(conf)
            except Exception:
                c = 0.0
            if cid not in best_by_cid or c > best_by_cid[cid]:
                best_by_cid[cid] = c
        ranked = sorted(best_by_cid.items(), key=lambda kv: kv[1], reverse=True)[: 12]
        finalists = {cid for cid, _ in ranked}
        finalist_objs = [c for c in pool if c.candidate_id in finalists]
        pick = llm_pick_best(client, ctx_for_llm, finalist_objs) if finalist_objs else {"match": None, "confidence": 0.0}
        match_obj = pick.get("match") if isinstance(pick, dict) else None
        match_cid = match_obj.get("candidate_id") if isinstance(match_obj, dict) else None
        try:
            confidence = float(pick.get("confidence", 0.0))
        except Exception:
            confidence = 0.0
        chosen: Optional[Candidate] = None
        if isinstance(match_cid, str):
            for c in finalist_objs:
                if c.candidate_id == match_cid:
                    chosen = c
                    break
        return {
            "mode": "fallback_single",
            "ranked": ranked,
            "finalist_objs": finalist_objs,
            "pick": pick,
            "match_cid": match_cid,
            "confidence": confidence,
            "chosen": chosen,
        }

    for group_key, group_ops in ops_by_group.items():
        if not group_ops:
            continue

        pool = candidates if group_key == "ALL" else [c for c in candidates if c.card_type == group_key]
        for batch_idx, batch in enumerate(_chunks(group_ops, args.ops_batch_size)):
            ctxs = [it["context_for_llm"] for it in batch]
            batch_out = llm_match_ops_batch(client, ctxs, pool)

            # Index results by op_index.
            idx_map: Dict[int, Dict[str, Any]] = {}
            for r in batch_out:
                if not isinstance(r, dict):
                    continue
                oi = r.get("op_index")
                if isinstance(oi, int):
                    idx_map[oi] = r

            # If the response is malformed, fall back to the old per-op flow for this batch.
            use_fallback = len(idx_map) != len(batch)

            print(
                f"[BATCH] group={group_key} idx={batch_idx} size={len(batch)} pool={len(pool)} "
                f"mode={'fallback_single' if use_fallback else 'ops_batch'}"
            )

            batch_log_rows: List[Dict[str, Any]] = []

            for local_i, it in enumerate(batch):
                fp = it["fingerprint"]
                op_type = it["op"]
                active = bool(it["active"])
                ctx_for_llm = it["context_for_llm"]
                persisted_last_seen_ui_id = it["persisted_last_seen_ui_id"]

                chosen: Optional[Candidate] = None
                match_cid: Optional[str] = None
                confidence = 0.0
                explanation = None

                debug_obj: Dict[str, Any] = {
                    "fingerprint": fp,
                    "op": op_type,
                    "active": active,
                    "persisted_last_seen_ui_id": persisted_last_seen_ui_id,
                    "context_for_llm": ctx_for_llm,
                    "pool_size": len(pool),
                    "batch_mode": "ops_batch",
                    "batch_size": args.ops_batch_size,
                    "group": group_key,
                }

                if not use_fallback:
                    r = idx_map.get(local_i, {})
                    cid = r.get("candidate_id")
                    try:
                        confidence = float(r.get("confidence", 0.0))
                    except Exception:
                        confidence = 0.0
                    explanation = r.get("explanation")
                    match_cid = cid if isinstance(cid, str) else None
                    if match_cid:
                        for c in pool:
                            if c.candidate_id == match_cid:
                                chosen = c
                                break
                    debug_obj["pick_raw"] = r
                    batch_log_rows.append(
                        {
                            "fingerprint": fp,
                            "op": op_type,
                            "candidate_id": match_cid,
                            "confidence": confidence,
                            "status": "matched" if (chosen and confidence >= 0.55) else "unmatched",
                            "fallback": False,
                        }
                    )
                else:
                    fb = _fallback_single_op(ctx_for_llm, pool)
                    chosen = fb.get("chosen")
                    match_cid = fb.get("match_cid")
                    confidence = float(fb.get("confidence") or 0.0)
                    pick = fb.get("pick")
                    explanation = pick.get("explanation") if isinstance(pick, dict) else None
                    debug_obj.update(
                        {
                            "fallback_used": True,
                            "shortlist_ranked": [
                                {"candidate_id": cid, "confidence": float(conf)}
                                for cid, conf in (fb.get("ranked") or [])
                            ],
                            "finalists": [
                                {
                                    "candidate_id": c.candidate_id,
                                    "ui_id": c.ui_id,
                                    "card_type": c.card_type,
                                    "bucket": c.bucket,
                                    "card_index": c.card_index,
                                    "item_index": c.item_index,
                                    "title": _clip(c.title, 160),
                                    "task_text": _clip(c.task_text, 220),
                                    "deadline": _clip(c.deadline, 80),
                                    "quote": _clip(c.quote, 260),
                                }
                                for c in (fb.get("finalist_objs") or [])
                            ],
                            "pick_raw": pick,
                        }
                    )
                    batch_log_rows.append(
                        {
                            "fingerprint": fp,
                            "op": op_type,
                            "candidate_id": match_cid,
                            "confidence": confidence,
                            "status": "matched" if (chosen and confidence >= 0.55) else "unmatched",
                            "fallback": True,
                        }
                    )

                debug_obj.update(
                    {
                        "picked_candidate_id": match_cid,
                        "picked_candidate_ui_id": chosen.ui_id if chosen else None,
                        "confidence": confidence,
                        "will_apply": bool(chosen and confidence >= 0.55 and active),
                    }
                )
                per_op_debug.append(debug_obj)

                result: Dict[str, Any] = {
                    "status": "unmatched",
                    "fingerprint": fp,
                    "op": op_type,
                    "active": active,
                    "last_seen_ui_id": persisted_last_seen_ui_id,
                    "picked_candidate_id": match_cid,
                    "confidence": confidence,
                    "explanation": explanation,
                }

                if chosen and confidence >= 0.55:
                    result.update(
                        {
                            "status": "matched",
                            "ui_id": chosen.ui_id,
                            "card_type": chosen.card_type,
                            "bucket": chosen.bucket,
                            "card_index": chosen.card_index,
                            "item_index": chosen.item_index,
                            "title": chosen.title,
                            "task_text": chosen.task_text,
                            "deadline": chosen.deadline,
                        }
                    )

                matches.append(result)

            try:
                matched_in_batch = sum(1 for r in batch_log_rows if r.get("status") == "matched")
                print(f"[BATCH_RESULT] matched={matched_in_batch}/{len(batch_log_rows)}")
                for r in batch_log_rows:
                    fp_short = str(r.get("fingerprint") or "")[:10]
                    print(
                        f"  - fp={fp_short} op={r.get('op')} status={r.get('status')} "
                        f"cid={r.get('candidate_id')} conf={_fmt_conf(r.get('confidence'))} "
                        f"fallback={bool(r.get('fallback'))}"
                    )
            except Exception:
                pass

        # No per-op sleep in restore mode.

    if args.write_user_ops:
        apply_matches_to_user_ops(args.user_ops_out, matches)

    if args.prune_unmatched_store:
        # Prune operations that we could not confidently match.
        # This prevents user_ops_store.json from accumulating stale actions that no longer
        # exist in the current briefing.
        matched_fps = {
            m.get("fingerprint")
            for m in matches
            if isinstance(m, dict) and m.get("status") == "matched"
        }

        ops_by_fp_store = store.get("ops_by_fingerprint")
        if isinstance(ops_by_fp_store, dict):
            for fp in list(ops_by_fp_store.keys()):
                entry = ops_by_fp_store.get(fp)
                if not isinstance(entry, dict):
                    continue
                if fp not in matched_fps:
                    pruned_unmatched_fps.append(fp)
                    if bool(entry.get("active")):
                        pruned_unmatched_active_fps.append(fp)
                    ops_by_fp_store.pop(fp, None)

            store["ops_by_fingerprint"] = ops_by_fp_store
            json_io.write_json(args.ops_store, store)

    try:
        json_io.write_json(
            restore_input_path,
            {
                "generated_at": time.time(),
                "ops": restore_inputs,
                "candidates": candidate_summaries,
            },
        )
        print(f"AI restore input written: {restore_input_path}")
    except Exception:
        pass

    try:
        matched = sum(1 for m in matches if isinstance(m, dict) and m.get("status") == "matched")
        json_io.write_json(
            restore_report_path,
            {
                "generated_at": time.time(),
                "ops_store_path": args.ops_store,
                "briefing_path": args.briefing,
                "threshold": 0.55,
                "stats": {
                    "ops_total": len(matches),
                    "ops_matched": matched,
                    "ops_pruned_unmatched": len(pruned_unmatched_fps),
                    "ops_pruned_unmatched_active": len(pruned_unmatched_active_fps),
                    "candidates_total": len(candidates),
                },
                "candidate_id_map": candidate_id_map,
                "pruned_unmatched_fingerprints": pruned_unmatched_fps,
                "pruned_unmatched_active_fingerprints": pruned_unmatched_active_fps,
                "per_op": per_op_debug,
                "matches": matches,
            },
        )
        print(f"AI restore debug report written: {restore_report_path}")
    except Exception:
        pass

    print(f"Done. Matched {matched}/{len(matches)} ops.")
    if args.write_user_ops:
        print(f"User ops written: {args.user_ops_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
