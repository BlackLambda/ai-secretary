from flask import Flask, request, jsonify, send_from_directory, send_file, make_response
import os
import json
import argparse
import base64
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Any
from uuid import uuid4

import re

import signal
import shutil
import atexit
from pathlib import Path
from lib.pipeline_config_manager import ensure_effective_config, get_config_paths, save_effective_from_updates

from ai_secretary_core import json_io
from ai_secretary_core import pipeline_state
from ai_secretary_core.paths import RepoPaths

from lib.task_vectorization import (
    compute_focus_vector,
    dot,
    extract_card_text,
    hash_embedding,
    text_sha256,
)

app = Flask(__name__, static_folder='static', static_url_path='')

# --- CORS for browser extension (chrome-extension:// origins) ---
@app.after_request
def _add_cors_headers(response):
    origin = request.headers.get('Origin', '')
    if origin.startswith('chrome-extension://') or origin.startswith('moz-extension://'):
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

_APP_SUBDIR = 'app'


# --- Pipeline status (server-side step injection) ---

_PIPELINE_STATUS_UPDATE_LOCK = threading.Lock()


def _atomic_write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{uuid4().hex}")
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
        f.write('\n')
    os.replace(str(tmp), str(path))


def _load_json_best_effort(path: Path) -> dict:
    try:
        if not path.exists() or not path.is_file():
            return {}
        with path.open('r', encoding='utf-8-sig') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _utc_now_iso_seconds() -> str:
    try:
        # Mirror run_incremental_pipeline.py format (Zulu time, no microseconds).
        from datetime import timezone

        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    except Exception:
        return datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


def _update_pipeline_status_with_step_event(
    status_path: Path,
    *,
    step_event: dict,
) -> None:
    """Best-effort append/update a step record inside pipeline_status.json.

    This is used for server-managed steps (e.g., auto-restore after reset) that
    are not executed inside run_incremental_pipeline.py.
    """
    try:
        now_iso = _utc_now_iso_seconds()
        with _PIPELINE_STATUS_UPDATE_LOCK:
            status = _load_json_best_effort(status_path)
            if not isinstance(status, dict):
                status = {}

            steps = status.get('steps')
            if not isinstance(steps, list):
                steps = []
            steps = [x for x in steps if isinstance(x, dict)]

            sid = step_event.get('id')
            sid = str(sid) if sid is not None else ''
            phase = str(step_event.get('phase') or '').strip().lower()
            name = str(step_event.get('name') or '')

            def find_step_index(step_id: str) -> int | None:
                for i, rec in enumerate(steps):
                    if str(rec.get('id') or '') == step_id:
                        return i
                return None

            def normalize_files(v: object) -> list[str] | None:
                if v is None:
                    return None
                if isinstance(v, (str, Path)):
                    s = str(v).strip()
                    return [s] if s else None
                if isinstance(v, (list, tuple, set)):
                    out: list[str] = []
                    for x in v:
                        s = str(x).strip()
                        if s:
                            out.append(s)
                    return out if out else None
                s = str(v).strip()
                return [s] if s else None

            def apply_files(dst: dict, src: dict) -> None:
                in_files = normalize_files(src.get('input_files'))
                out_files = normalize_files(src.get('output_files'))
                if in_files is not None:
                    dst['input_files'] = in_files
                if out_files is not None:
                    dst['output_files'] = out_files

            if sid and phase in ('plan', 'queued'):
                idx = find_step_index(sid)
                rec = steps[idx] if idx is not None else {}
                if not isinstance(rec, dict):
                    rec = {}
                rec['id'] = sid
                if name:
                    rec['name'] = name
                rec['status'] = str(step_event.get('status') or rec.get('status') or 'queued')
                if step_event.get('index') is not None:
                    rec['index'] = step_event.get('index')
                apply_files(rec, step_event)
                if idx is None:
                    steps.append(rec)
                else:
                    steps[idx] = rec

            if sid and phase == 'start':
                idx = find_step_index(sid)
                prev = steps[idx] if idx is not None else {}
                if not isinstance(prev, dict):
                    prev = {}
                rec = {
                    **prev,
                    'id': sid,
                    'name': name or str(prev.get('name') or ''),
                    'status': 'running',
                    'started_at': str(step_event.get('started_at') or now_iso),
                }
                if step_event.get('index') is not None:
                    rec['index'] = step_event.get('index')
                apply_files(rec, step_event)
                if idx is None:
                    steps.append(rec)
                else:
                    steps[idx] = rec
                status['current_step_id'] = sid

            if sid and phase == 'end':
                idx = find_step_index(sid)
                rec = steps[idx] if idx is not None else {
                    'id': sid,
                    'name': name,
                    'started_at': str(step_event.get('started_at') or now_iso),
                }
                if not isinstance(rec, dict):
                    rec = {'id': sid, 'name': name}
                rec['ended_at'] = str(step_event.get('ended_at') or now_iso)
                rec['status'] = str(step_event.get('status') or rec.get('status') or 'ok')
                if step_event.get('index') is not None:
                    rec['index'] = step_event.get('index')
                if step_event.get('exit_code') is not None:
                    rec['exit_code'] = step_event.get('exit_code')
                if step_event.get('error'):
                    rec['error'] = str(step_event.get('error'))
                apply_files(rec, step_event)
                if idx is None:
                    steps.append(rec)
                else:
                    steps[idx] = rec
                if str(status.get('current_step_id') or '') == sid:
                    status['current_step_id'] = ''

            # Keep file bounded.
            MAX_STEPS = 200
            if len(steps) > MAX_STEPS:
                steps = steps[-MAX_STEPS:]

            status.setdefault('state', 'working')
            status.setdefault('message', '')
            status.setdefault('next_run', '')
            status['last_updated'] = now_iso
            status['steps'] = steps

            _atomic_write_json(status_path, status)
    except Exception:
        # Best-effort; do not break pipeline control APIs.
        return


def _compute_user_ops_restore_status_raw() -> dict:
    """Pure helper for background threads (no request context needed)."""
    store_exists = os.path.exists(PERSISTED_USER_OP_FILE)
    store_count = 0
    if store_exists:
        try:
            store = _load_persisted_ops_store()
            ops_by_fp = store.get('ops_by_fingerprint')
            store_count = len(ops_by_fp) if isinstance(ops_by_fp, dict) else 0
        except Exception:
            store_count = 0

    try:
        ops = load_user_ops()
    except Exception:
        ops = {}

    user_op_exists = os.path.exists(USER_OP_FILE)
    try:
        user_op_has_any = _has_any_user_ops(ops)
    except Exception:
        user_op_has_any = False

    can_restore = store_count > 0 and (not user_op_exists or not user_op_has_any)
    return {
        'can_restore': bool(can_restore),
        'store_exists': bool(store_exists),
        'store_count': int(store_count),
        'user_op_exists': bool(user_op_exists),
        'user_op_has_any': bool(user_op_has_any),
    }


def _start_auto_restore_after_reset(active_dir: Path) -> None:
    """Kick off a background restore once briefing_data.json exists after a reset."""
    def worker():
        step_id = f"server:restore_user_ops:{int(time.time() * 1000)}"
        status_path = active_dir / PIPELINE_STATUS_FILE

        # Always show a step entry in Observation so the user can see the outcome.
        _update_pipeline_status_with_step_event(
            status_path,
            step_event={
                'id': step_id,
                'name': 'Restore operations',
                'phase': 'queued',
                'status': 'queued',
            },
        )

        # If there's nothing to restore, mark as OK (skipped).
        status = _compute_user_ops_restore_status_raw()
        if not status.get('can_restore'):
            note = 'Restore not needed'
            if not status.get('store_exists') or int(status.get('store_count') or 0) <= 0:
                note = 'No ops store found'
            elif bool(status.get('user_op_exists')) and bool(status.get('user_op_has_any')):
                note = 'user_operation.json already populated'

            _update_pipeline_status_with_step_event(
                status_path,
                step_event={
                    'id': step_id,
                    'name': f"Restore operations ({note})",
                    'phase': 'end',
                    'status': 'ok',
                    'ended_at': _utc_now_iso_seconds(),
                },
            )
            return

        started_at = _utc_now_iso_seconds()
        _update_pipeline_status_with_step_event(
            status_path,
            step_event={
                'id': step_id,
                'name': 'Restore operations',
                'phase': 'start',
                'started_at': started_at,
                'input_files': [PERSISTED_USER_OP_FILE],
            },
        )

        # Wait for briefing_data.json to exist (pipeline needs to generate it after reset).
        candidates = [
            (active_dir / 'output' / 'briefing_data.json'),
            (active_dir / 'briefing_data.json'),
        ]
        data_path: Path | None = None
        timeout_sec = 300
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            for p in candidates:
                try:
                    if p.exists() and p.is_file():
                        data_path = p
                        break
                except Exception:
                    continue
            if data_path is not None:
                break
            time.sleep(2)

        if data_path is None:
            _update_pipeline_status_with_step_event(
                status_path,
                step_event={
                    'id': step_id,
                    'name': 'Restore operations',
                    'phase': 'end',
                    'status': 'error',
                    'started_at': started_at,
                    'ended_at': _utc_now_iso_seconds(),
                    'error': f"Timed out waiting for briefing_data.json after reset ({timeout_sec}s)",
                },
            )
            return

        # Run restore script (same as /api/restore_user_ops_via_ai).
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(base_dir, 'pipeline', 'match_user_ops_to_briefing_ai.py')
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Missing script: {script_path}")

            cmd = [
                sys.executable,
                script_path,
                '--ops-store', PERSISTED_USER_OP_FILE,
                '--briefing', str(data_path),
                '--user-ops-out', USER_OP_FILE,
                '--write-user-ops',
                '--prune-unmatched-store',
                '--ops-batch-size', '5',
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                err = (result.stderr or result.stdout or '').strip()
                if not err:
                    err = 'Restore failed'
                raise RuntimeError(err)

            _update_pipeline_status_with_step_event(
                status_path,
                step_event={
                    'id': step_id,
                    'name': 'Restore operations',
                    'phase': 'end',
                    'status': 'ok',
                    'started_at': started_at,
                    'ended_at': _utc_now_iso_seconds(),
                    'input_files': [PERSISTED_USER_OP_FILE, str(data_path)],
                    'output_files': [USER_OP_FILE],
                },
            )
        except Exception as e:
            _update_pipeline_status_with_step_event(
                status_path,
                step_event={
                    'id': step_id,
                    'name': 'Restore operations',
                    'phase': 'end',
                    'status': 'error',
                    'started_at': started_at,
                    'ended_at': _utc_now_iso_seconds(),
                    'error': str(e),
                },
            )

    t = threading.Thread(target=worker, name='auto-restore-user-ops', daemon=True)
    t.start()


def _is_demo_mode() -> bool:
    try:
        v = (request.args.get('demo') or '').strip().lower()
        return v in ('1', 'true', 'yes')
    except Exception:
        return False


def _app_index_path() -> Path:
    return Path(app.static_folder) / _APP_SUBDIR / 'index.html'


def _serve_app_index() -> Any:
    """Serve the built React index.html from static/app, optionally injecting demo overlay."""
    idx = _app_index_path()
    if not _is_demo_mode():
        resp = make_response(send_from_directory(str(idx.parent), idx.name))
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    # Demo mode: inject overlay loader without modifying the build output on disk.
    try:
        html = idx.read_text(encoding='utf-8')
    except Exception:
        return send_from_directory(str(idx.parent), idx.name)

    injection = (
        "\n<link rel=\"stylesheet\" href=\"/demo_mode.css\">\n"
        "<script src=\"/demo_mode.js\"></script>\n"
    )

    if '</head>' in html:
        html = html.replace('</head>', injection + '</head>', 1)
    elif '</body>' in html:
        html = html.replace('</body>', injection + '</body>', 1)
    else:
        html = html + injection

    resp = make_response(html)
    resp.headers['Content-Type'] = 'text/html; charset=utf-8'
    resp.headers['Cache-Control'] = 'no-store'
    return resp

BASE_DIR = Path(__file__).resolve().parent
PATHS = RepoPaths(BASE_DIR)


# --- OneDrive todos.json monitor ---

_TODOS_MONITOR_LOCK = threading.Lock()
_TODOS_MONITOR = None  # type: ignore


def _onedrive_ai_secretary_dirs() -> list[Path]:
    """Return the OneDrive ai-secretary folder to watch.

    Prefer the correctly-spelled folder name. Only fall back to the common
    misspelling if that is the only existing folder.
    """
    root = _resolve_onedrive_root()
    if root is None:
        return []
    preferred = (root / 'ai-secretary').resolve()
    fallback = (root / 'ai-secretrary').resolve()

    try:
        if preferred.exists() and preferred.is_dir():
            return [preferred]
    except Exception:
        pass

    try:
        if fallback.exists() and fallback.is_dir():
            return [fallback]
    except Exception:
        pass

    return []


def _todos_monitor_config_path() -> Path:
    return (_paths().user_state_dir() / 'todos_monitor.json').resolve()


def _load_todos_monitor_config() -> dict:
    default = {
        'enabled': False,
        'auto_start': False,
        'interval_sec': 30,
    }
    try:
        cfg = json_io.load_json_best_effort(_todos_monitor_config_path(), default, base_dir=None)
        return cfg if isinstance(cfg, dict) else dict(default)
    except Exception:
        return dict(default)


def _save_todos_monitor_config(cfg: dict) -> None:
    try:
        json_io.save_json_best_effort(_todos_monitor_config_path(), cfg, base_dir=None)
    except Exception:
        pass


def _persist_user_operation_to_store(op_type: str, item_id: str, is_active: bool, op_context: dict | None) -> None:
    """Persist user operation into the stable ops store (survives incremental_data resets)."""
    try:
        now = _utc_now_iso()

        store = _load_persisted_ops_store()
        ops_by_fp = store.get('ops_by_fingerprint')
        if not isinstance(ops_by_fp, dict):
            ops_by_fp = {}
            store['ops_by_fingerprint'] = ops_by_fp

        item_id_str = str(item_id)

        # Primary identity within the same dataset is last_seen_ui_id.
        keys_by_ui = [
            fp_key
            for fp_key, entry in list(ops_by_fp.items())
            if isinstance(entry, dict)
            and str(entry.get('last_seen_ui_id')) == item_id_str
            and entry.get('op') == op_type
        ]

        fp_ctx = None
        if op_context and isinstance(op_context, dict):
            fp_ctx = _compute_action_fingerprint_from_context(op_context)

        fp_lookup = None
        ctx_payload = None
        if fp_ctx is None:
            data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())
            briefing_data = {}
            if data_path and os.path.exists(data_path):
                with open(data_path, 'r', encoding='utf-8') as f:
                    briefing_data = json.load(f)

            ctx = _find_action_context_by_ui_id(briefing_data, item_id_str)
            if ctx:
                card_type, container_key, bucket, item, extra = ctx
                fp_lookup = _compute_action_fingerprint(card_type, container_key, item, bucket)
                ctx_payload = {
                    'card_type': card_type,
                    'bucket': bucket,
                    'container_key': container_key,
                    **(extra or {}),
                    'text': _get_item_text(item),
                    'deadline': (item.get('deadline') or (item.get('original_data') or {}).get('deadline')),
                    'owner': (item.get('owner') or (item.get('original_data') or {}).get('owner')),
                    'assignees': (
                        item.get('assignees')
                        if isinstance(item.get('assignees'), list)
                        else (item.get('original_data') or {}).get('assignees')
                    ),
                    'original_quote': (item.get('original_quote') or (item.get('original_data') or {}).get('original_quote')),
                }

        if not bool(is_active):
            keys_to_remove = set(keys_by_ui)
            if fp_ctx and fp_ctx in ops_by_fp:
                keys_to_remove.add(fp_ctx)
            if fp_lookup and fp_lookup in ops_by_fp:
                keys_to_remove.add(fp_lookup)

            for k in keys_to_remove:
                if k in ops_by_fp:
                    del ops_by_fp[k]
            _save_persisted_ops_store(store)
            return

        primary_fp = fp_ctx or fp_lookup
        if not primary_fp:
            return

        for k in keys_by_ui:
            if k != primary_fp and k in ops_by_fp:
                del ops_by_fp[k]

        existing = ops_by_fp.get(primary_fp) if isinstance(ops_by_fp.get(primary_fp), dict) else {}
        first_seen = existing.get('first_seen') or now

        context_payload = None
        if op_context and isinstance(op_context, dict):
            op_context.setdefault('card_type', op_context.get('source'))
            op_context.setdefault('bucket', None)
            op_context['last_seen_ui_id'] = item_id_str
            context_payload = op_context
        elif ctx_payload is not None:
            context_payload = ctx_payload

        ops_by_fp[primary_fp] = {
            'op': op_type,
            'active': True,
            'first_seen': first_seen,
            'last_updated': now,
            'last_seen_ui_id': item_id_str,
            'context': context_payload or {},
        }
        _save_persisted_ops_store(store)
    except Exception as e:
        print(f"[WARN] Failed to persist user operation context: {e}")


def _apply_complete_ui_id(ui_id: str) -> bool:
    """Mark an action item (_ui_id) completed in user_operation.json + persisted store."""
    item_id = str(ui_id or '').strip()
    if not item_id:
        return False

    ops = load_user_ops()
    ops.setdefault('completed_ai', [])
    ops.setdefault('dismissed_ai', [])
    ops.setdefault('completed', [])
    ops.setdefault('dismissed', [])

    changed = False
    if item_id not in ops['completed']:
        ops['completed'].append(item_id)
        changed = True

    # Manual action overrides any AI-labeled state.
    if item_id in ops.get('completed_ai', []):
        ops['completed_ai'].remove(item_id)
        changed = True
    if item_id in ops.get('dismissed_ai', []):
        ops['dismissed_ai'].remove(item_id)
        changed = True
    if item_id in ops.get('dismissed', []):
        ops['dismissed'].remove(item_id)
        changed = True

    if changed:
        save_user_ops(ops)
        _persist_user_operation_to_store('complete', item_id, True, None)
    return changed


def _extract_completed_task_ids_from_todos_payload(payload: dict) -> list[str]:
    """Extract selected task ids from a Power Automate submit payload."""
    if not isinstance(payload, dict):
        return []
    data = payload.get('data')
    if not isinstance(data, dict):
        data = payload

    selected_nums: list[str] = []
    for k, v in data.items():
        m = re.match(r'^task-(\d+)-selected$', str(k))
        if not m:
            continue
        truthy = str(v).strip().lower() in ('1', 'true', 'yes', 'on')
        if truthy:
            selected_nums.append(m.group(1))

    task_ids: list[str] = []
    for n in selected_nums:
        tid = data.get(f'task-{n}-task_id')
        s = str(tid or '').strip()
        if s:
            task_ids.append(s)
    # de-dupe while preserving order
    out: list[str] = []
    seen: set[str] = set()
    for t in task_ids:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _find_ui_ids_for_stable_card_id(briefing_data: dict, stable_card_id: str) -> list[str]:
    stable = str(stable_card_id or '').strip()
    if not stable:
        return []
    cards = briefing_data.get('cards')
    if not isinstance(cards, list):
        return []

    ui_ids: list[str] = []
    seen: set[str] = set()

    for card in cards:
        if not isinstance(card, dict):
            continue
        try:
            ctype = str(card.get('type') or '').strip()
            data = card.get('data') if isinstance(card.get('data'), dict) else {}
            if ctype == 'Outlook':
                raw = data.get('event_id') or data.get('eventId') or data.get('id') or data.get('event_name') or ''
            else:
                conv = data.get('conversation') if isinstance(data.get('conversation'), dict) else {}
                raw = conv.get('conversation_id') or conv.get('chat_id') or conv.get('chatId') or conv.get('chat_name') or ''
            s = re.sub(r'[^a-zA-Z0-9_-]', '-', str(raw).strip())
            s = re.sub(r'-+', '-', s).strip('-')[:120] or 'unknown'
            cid = (ctype.lower() or 'unknown') + '|' + s
        except Exception:
            cid = ''
        if cid != stable:
            continue

        data = card.get('data')
        if not isinstance(data, dict):
            continue

        buckets = []
        if str(card.get('type') or '') == 'Outlook':
            buckets = ['todos', 'recommendations']
        elif str(card.get('type') or '') == 'Teams':
            buckets = ['linked_items', 'unlinked_items']

        for b in buckets:
            items = data.get(b)
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict):
                    continue
                ui = str(it.get('_ui_id') or '').strip()
                if not ui or ui in seen:
                    continue
                seen.add(ui)
                ui_ids.append(ui)

    return ui_ids


class _TodosMonitor:
    def __init__(self) -> None:
        cfg = _load_todos_monitor_config()
        self.interval_sec = int(cfg.get('interval_sec') or 30)
        self.auto_start = bool(cfg.get('auto_start'))
        self.enabled = bool(cfg.get('enabled'))

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self.started_at: str | None = None
        self.start_attempts: int = 0
        self.last_start_error: str | None = None

        # One-shot execution (Run once)
        self._run_once_lock = threading.Lock()
        self._run_once_busy: bool = False
        self.last_run_once_started_at: str | None = None
        self.last_run_once_finished_at: str | None = None

        self.last_error: str | None = None
        self.last_checked_at: str | None = None
        self.check_seq: int = 0
        self.last_check_summary: str | None = None
        self.last_scan_dirs: list[str] = []
        self.last_found_files: list[str] = []
        self.last_processed_at: str | None = None
        self.last_processed_count: int = 0
        self.last_already_completed_count: int = 0
        self.last_selected_count: int = 0
        self.last_unmapped_task_ids: list[str] = []
        self.last_deleted_at: str | None = None
        self.last_deleted_filename: str | None = None
        self.last_detected_filename: str | None = None
        self.last_detected_task_ids: list[str] = []
        self.last_mapped_ui_ids_count: int = 0
        self.refresh_seq: int = 0
        self.last_seen_path: str | None = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and not self._stop.is_set()

    def set_config(self, *, enabled: bool | None = None, auto_start: bool | None = None, interval_sec: int | None = None) -> None:
        if enabled is not None:
            self.enabled = bool(enabled)
        if auto_start is not None:
            self.auto_start = bool(auto_start)
        if interval_sec is not None:
            try:
                v = int(interval_sec)
                if v < 5:
                    v = 5
                if v > 3600:
                    v = 3600
                self.interval_sec = v
            except Exception:
                pass

        _save_todos_monitor_config({
            'enabled': bool(self.enabled),
            'auto_start': bool(self.auto_start),
            'interval_sec': int(self.interval_sec),
        })

        if self.enabled:
            self.start()
        else:
            self.stop()

    def start(self) -> None:
        if self.is_running():
            return
        self.start_attempts += 1
        self.last_start_error = None
        try:
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name='ai-secretary-todos-monitor', daemon=True)
            self._thread.start()
            self.started_at = _utc_now_iso()
        except Exception as e:
            self.last_start_error = str(e)

    def stop(self) -> None:
        self._stop.set()

    def _process_available_input_once(self) -> dict:
        """Process at most one input file (todos.json/actions.json) if present."""
        scan_dirs = _onedrive_ai_secretary_dirs()
        self.last_scan_dirs = [str(d) for d in scan_dirs]

        paths = self._candidate_todos_paths()
        try:
            self.last_found_files = [p.name for p in paths if p.exists() and p.is_file()]
        except Exception:
            self.last_found_files = []

        found = next((p for p in paths if p.exists() and p.is_file()), None)
        if found is None:
            self.last_check_summary = f'No input file found (scanned {len(scan_dirs)} dirs)'
            return {'found': False}

        self.last_seen_path = str(found)
        self.last_detected_filename = found.name

        payload = None
        try:
            payload = json.loads(found.read_text(encoding='utf-8'))
        except Exception as e:
            self.last_error = f'Failed to parse {found.name}: {e}'
            self.last_check_summary = f'Found {found.name} but failed to parse'
            return {'found': True, 'parsed': False}

        task_ids = _extract_completed_task_ids_from_todos_payload(payload if isinstance(payload, dict) else {})
        self.last_detected_task_ids = list(task_ids)
        self.last_selected_count = int(len(task_ids))

        if not task_ids:
            # Nothing selected; delete file to avoid stuck state.
            try:
                found.unlink(missing_ok=True)
            except Exception:
                pass
            self.last_deleted_at = _utc_now_iso()
            self.last_deleted_filename = found.name
            self.last_processed_at = self.last_deleted_at
            self.last_processed_count = 0
            self.last_already_completed_count = 0
            self.last_mapped_ui_ids_count = 0
            self.last_unmapped_task_ids = []
            self.last_check_summary = f'Found {found.name} with 0 selected; deleted input'
            self.refresh_seq += 1
            return {'found': True, 'parsed': True, 'selected': 0, 'deleted': True}

        data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())
        briefing_data = {}
        if data_path and os.path.exists(data_path):
            with open(data_path, 'r', encoding='utf-8') as f:
                briefing_data = json.load(f)

        ui_ids: list[str] = []
        seen_ui: set[str] = set()
        unmapped: list[str] = []
        for stable_id in task_ids:
            matched = _find_ui_ids_for_stable_card_id(briefing_data, stable_id)
            if not matched:
                unmapped.append(stable_id)
                continue
            for ui in matched:
                if ui in seen_ui:
                    continue
                seen_ui.add(ui)
                ui_ids.append(ui)

        self.last_unmapped_task_ids = list(unmapped)
        self.last_mapped_ui_ids_count = int(len(ui_ids))

        newly_completed = 0
        for ui in ui_ids:
            if _apply_complete_ui_id(ui):
                newly_completed += 1

        already_completed = max(0, int(len(ui_ids) - newly_completed))

        self.last_processed_at = _utc_now_iso()
        self.last_processed_count = int(newly_completed)
        self.last_already_completed_count = int(already_completed)
        self.refresh_seq += 1

        self.last_check_summary = (
            f'Processed {found.name}: selected={len(task_ids)} '
            f'mapped={len(ui_ids)} newly_completed={newly_completed} '
            f'already_completed={already_completed} unmapped={len(unmapped)}'
        )

        try:
            found.unlink(missing_ok=True)
        except Exception:
            pass
        self.last_deleted_at = _utc_now_iso()
        self.last_deleted_filename = found.name

        return {
            'found': True,
            'parsed': True,
            'filename': found.name,
            'selected': len(task_ids),
            'mapped': len(ui_ids),
            'newly_completed': newly_completed,
            'already_completed': already_completed,
            'unmapped': len(unmapped),
            'deleted': True,
        }

    def run_once(self) -> dict:
        """Run the job exactly once and return a summary."""
        acquired = self._run_once_lock.acquire(blocking=False)
        if not acquired:
            return {'started': False, 'reason': 'busy'}
        try:
            self._run_once_busy = True
            self.last_run_once_started_at = _utc_now_iso()
            self.last_error = None
            self.check_seq += 1
            self.last_checked_at = _utc_now_iso()
            summary = self._process_available_input_once()
            self.last_run_once_finished_at = _utc_now_iso()
            return {'started': True, 'summary': summary}
        except Exception as e:
            self.last_error = str(e)
            self.last_check_summary = 'Error during run_once'
            self.last_run_once_finished_at = _utc_now_iso()
            return {'started': True, 'error': str(e)}
        finally:
            self._run_once_busy = False
            try:
                self._run_once_lock.release()
            except Exception:
                pass

    def _candidate_todos_paths(self) -> list[Path]:
        out: list[Path] = []
        for d in _onedrive_ai_secretary_dirs():
            try:
                if d.exists() and d.is_dir():
                    # Primary: todos.json (spec)
                    out.append((d / 'todos.json').resolve())
                    # Back-compat / common Flow output name
                    out.append((d / 'actions.json').resolve())
            except Exception:
                continue
        return out

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.check_seq += 1
                self.last_checked_at = _utc_now_iso()
                self.last_error = None

                self._process_available_input_once()

            except Exception as e:
                self.last_error = str(e)
                self.last_check_summary = 'Error during check'

            time.sleep(max(0.25, float(self.interval_sec)))

    def status_payload(self) -> dict:
        dirs = [str(p) for p in _onedrive_ai_secretary_dirs()]
        state = 'running' if (bool(self._run_once_busy) or bool(self.is_running())) else 'idle'
        return {
            'state': state,
            'running': bool(self.is_running()),
            'enabled': bool(self.enabled),
            'auto_start': bool(self.auto_start),
            'interval_sec': int(self.interval_sec),
            'stop_requested': bool(self._stop.is_set()),
            'started_at': self.started_at,
            'start_attempts': int(self.start_attempts),
            'last_start_error': self.last_start_error,
            'onedrive_dirs': dirs,
            'last_checked_at': self.last_checked_at,
            'check_seq': int(self.check_seq),
            'last_check_summary': self.last_check_summary,
            'last_scan_dirs': list(self.last_scan_dirs or []),
            'last_found_files': list(self.last_found_files or []),
            'last_processed_at': self.last_processed_at,
            'last_processed_count': int(self.last_processed_count),
            'last_already_completed_count': int(self.last_already_completed_count),
            'last_selected_count': int(self.last_selected_count),
            'last_unmapped_task_ids': list(self.last_unmapped_task_ids or []),
            'last_deleted_at': self.last_deleted_at,
            'last_deleted_filename': self.last_deleted_filename,
            'last_run_once_started_at': self.last_run_once_started_at,
            'last_run_once_finished_at': self.last_run_once_finished_at,
            'last_detected_filename': self.last_detected_filename,
            'last_detected_task_ids': list(self.last_detected_task_ids or []),
            'last_mapped_ui_ids_count': int(self.last_mapped_ui_ids_count),
            'refresh_seq': int(self.refresh_seq),
            'last_seen_path': self.last_seen_path,
            'last_error': self.last_error,
        }


def _get_todos_monitor() -> _TodosMonitor:
    global _TODOS_MONITOR
    with _TODOS_MONITOR_LOCK:
        if _TODOS_MONITOR is None:
            _TODOS_MONITOR = _TodosMonitor()
            # Auto-start if configured.
            if bool(_TODOS_MONITOR.enabled) and bool(_TODOS_MONITOR.auto_start):
                try:
                    _TODOS_MONITOR.start()
                except Exception:
                    pass
        return _TODOS_MONITOR


def _resolve_onedrive_root() -> Path | None:
    # Common env var names on Windows.
    for key in ('OneDrive', 'OneDriveCommercial', 'OneDriveConsumer'):
        raw = os.environ.get(key)
        if isinstance(raw, str) and raw.strip():
            p = Path(raw.strip())
            try:
                if p.exists() and p.is_dir():
                    return p
            except Exception:
                continue

    # Common fallback location.
    try:
        home = Path.home()
        # 1) Standard personal OneDrive folder
        p = home / 'OneDrive'
        if p.exists() and p.is_dir():
            return p

        # 2) Common enterprise naming: "OneDrive - <Org>" (e.g., "OneDrive - Microsoft")
        candidates: list[Path] = []
        try:
            for child in home.iterdir():
                name = child.name
                if not name.lower().startswith('onedrive'):
                    continue
                if child.exists() and child.is_dir():
                    candidates.append(child)
        except Exception:
            candidates = []

        # Prefer a OneDrive root that already contains the expected ai-secretary folder.
        for c in candidates:
            try:
                if (c / 'ai-secretary').exists() or (c / 'ai-secretrary').exists():
                    return c
            except Exception:
                continue

        # Otherwise, if there's exactly one OneDrive-like folder, use it.
        if len(candidates) == 1:
            return candidates[0]
    except Exception:
        pass

    return None


def _onedrive_adaptive_card_dir() -> Path | None:
    root = _resolve_onedrive_root()
    if root is None:
        return None
    # Use the plural folder name to match the per-task card export location.
    return (root / 'ai-secretary' / 'adaptive-cards').resolve()


def _onedrive_top_tasks_path() -> Path | None:
    d = _onedrive_adaptive_card_dir()
    if d is None:
        return None
    return (d / 'top-tasks.json').resolve()


@app.route('/api/todos_monitor/status', methods=['GET'])
def todos_monitor_status():
    try:
        mon = _get_todos_monitor()
        # One-shot UX: do not auto-start the background thread from a status poll.
        return jsonify({'status': 'ok', 'monitor': mon.status_payload()})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/todos_monitor/config', methods=['GET', 'POST'])
def todos_monitor_config():
    try:
        mon = _get_todos_monitor()
        if request.method == 'GET':
            return jsonify({'status': 'ok', 'monitor': mon.status_payload()})

        payload = request.json or {}
        enabled = payload.get('enabled') if 'enabled' in payload else None
        auto_start = payload.get('auto_start') if 'auto_start' in payload else None
        interval_sec = payload.get('interval_sec') if 'interval_sec' in payload else None

        mon.set_config(
            enabled=(bool(enabled) if enabled is not None else None),
            auto_start=(bool(auto_start) if auto_start is not None else None),
            interval_sec=(int(interval_sec) if interval_sec is not None else None),
        )
        return jsonify({'status': 'ok', 'monitor': mon.status_payload()})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/todos_monitor/run_once', methods=['POST'])
def todos_monitor_run_once():
    try:
        mon = _get_todos_monitor()
        res = mon.run_once()
        return jsonify({'status': 'ok', 'result': res, 'monitor': mon.status_payload()})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


def _shutdown_todos_monitor_best_effort() -> None:
    try:
        mon = _get_todos_monitor()
        mon.stop()
    except Exception:
        pass


import atexit  # noqa: E402

atexit.register(_shutdown_todos_monitor_best_effort)


def _append_timestamp_to_filename(path: Path, *, timestamp_iso: str) -> Path:
    safe_ts = str(timestamp_iso or '').strip() or _utc_now_iso()
    safe_ts = safe_ts.replace(':', '-').replace('.', '-')
    return path.with_name(f"{path.stem}_{safe_ts}{path.suffix}")


# --- Pipeline process control ---

_PIPELINE_LOCK = threading.Lock()
_PIPELINE_PROCESS = None  # type: ignore
_PIPELINE_STATE_LOCK = threading.Lock()
_LAST_PIPELINE_STATE = None  # type: ignore

# --- Windows Job Object: auto-kill pipeline when parent process exits ---
_PIPELINE_JOB_HANDLE = None  # Prevents GC from closing the handle

def _get_or_create_job_object():
    """Create a Windows Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE.

    When the last handle to this job is closed (i.e. the desktop app process
    exits — even via crash or Task Manager kill), Windows will automatically
    terminate all processes assigned to the job.  This is the most reliable
    way to ensure the pipeline console window doesn't outlive the app.
    """
    global _PIPELINE_JOB_HANDLE
    if _PIPELINE_JOB_HANDLE is not None:
        return _PIPELINE_JOB_HANDLE
    if os.name != 'nt':
        return None
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)

        kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None

        # --- struct definitions for SetInformationJobObject ---
        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('PerProcessUserTimeLimit', wintypes.LARGE_INTEGER),
                ('PerJobUserTimeLimit', wintypes.LARGE_INTEGER),
                ('LimitFlags', wintypes.DWORD),
                ('MinimumWorkingSetSize', ctypes.c_size_t),
                ('MaximumWorkingSetSize', ctypes.c_size_t),
                ('ActiveProcessLimit', wintypes.DWORD),
                ('Affinity', ctypes.c_size_t),
                ('PriorityClass', wintypes.DWORD),
                ('SchedulingClass', wintypes.DWORD),
            ]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ('ReadOperationCount', ctypes.c_ulonglong),
                ('WriteOperationCount', ctypes.c_ulonglong),
                ('OtherOperationCount', ctypes.c_ulonglong),
                ('ReadTransferCount', ctypes.c_ulonglong),
                ('WriteTransferCount', ctypes.c_ulonglong),
                ('OtherTransferCount', ctypes.c_ulonglong),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('BasicLimitInformation', JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ('IoInfo', IO_COUNTERS),
                ('ProcessMemoryLimit', ctypes.c_size_t),
                ('JobMemoryLimit', ctypes.c_size_t),
                ('PeakProcessMemoryUsed', ctypes.c_size_t),
                ('PeakJobMemoryUsed', ctypes.c_size_t),
            ]

        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
        JobObjectExtendedLimitInformation = 9

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL

        ok = kernel32.SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not ok:
            kernel32.CloseHandle(job)
            return None

        _PIPELINE_JOB_HANDLE = job
        return job
    except Exception:
        return None


def _assign_process_to_job(proc) -> bool:
    """Assign a subprocess.Popen to the pipeline Job Object."""
    job = _get_or_create_job_object()
    if job is None or proc is None:
        return False
    try:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        return bool(kernel32.AssignProcessToJobObject(job, int(proc._handle)))
    except Exception:
        return False

_BRIEFING_DATA_LOCK = threading.Lock()

# --- Auto-cleanup: stop pipeline when server process exits ---

def _atexit_stop_pipeline() -> None:
    """Best-effort pipeline cleanup on server exit (atexit / signal)."""
    global _PIPELINE_PROCESS
    proc = _PIPELINE_PROCESS
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return  # Already dead
        pid = getattr(proc, 'pid', None)
        print(f"[PIPELINE] Server exiting — killing pipeline (pid={pid})...", flush=True)
        if os.name == 'nt' and pid:
            subprocess.call(
                ['taskkill', '/F', '/T', '/PID', str(pid)],
                timeout=8,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    except Exception:
        pass
    finally:
        _PIPELINE_PROCESS = None
        # Remove stale PID file
        try:
            pid_path = Path(BASE_DIR) / 'pipeline_pid.txt'
            if pid_path.exists():
                pid_path.unlink()
        except Exception:
            pass

atexit.register(_atexit_stop_pipeline)

# Also handle SIGTERM / SIGINT so pipeline dies even if process is killed externally.
def _signal_cleanup_handler(signum, frame):
    _atexit_stop_pipeline()
    # Re-raise the default behaviour
    raise SystemExit(128 + signum)

try:
    signal.signal(signal.SIGTERM, _signal_cleanup_handler)
except (OSError, ValueError):
    pass  # Can't set SIGTERM on some platforms / threads

_PIPELINE_AUTOSTART_ATTEMPTED = False
_PIPELINE_USER_STOPPED = False          # Set when user explicitly stops; prevents auto-start / false "working" status

INCREMENTAL_DATA_DIR = PATHS.incremental_data_dirname
INCREMENTAL_DATA_BACKUP_DIR = PATHS.incremental_backup_dirname  # legacy default; dynamic per-dataset backup is computed at runtime
PIPELINE_STATUS_FILE = PATHS.pipeline_status_filename

OBSERVATION_SNAPSHOT_DIRNAME = "observation_runs"

# Deprecated legacy storage for removed per-card like/dislike feedback.
CARD_FEEDBACK_FILE = str(PATHS.card_feedback_file().relative_to(BASE_DIR))
TASK_VECTORS_FILE = str(PATHS.task_vectors_file().relative_to(BASE_DIR))
# Deprecated legacy storage for the removed feedback-derived focus model.
FOCUS_MODEL_FILE = str(PATHS.focus_model_file().relative_to(BASE_DIR))
TASK_VECTOR_DIM = 512


def _paths() -> RepoPaths:
    return PATHS


def _incremental_data_path() -> Path:
    # Prefer the configured dataset folder (active_data_folder_path) so all
    # server-managed operations (reset/backup/export/etc.) stay dataset-consistent.
    try:
        cfg = load_config()
    except Exception:
        cfg = None
    try:
        return _resolve_data_folder_path(cfg)
    except Exception:
        return Path(_repo_root_dir()) / INCREMENTAL_DATA_DIR


def _incremental_backup_path() -> Path:
    # IMPORTANT: incremental_data may be a symlink/junction to a dataset folder.
    # Always derive the backup next to the *resolved* dataset folder to avoid
    # collisions when multiple datasets are used.
    try:
        src = _incremental_data_path().resolve()
        return src.parent / f"{src.name}_backup"
    except Exception:
        return Path(_repo_root_dir()) / INCREMENTAL_DATA_BACKUP_DIR


def _incremental_backup_spec() -> tuple[Path, str, str]:
    """Return (base_dir, incremental_dirname, backup_dirname) for pipeline_state.* calls."""
    try:
        src = _incremental_data_path().resolve()
    except Exception:
        src = _incremental_data_path()
    base_dir = src.parent
    incremental_dirname = src.name
    backup_dirname = f"{incremental_dirname}_backup"
    return base_dir, incremental_dirname, backup_dirname


def _pipeline_status_path() -> Path:
    try:
        cfg = load_config()
    except Exception:
        cfg = None
    folder = _resolve_data_folder_path(cfg)
    return folder / PIPELINE_STATUS_FILE


def _observation_snapshots_dir() -> Path:
    try:
        cfg = load_config()
    except Exception:
        cfg = None
    try:
        folder = _resolve_data_folder_path(cfg)
    except Exception:
        folder = _incremental_data_path()
    return folder / OBSERVATION_SNAPSHOT_DIRNAME


def _load_latest_observation_snapshot() -> dict[str, Any] | None:
    try:
        d = _observation_snapshots_dir()
        if not d.exists():
            return None
        files = [p for p in d.glob('*.json') if p.is_file()]
        if not files:
            return None
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        with open(files[0], 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _pipeline_is_running() -> bool:
    global _PIPELINE_PROCESS
    try:
        return _PIPELINE_PROCESS is not None and _PIPELINE_PROCESS.poll() is None
    except Exception:
        return False


def _incremental_data_present() -> bool:
    """Return True if incremental_data exists and appears non-empty."""
    try:
        d = _incremental_data_path()
        if not d.exists() or not d.is_dir():
            return False
        # Consider it present if any file exists under the directory.
        for p in d.rglob('*'):
            if p.is_file():
                return True
        return False
    except Exception:
        # If we can't determine, assume it's present to avoid surprising skips.
        return True


def _truthy(v) -> bool:
    try:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ('1', 'true', 'yes', 'y', 'on')
    except Exception:
        return False


def _pipeline_auto_start_enabled(cfg: dict | None) -> bool:
    if not isinstance(cfg, dict):
        return False
    return _truthy(cfg.get('run_pipeline_after_start')) or _truthy(cfg.get('schedule_enabled'))


def _autostart_pipeline_if_enabled(*, once: bool = True) -> None:
    """Start the pipeline when enabled in config or when schedule is active."""
    global _PIPELINE_AUTOSTART_ATTEMPTED
    if once and _PIPELINE_AUTOSTART_ATTEMPTED:
        return
    if once:
        _PIPELINE_AUTOSTART_ATTEMPTED = True

    try:
        cfg = load_config()
        if not _pipeline_auto_start_enabled(cfg):
            return
        if _PIPELINE_USER_STOPPED:
            return

        with _PIPELINE_LOCK:
            if _pipeline_is_running():
                return

            # Mirror the safety behavior of /api/pipeline_start.
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            pipeline_state.delete_stale_backup_if_coexists(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            pipeline_state.create_incremental_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            info = _start_pipeline_process()
            reason = "schedule active" if cfg.get('schedule_enabled') else "run_pipeline_after_start=true"
            print(f"[PIPELINE] Auto-started (pid={info.get('pid')}, reason: {reason})")
    except Exception as e:
        print(f"[PIPELINE] Auto-start error: {e}")


def _kickoff_pipeline_autostart_background(*, once: bool = True) -> None:
    """Fire-and-forget wrapper to avoid blocking request/startup."""
    try:
        t = threading.Thread(target=_autostart_pipeline_if_enabled, kwargs={'once': once}, daemon=True)
        t.start()
    except Exception:
        pass


def _pipeline_logs_dir() -> str:
    path = os.path.join(_repo_root_dir(), 'user_state')
    os.makedirs(path, exist_ok=True)
    return path


def _user_state_dir() -> str:
    path = os.path.join(_repo_root_dir(), 'user_state')
    os.makedirs(path, exist_ok=True)
    return path


def _load_json_best_effort(path: str, default: Any) -> Any:
    return json_io.load_json_best_effort(path, default, base_dir=_repo_root_dir())


def _save_json_best_effort(path: str, obj: Any) -> None:
    # Preserve existing behavior: ensure user_state exists (some callers rely on it)
    # then save best-effort without raising.
    try:
        _user_state_dir()
    except Exception:
        pass
    json_io.save_json_best_effort(path, obj, base_dir=_repo_root_dir())


def _sanitize_id_part(raw: Any) -> str:
    s = str(raw or '').strip()
    if not s:
        return 'unknown'
    s = ''.join(ch if (ch.isalnum() or ch in '_-') else '-' for ch in s)
    while '--' in s:
        s = s.replace('--', '-')
    s = s.strip('-')
    return s[:120] if s else 'unknown'


def _stable_card_key(card: dict) -> str:
    card_type = str(card.get('type') or '').strip()
    data = card.get('data') if isinstance(card.get('data'), dict) else {}
    if card_type.lower() == 'outlook':
        return _sanitize_id_part(data.get('event_id') or data.get('eventId') or data.get('id') or data.get('event_name'))
    conv = data.get('conversation') if isinstance(data.get('conversation'), dict) else {}
    return _sanitize_id_part(conv.get('conversation_id') or conv.get('chat_id') or conv.get('chatId') or conv.get('chat_name'))


def _card_id(card_type: str, card_key: str) -> str:
    return f"{str(card_type).lower()}|{str(card_key)}"


def _load_card_feedback() -> dict:
    fb = _load_json_best_effort(CARD_FEEDBACK_FILE, {})
    return fb if isinstance(fb, dict) else {}


def _save_card_feedback(fb: dict) -> None:
    _save_json_best_effort(CARD_FEEDBACK_FILE, fb)


def _load_task_vectors() -> dict:
    store = _load_json_best_effort(TASK_VECTORS_FILE, {})
    if isinstance(store, dict) and store:
        return store

    return {}


def _save_task_vectors(store: dict) -> None:
    try:
        _incremental_data_path().mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    _save_json_best_effort(TASK_VECTORS_FILE, store)


def _load_focus_model() -> dict:
    m = _load_json_best_effort(FOCUS_MODEL_FILE, {})
    if isinstance(m, dict) and m:
        return m

    return {}


def _save_focus_model(m: dict) -> None:
    try:
        _incremental_data_path().mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    _save_json_best_effort(FOCUS_MODEL_FILE, m)


def _ensure_task_vectors_for_cards(cards_payload: list) -> dict:
    """Best-effort: compute/update vectors for the current cards."""
    store = _load_task_vectors()
    now = _utc_now_iso()

    for c in cards_payload:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get('type') or '').strip()
        if not ctype:
            continue

        ckey = _stable_card_key(c)
        cid = _card_id(ctype, ckey)

        text = extract_card_text(c)
        th = text_sha256(text)

        prev = store.get(cid)
        if isinstance(prev, dict) and prev.get('text_sha256') == th and isinstance(prev.get('vector'), list):
            continue

        vec = hash_embedding(text, dim=TASK_VECTOR_DIM)
        store[cid] = {
            'v': 1,
            'card_type': ctype,
            'card_key': ckey,
            'text_sha256': th,
            'vector_dim': TASK_VECTOR_DIM,
            'vector': vec,
            'last_updated': now,
        }

    _save_task_vectors(store)
    return store


def _compute_focus_scores(cards_payload: list, vectors_by_id: dict, focus_vec: list) -> dict:
    scores: dict = {}
    if not isinstance(focus_vec, list) or len(focus_vec) != TASK_VECTOR_DIM:
        return scores

    for c in cards_payload:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get('type') or '').strip()
        if not ctype:
            continue
        ckey = _stable_card_key(c)
        cid = _card_id(ctype, ckey)
        rec = vectors_by_id.get(cid)
        vec = rec.get('vector') if isinstance(rec, dict) else None
        if not isinstance(vec, list) or len(vec) != TASK_VECTOR_DIM:
            continue
        try:
            scores[cid] = dot(focus_vec, vec)
        except Exception:
            continue
    return scores


def _start_pipeline_process(
    extra_args: list[str] | None = None,
    *,
    cmd_override: list[str] | None = None,
) -> dict:
    """Start a pipeline-style background job.

    Default: run_incremental_pipeline.py.
    Optional: cmd_override to run a different job while still being tracked by
    the same Start/Stop/Status logic.
    """
    global _PIPELINE_PROCESS
    base_dir = _repo_root_dir()
    cmd: list[str]
    if cmd_override:
        cmd = [str(x) for x in cmd_override if str(x).strip()]
    else:
        script_path = os.path.join(base_dir, 'pipeline', 'run_incremental_pipeline.py')
        if not os.path.exists(script_path):
            raise RuntimeError(f'run_incremental_pipeline.py not found: {script_path}')

        cmd = [sys.executable, script_path, '--no-servers', '--no-browser', '--skip-backup']

        # Only pass --interval when the schedule / recurring fetch is enabled.
        # A manual one-off trigger must NOT get --interval so the process exits
        # after completion instead of sleeping until the next scheduled run.
        try:
            cfg = load_config()
            schedule_enabled = bool(cfg.get('schedule_enabled'))
            interval = cfg.get('fetch_interval_minutes')
            if schedule_enabled and interval and int(interval) > 0:
                cmd.extend(['--interval', str(int(interval))])
        except Exception:
            pass  # no interval → one-off run

        if extra_args:
            cmd.extend([str(x) for x in extra_args if x])

    # On Windows, users asked to see a separate terminal window running the pipeline.
    # Use a new console so logs are visible. (stdout/stderr are not redirected.)
    creationflags = 0
    if os.name == 'nt':
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE

    if not cmd:
        raise RuntimeError('Failed to build pipeline command')

    # Dataset-scoped pipeline status file so switching datasets doesn't mix UI state.
    env = os.environ.copy()
    try:
        status_path = _pipeline_status_path().resolve()
        status_path.parent.mkdir(parents=True, exist_ok=True)
        env['AI_SECRETARY_PIPELINE_STATUS_PATH'] = str(status_path)
    except Exception:
        pass

    _PIPELINE_PROCESS = subprocess.Popen(
        cmd,
        cwd=base_dir,
        creationflags=creationflags,
        env=env,
    )

    # Assign to a Windows Job Object so the pipeline is automatically killed
    # when the desktop app exits (including crashes / Task Manager kills).
    _assign_process_to_job(_PIPELINE_PROCESS)

    # Save PID to file as a fallback for killing stray processes.
    try:
        pid_path = Path(base_dir) / 'pipeline_pid.txt'
        pid_path.write_text(str(_PIPELINE_PROCESS.pid), encoding='utf-8')
    except Exception:
        pass

    info = {'pid': _PIPELINE_PROCESS.pid}
    try:
        logs_dir = _pipeline_logs_dir()
        info['hint_logs_dir'] = logs_dir
    except Exception:
        pass
    return info


def _stop_pipeline_process(timeout_sec: int = 12) -> dict:
    """Best-effort stop of the pipeline subprocess."""
    global _PIPELINE_PROCESS
    if _PIPELINE_PROCESS is None:
        return {'stopped': False, 'reason': 'not_running'}

    proc = _PIPELINE_PROCESS
    pid = getattr(proc, 'pid', None)

    try:
        if proc.poll() is None:
            if os.name == 'nt' and pid:
                # When the pipeline runs in its own console, CTRL_BREAK may not work.
                # Use taskkill /F /T to force-kill the entire process tree.
                try:
                    subprocess.call(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        timeout=10,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception:
                    pass
                # Wait briefly for the kill to take effect.
                try:
                    proc.wait(timeout=min(timeout_sec, 5))
                except Exception:
                    pass
            else:
                proc.terminate()
                try:
                    proc.wait(timeout=timeout_sec)
                except Exception:
                    pass

        # Double-check: if still alive, use proc.kill() as fallback.
        if proc.poll() is None:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass

        # Last resort on Windows: try os.kill to call TerminateProcess directly.
        if proc.poll() is None and pid and os.name == 'nt':
            try:
                import signal as _signal
                os.kill(pid, _signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass  # Already dead — expected

        return {'stopped': True, 'pid': pid, 'exit_code': proc.poll()}
    finally:
        # Drop the handle so a future start works cleanly.
        _PIPELINE_PROCESS = None
        # Clean up PID file.
        try:
            pid_path = Path(_repo_root_dir()) / 'pipeline_pid.txt'
            if pid_path.exists():
                pid_path.unlink()
        except Exception:
            pass


def _kill_stray_pipeline_processes() -> int:
    """Find and kill any stray run_incremental_pipeline.py processes.

    This is a safety net for when _stop_pipeline_process fails or the tracked
    PID doesn't match the actual pipeline process.
    Returns the number of processes killed.
    """
    if os.name != 'nt':
        return 0
    killed = 0
    my_pid = os.getpid()

    # --- Strategy 1: Kill from saved PID file ---
    try:
        pid_path = Path(_repo_root_dir()) / 'pipeline_pid.txt'
        if pid_path.exists():
            saved_pid_str = pid_path.read_text(encoding='utf-8').strip()
            if saved_pid_str.isdigit():
                saved_pid = int(saved_pid_str)
                if saved_pid != my_pid:
                    try:
                        subprocess.call(
                            ['taskkill', '/F', '/T', '/PID', str(saved_pid)],
                            timeout=8,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                        killed += 1
                    except Exception:
                        pass
            try:
                pid_path.unlink()
            except Exception:
                pass
    except Exception:
        pass

    # --- Strategy 2: Use PowerShell to find all matching processes ---
    try:
        ps_cmd = (
            "Get-CimInstance Win32_Process "
            "| Where-Object { $_.CommandLine -like '*run_incremental_pipeline*' -and $_.Name -like '*python*' } "
            "| Select-Object -ExpandProperty ProcessId"
        )
        result = subprocess.run(
            ['powershell', '-NoProfile', '-NoLogo', '-Command', ps_cmd],
            capture_output=True, text=True, timeout=12,
        )
        for line in (result.stdout or '').splitlines():
            line = line.strip()
            if line.isdigit():
                pid = int(line)
                if pid == my_pid:
                    continue
                try:
                    subprocess.call(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        timeout=8,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    killed += 1
                except Exception:
                    pass
    except Exception:
        pass
    return killed


# --- App updater (git-based) ---

_APP_UPDATE_LOCK = threading.Lock()
_APP_UPDATE_STATE = {
    'update_available': False,
    'behind_by': 0,
    'checking': False,
    'updating': False,
    'last_checked': None,
    'last_update': None,
    'error': None,
    'branch': None,
    'upstream': None,
    'head': None,
    'pulled_commits': [],
    'last_pull_exit_code': None,
    'last_pull_stdout': None,
    'last_pull_stderr': None,
    'current': None,
    'latest': None,
    'server_commit': None,
    'server_stale': False,
    'message': None,
}

_APP_UPDATE_THREAD_STARTED = False


def _repo_root_dir() -> str:
    # server_react.py lives at the repo root in this workspace.
    return os.path.abspath(os.path.dirname(__file__))


def _safe_resolve_repo_path(raw_path: str) -> Path:
    """Resolve a user-supplied path to an on-disk path within the repo root.

    Accepts either repo-relative paths (preferred) or absolute paths, but always
    enforces that the resolved target stays under the repo root.
    """
    root = Path(_repo_root_dir()).resolve()
    s = str(raw_path or '').strip()
    if not s:
        raise ValueError('Missing path')

    p = Path(s)
    if not p.is_absolute():
        p = root / p

    resolved = p.resolve()
    try:
        resolved.relative_to(root)
    except Exception:
        raise PermissionError('Path is outside the repo root')
    return resolved


def _is_probably_text(data: bytes) -> bool:
    if not data:
        return True
    # NUL is a strong binary indicator.
    if b'\x00' in data:
        return False
    # Heuristic: if many bytes are non-printable, treat as binary.
    sample = data[:4096]
    non_printable = 0
    for b in sample:
        if b in (9, 10, 13):
            continue
        if 32 <= b <= 126:
            continue
        non_printable += 1
    return (non_printable / max(1, len(sample))) < 0.18


def _run_git(args, timeout_sec=25):
    return subprocess.run(
        ['git', *args],
        cwd=_repo_root_dir(),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )


def _git_stdout(result) -> str:
    return (result.stdout or '').strip()


def _git_stderr(result) -> str:
    return (result.stderr or '').strip()


def _pick_upstream() -> str:
    # Prefer configured upstream (@{u}); fallback to origin/main or origin/master.
    r = _run_git(['rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{u}'], timeout_sec=10)
    if r.returncode == 0:
        return _git_stdout(r)

    remotes = _run_git(['branch', '-r'], timeout_sec=10)
    if remotes.returncode == 0:
        remote_lines = set(l.strip() for l in (remotes.stdout or '').splitlines() if l.strip())
        if 'origin/main' in remote_lines:
            return 'origin/main'
        if 'origin/master' in remote_lines:
            return 'origin/master'

    # As a last resort, try origin/main.
    return 'origin/main'


def _check_for_app_update():
    with _APP_UPDATE_LOCK:
        if _APP_UPDATE_STATE.get('updating'):
            return
        _APP_UPDATE_STATE['checking'] = True
        _APP_UPDATE_STATE['error'] = None

    try:
        inside = _run_git(['rev-parse', '--is-inside-work-tree'], timeout_sec=10)
        if inside.returncode != 0 or _git_stdout(inside).lower() != 'true':
            raise RuntimeError('Not a git work tree')

        branch_r = _run_git(['rev-parse', '--abbrev-ref', 'HEAD'], timeout_sec=10)
        branch = _git_stdout(branch_r) if branch_r.returncode == 0 else None
        upstream = _pick_upstream()
        head_r = _run_git(['rev-parse', 'HEAD'], timeout_sec=10)
        local_head = _git_stdout(head_r) if head_r.returncode == 0 else None

        # Update remote refs.
        fetch_r = _run_git(['fetch', '--all', '--prune'], timeout_sec=60)
        if fetch_r.returncode != 0:
            raise RuntimeError(_git_stderr(fetch_r) or 'git fetch failed')

        latest_r = _run_git(['rev-parse', upstream], timeout_sec=10)
        remote_head = _git_stdout(latest_r) if latest_r.returncode == 0 else None

        remote_msg = None
        if remote_head:
            msg_r = _run_git(['log', '-1', '--format=%s', upstream], timeout_sec=10)
            if msg_r.returncode == 0:
                remote_msg = _git_stdout(msg_r) or None

        behind_r = _run_git(['rev-list', '--count', f'HEAD..{upstream}'], timeout_sec=20)
        behind_by = 0
        if behind_r.returncode == 0:
            try:
                behind_by = int((_git_stdout(behind_r) or '0'))
            except Exception:
                behind_by = 0

        with _APP_UPDATE_LOCK:
            _APP_UPDATE_STATE['branch'] = branch
            _APP_UPDATE_STATE['upstream'] = upstream
            _APP_UPDATE_STATE['behind_by'] = behind_by
            _APP_UPDATE_STATE['update_available'] = behind_by > 0
            _APP_UPDATE_STATE['current'] = local_head[:8] if local_head else None
            _APP_UPDATE_STATE['latest'] = remote_head[:8] if remote_head else None
            _APP_UPDATE_STATE['server_commit'] = _SERVER_GIT_COMMIT[:8] if _SERVER_GIT_COMMIT else None
            _APP_UPDATE_STATE['server_stale'] = bool(_SERVER_GIT_COMMIT and local_head and _SERVER_GIT_COMMIT != local_head)
            _APP_UPDATE_STATE['message'] = remote_msg
            _APP_UPDATE_STATE['last_checked'] = _utc_now_iso()
            _APP_UPDATE_STATE['checking'] = False
    except Exception as e:
        with _APP_UPDATE_LOCK:
            _APP_UPDATE_STATE['checking'] = False
            _APP_UPDATE_STATE['error'] = str(e)


def _start_update_checker_thread(interval_sec: int = 60):
    def loop():
        # Initial check
        _check_for_app_update()
        while True:
            time.sleep(interval_sec)
            _check_for_app_update()

    t = threading.Thread(target=loop, name='app-update-checker', daemon=True)
    t.start()


def _ensure_update_checker_started(interval_sec: int = 60):
    """Start background update checker once.

    Flask debug mode uses a reloader which runs the module twice.
    Use WERKZEUG_RUN_MAIN to only start threads in the actual worker.
    """
    global _APP_UPDATE_THREAD_STARTED
    if _APP_UPDATE_THREAD_STARTED:
        return
    _APP_UPDATE_THREAD_STARTED = True
    _start_update_checker_thread(interval_sec=interval_sec)


def _start_git_pull_thread():
    def do_pull():
        with _APP_UPDATE_LOCK:
            if _APP_UPDATE_STATE.get('updating'):
                return
            _APP_UPDATE_STATE['updating'] = True
            _APP_UPDATE_STATE['error'] = None
            _APP_UPDATE_STATE['last_pull_stdout'] = None
            _APP_UPDATE_STATE['last_pull_stderr'] = None
            _APP_UPDATE_STATE['last_pull_exit_code'] = None
            _APP_UPDATE_STATE['pulled_commits'] = []

        try:
            before_head_r = _run_git(['rev-parse', 'HEAD'], timeout_sec=10)
            before_head = _git_stdout(before_head_r) if before_head_r.returncode == 0 else None
            pull_r = _run_git(['pull', '--ff-only'], timeout_sec=180)
            after_head_r = _run_git(['rev-parse', 'HEAD'], timeout_sec=10)
            after_head = _git_stdout(after_head_r) if after_head_r.returncode == 0 else None

            pulled_commits = []
            if before_head and after_head and before_head != after_head:
                log_r = _run_git(['log', '--oneline', f'{before_head}..{after_head}'], timeout_sec=15)
                if log_r.returncode == 0:
                    pulled_commits = [l.strip() for l in (log_r.stdout or '').splitlines() if l.strip()]

            with _APP_UPDATE_LOCK:
                _APP_UPDATE_STATE['head'] = after_head
                _APP_UPDATE_STATE['pulled_commits'] = pulled_commits
                _APP_UPDATE_STATE['last_pull_exit_code'] = pull_r.returncode
                _APP_UPDATE_STATE['last_pull_stdout'] = _git_stdout(pull_r)
                _APP_UPDATE_STATE['last_pull_stderr'] = _git_stderr(pull_r)
                if pull_r.returncode != 0:
                    _APP_UPDATE_STATE['error'] = _APP_UPDATE_STATE['last_pull_stderr'] or 'git pull failed'
                _APP_UPDATE_STATE['last_update'] = _utc_now_iso()
        except Exception as e:
            with _APP_UPDATE_LOCK:
                _APP_UPDATE_STATE['error'] = str(e)
        finally:
            with _APP_UPDATE_LOCK:
                _APP_UPDATE_STATE['updating'] = False
            # Re-check availability after pulling.
            _check_for_app_update()

    t = threading.Thread(target=do_pull, name='app-update-pull', daemon=True)
    t.start()

# Derived, UI-facing ops file (can be safely deleted with incremental_data)
USER_OP_FILE = str(_paths().user_operation_file())
# Persistent ops store (survives incremental_data cleanup)
PERSISTED_USER_OP_FILE = str(_paths().user_ops_store_file())


def _norm_topic(value: str) -> str:
    return (value or '').strip().casefold()


def _clean_topic_list(items) -> list[str]:
    if not isinstance(items, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for it in items:
        if not isinstance(it, str):
            continue
        cleaned = it.strip()
        if not cleaned:
            continue
        k = _norm_topic(cleaned)
        if k in seen:
            continue
        seen.add(k)
        out.append(cleaned)
    return out


def _ordered_unique(primary: list[str], extra: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for src in (primary, extra):
        for it in src:
            cleaned = str(it).strip()
            if not cleaned:
                continue
            k = _norm_topic(cleaned)
            if k in seen:
                continue
            seen.add(k)
            out.append(cleaned)
    return out


def _ensure_user_topics() -> dict:
    data_path = _paths().user_topics_file()
    data = {}
    try:
        if data_path.exists():
            loaded = json_io.read_json(data_path)
            if isinstance(loaded, dict):
                data = loaded
    except Exception:
        data = {}
    if 'following' not in data:
        data['following'] = []
    if 'not_following' not in data:
        data['not_following'] = []
    data['following'] = _clean_topic_list(data.get('following'))
    data['not_following'] = _clean_topic_list(data.get('not_following'))
    json_io.write_json(data_path, data)
    return data


def _merge_topics(base_topics: dict, user_topics: dict) -> tuple[list[str], list[str]]:
    base_following = _clean_topic_list(base_topics.get('following'))
    base_not = _clean_topic_list(base_topics.get('not_following'))
    user_following = _clean_topic_list(user_topics.get('following'))
    user_not = _clean_topic_list(user_topics.get('not_following'))

    following = _ordered_unique(base_following, user_following)
    not_following = _ordered_unique(base_not, user_not)

    user_not_keys = {_norm_topic(x) for x in user_not}
    following = [x for x in following if _norm_topic(x) not in user_not_keys]

    user_following_keys = {_norm_topic(x) for x in user_following}
    not_following = [x for x in not_following if _norm_topic(x) not in user_following_keys]

    not_keys = {_norm_topic(x) for x in not_following}
    following = [x for x in following if _norm_topic(x) not in not_keys]

    return following, not_following


def _profile_following_list(profile: dict) -> list[str]:
    if not isinstance(profile, dict):
        return []
    following = profile.get('following')
    if isinstance(following, list):
        return _clean_topic_list(following)
    return []


def _save_profile_with_following(profile: dict, following: list[str]) -> dict:
    if not isinstance(profile, dict):
        profile = {}
    profile['following'] = _clean_topic_list(following)
    profile.pop('WATCH_ITEMS', None)
    save_user_profile(profile)
    return profile


def _clear_focus_topics_from_user_topics() -> None:
    """Remove all non-base (focus-derived) topics from user_topics.json.

    Called during pipeline_reset so stale focus topics don't survive a data wipe.
    """
    base_path = _paths().topics_file()
    base = {}
    try:
        if base_path.exists():
            loaded = json_io.read_json(base_path)
            if isinstance(loaded, dict):
                base = loaded
    except Exception:
        base = {}

    base_keys = {_norm_topic(t) for t in _clean_topic_list(base.get('following'))}

    user = _ensure_user_topics()
    uf = _clean_topic_list(user.get('following'))
    # Keep only topics that exist in base topics.json
    uf = [t for t in uf if _norm_topic(t) in base_keys]
    user['following'] = uf
    json_io.write_json(_paths().user_topics_file(), user)

    # Also update user_profile.json to reflect the trimmed following list.
    following, _ = _merge_topics(base, user)
    profile = load_user_profile()
    _save_profile_with_following(profile, following)


def load_user_profile():
    cfg = None
    try:
        cfg = load_config()
    except Exception:
        cfg = None

    active_dir = _resolve_data_folder_path(cfg)
    profile_path = active_dir / 'user_profile.json'

    # Dataset-scoped profile only. Do NOT fall back to repo-root user_profile.json,
    # otherwise datasets will share cached/preserved fields.
    if profile_path.exists():
        with open(profile_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    return {}


def save_user_profile(profile):
    cfg = None
    try:
        cfg = load_config()
    except Exception:
        cfg = None

    active_dir = _resolve_data_folder_path(cfg)
    profile_path = active_dir / 'user_profile.json'
    profile_path.parent.mkdir(parents=True, exist_ok=True)

    with open(profile_path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)
        f.write('\n')


def _run_python_script(args: list[str], *, timeout_sec: int = 180) -> dict:
    """Run a repo-local python script and return {code, stdout, stderr}."""
    try:
        cp = subprocess.run(
            [sys.executable, *args],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        return {
            'code': int(cp.returncode),
            'stdout': cp.stdout or '',
            'stderr': cp.stderr or '',
        }
    except subprocess.TimeoutExpired as e:
        return {
            'code': 124,
            'stdout': (getattr(e, 'stdout', '') or ''),
            'stderr': f'Timeout after {timeout_sec}s',
        }
    except Exception as e:
        return {
            'code': 1,
            'stdout': '',
            'stderr': str(e),
        }


@app.route('/api/user_profile', methods=['GET'])
def get_user_profile():
    """Return the full user_profile.json payload (best-effort)."""
    try:
        cfg = load_config()
        active_dir = _resolve_data_folder_path(cfg)
        profile_path = active_dir / 'user_profile.json'
        if not profile_path.exists():
            return jsonify({
                'error': 'Dataset user_profile.json not found',
                'profile_path': str(profile_path),
                'hint': 'Create it in the active dataset or use Refetch Profile.',
            }), 404
        profile = load_user_profile()
        if not isinstance(profile, dict):
            profile = {}
        return jsonify({'profile': profile, 'profile_path': str(profile_path)})
    except Exception as e:
        return jsonify({'error': f'Failed to load user profile: {str(e)}'}), 500


@app.route('/api/user_profile', methods=['POST'])
def post_user_profile():
    """Replace user_profile.json with the provided JSON object."""
    data = request.json
    if data is None:
        return jsonify({'error': 'Missing JSON body'}), 400

    # Accept either { profile: {...} } or a raw object.
    profile = data.get('profile') if isinstance(data, dict) and 'profile' in data else data
    if not isinstance(profile, dict):
        return jsonify({'error': 'Profile must be a JSON object'}), 400

    try:
        # Hard deprecation: identity/active_projects are no longer persisted.
        profile.pop('identity', None)
        profile.pop('active_projects', None)

        # Enforce new shape: following only
        incoming_following = _profile_following_list(profile)
        profile = _save_profile_with_following(profile, incoming_following)
        return jsonify({'status': 'success', 'profile': profile, 'following': incoming_following})
    except Exception as e:
        return jsonify({'error': f'Failed to save user profile: {str(e)}'}), 500


@app.route('/api/refetch_user_profile', methods=['POST'])
def refetch_user_profile():
    """Refetch user profile from SubstrateDataExtraction.

    NOTE: We no longer merge user_profile_ext.json into user_profile.json.
    Recent focus is the supported source of project context.
    """
    try:
        cfg = load_config()
        active_dir = _resolve_data_folder_path(cfg)
        target_profile_path = active_dir / 'user_profile.json'

        # 1) Refetch baseline profile directly into the dataset-scoped user_profile.json,
        # seeded from the existing dataset profile so it preserves dataset-specific edits.
        base_dir = Path(_repo_root_dir())
        seed_tmp_path: Path | None = None
        seed_path = target_profile_path
        if not seed_path.exists():
            try:
                ts = str(int(time.time() * 1000))
                seed_tmp_path = base_dir / '_tmp' / 'profile_seed' / f'user_profile_seed_{ts}.json'
                seed_tmp_path.parent.mkdir(parents=True, exist_ok=True)
                with seed_tmp_path.open('w', encoding='utf-8') as f:
                    json.dump({}, f, indent=2, ensure_ascii=False)
                    f.write('\n')
                seed_path = seed_tmp_path
            except Exception:
                seed_tmp_path = None
                seed_path = target_profile_path

        fetch_args = ['pipeline/fetch_user_profile.py', '--force', '--output-profile', str(target_profile_path)]
        if seed_path and seed_path.exists():
            fetch_args.extend(['--user-profile', str(seed_path)])

        fetch = _run_python_script(fetch_args, timeout_sec=240)
        if fetch.get('code') not in (0,):
            return jsonify({
                'error': 'Failed to refetch user profile',
                'fetch': fetch,
            }), 500

        # Cleanup seed temp (best-effort).
        if seed_tmp_path:
            try:
                if seed_tmp_path.exists():
                    seed_tmp_path.unlink()
            except Exception:
                pass

        # fetch_user_profile.py wrote directly to target_profile_path.

        # 2) Profile-ext merge disabled; ensure identity cannot linger via seeded refetch.
        ext_profile_path = _resolve_user_profile_ext_path(cfg)
        ext_profile_present = bool(ext_profile_path)
        ext_profile_cleared_keys: list[str] = []

        profile_after_fetch = load_user_profile()
        if not isinstance(profile_after_fetch, dict):
            profile_after_fetch = {}
        changed = False
        if 'identity' in profile_after_fetch:
            profile_after_fetch.pop('identity', None)
            ext_profile_cleared_keys.append('identity')
            changed = True
        if 'active_projects' in profile_after_fetch:
            profile_after_fetch.pop('active_projects', None)
            ext_profile_cleared_keys.append('active_projects')
            changed = True
        if changed:
            save_user_profile(profile_after_fetch)

        merge = {
            'code': 0,
            'stdout': '[INFO] Profile-ext merge disabled; identity stripped from profile if present.',
            'stderr': '',
        }

        profile = load_user_profile()
        if not isinstance(profile, dict):
            profile = {}

        return jsonify({
            'status': 'ok',
            'profile': profile,
            'fetch': fetch,
            'merge': merge,
            'ext_profile_present': ext_profile_present,
            'ext_profile_cleared_keys': ext_profile_cleared_keys,
            'ext_profile_path': str(ext_profile_path) if ext_profile_path else None,
            'profile_path': str(target_profile_path),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Tracks the currently running focus-analysis subprocess so it can be cancelled.
_FOCUS_ANALYSIS_PROC: subprocess.Popen | None = None
_FOCUS_ANALYSIS_LOCK = threading.Lock()


def _focus_analysis_proc_is_running() -> bool:
    with _FOCUS_ANALYSIS_LOCK:
        proc = _FOCUS_ANALYSIS_PROC
        return proc is not None and proc.poll() is None


def _recent_focus_output_path() -> str:
    """Dataset-aware recent_focus.json path."""
    try:
        cfg = load_config()
    except Exception:
        cfg = None
    try:
        folder = _resolve_data_folder_path(cfg)
    except Exception:
        folder = BASE_DIR / 'incremental_data'
    return str((folder / 'output' / 'recent_focus.json').resolve())


@app.route('/api/analyze_recent_focus', methods=['POST'])
def analyze_recent_focus():
    """Run analyze_recent_focus.py in a tracked subprocess so it can be cancelled."""
    global _FOCUS_ANALYSIS_PROC
    try:
        data = request.json if isinstance(request.json, dict) else {}
        days = data.get('days', 7)
        try:
            days = int(days)
        except Exception:
            days = 7
        days = max(0, min(60, days))

        output_path = _recent_focus_output_path()
        progress_path = output_path + '.progress.json'
        no_fetch = data.get('no_fetch', False)

        try:
            if os.path.exists(progress_path):
                os.remove(progress_path)
        except Exception:
            pass

        cmd = [
            sys.executable,
            str(BASE_DIR / 'pipeline' / 'analyze_recent_focus.py'),
            '--days', str(days),
            '--meeting-attendance', 'strict',
            '--output', output_path,
            '--openai-timeout', '90',
        ]
        if no_fetch:
            cmd.append('--no-fetch')

        with _FOCUS_ANALYSIS_LOCK:
            # Kill any prior run still going.
            if _FOCUS_ANALYSIS_PROC is not None and _FOCUS_ANALYSIS_PROC.poll() is None:
                try:
                    _FOCUS_ANALYSIS_PROC.kill()
                    _FOCUS_ANALYSIS_PROC.wait(timeout=5)
                except Exception:
                    pass
            proc = subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            _FOCUS_ANALYSIS_PROC = proc

        TIMEOUT = 480
        try:
            stdout, stderr = proc.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            return jsonify({'error': f'Focus analysis timed out after {TIMEOUT}s'}), 500

        with _FOCUS_ANALYSIS_LOCK:
            if _FOCUS_ANALYSIS_PROC is proc:
                _FOCUS_ANALYSIS_PROC = None

        if proc.returncode == -1 or proc.returncode is None:
            return jsonify({'error': 'Focus analysis was cancelled'}), 499

        if proc.returncode not in (0,):
            return jsonify({'error': 'Failed to analyze recent focus',
                            'run': {'code': proc.returncode, 'stdout': stdout, 'stderr': stderr}}), 500

        report = {}
        try:
            if os.path.exists(output_path):
                with open(output_path, 'r', encoding='utf-8') as f:
                    report = json.load(f)
        except Exception as e:
            return jsonify({'error': f'Focus analysis succeeded but failed to read report: {e}'}), 500

        return jsonify({'status': 'ok', 'report': report, 'report_path': output_path})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analyze_recent_focus/cancel', methods=['POST'])
def cancel_recent_focus():
    """Kill a running focus analysis subprocess."""
    global _FOCUS_ANALYSIS_PROC
    with _FOCUS_ANALYSIS_LOCK:
        proc = _FOCUS_ANALYSIS_PROC
        if proc is None or proc.poll() is not None:
            return jsonify({'status': 'not_running'})
        try:
            if os.name == 'nt':
                subprocess.call(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass
        _FOCUS_ANALYSIS_PROC = None
    return jsonify({'status': 'cancelled'})


@app.route('/api/focus_analysis_progress', methods=['GET'])
def focus_analysis_progress():
    """Return the current progress of a running focus analysis job.

    The sidecar file is written by analyze_recent_focus.py at
    ``{output_path}.progress.json``.
    """
    try:
        try:
            cfg = load_config()
        except Exception:
            cfg = None
        try:
            folder = _resolve_data_folder_path(cfg)
        except Exception:
            folder = (BASE_DIR / 'incremental_data')

        output_path = (folder / 'output' / 'recent_focus.json').resolve()
        progress_path = str(output_path) + '.progress.json'

        if not os.path.exists(progress_path):
            return jsonify({'status': 'idle', 'step': 0, 'total': 0, 'label': '', 'percent': 0})

        with open(progress_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        proc_running = _focus_analysis_proc_is_running()
        percent = data.get('percent', 0)
        if proc_running:
            status = 'running'
        elif percent >= 100:
            status = 'done'
        else:
            status = 'idle'
            data = {'step': 0, 'total': 0, 'label': '', 'percent': 0}

        return jsonify({
            'status': status,
            'step': data.get('step', 0),
            'total': data.get('total', 0),
            'label': data.get('label', ''),
            'percent': data.get('percent', 0),
        })
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/recent_focus', methods=['GET'])
def get_recent_focus():
    """Return cached recent_focus.json if available.

    This is used by the Settings UI to display the last focus report on open
    without triggering a new analysis run.
    """
    try:
        try:
            cfg = load_config()
        except Exception:
            cfg = None
        try:
            folder = _resolve_data_folder_path(cfg)
        except Exception:
            folder = (BASE_DIR / 'incremental_data')

        output_path = (folder / 'output' / 'recent_focus.json').resolve()
        if not os.path.exists(str(output_path)):
            return jsonify({'status': 'missing', 'report': None, 'report_path': str(output_path)})

        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
        except Exception as e:
            return jsonify({'error': f'Failed to read recent focus report: {e}', 'report_path': str(output_path)}), 500

        return jsonify({'status': 'ok', 'report': report, 'report_path': str(output_path)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent_focus/clear', methods=['POST'])
def clear_recent_focus():
    """Delete the recent_focus.json file entirely."""
    try:
        try:
            cfg = load_config()
        except Exception:
            cfg = None
        try:
            folder = _resolve_data_folder_path(cfg)
        except Exception:
            folder = (BASE_DIR / 'incremental_data')
        output_path = (folder / 'output' / 'recent_focus.json').resolve()
        if output_path.exists():
            output_path.unlink()
        # Also remove the progress sidecar if present
        progress_path = Path(str(output_path) + '.progress.json')
        if progress_path.exists():
            progress_path.unlink()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent_focus/delete', methods=['POST'])
def delete_recent_focus_topic():
    """Persist removal of a focus topic by updating recent_focus.json on disk."""
    try:
        data = request.json if isinstance(request.json, dict) else {}

        index = data.get('index', None)
        try:
            index = int(index)
        except Exception:
            index = None

        name = data.get('name', None)
        name = str(name or '').strip() if isinstance(name, (str, int, float)) else ''

        try:
            cfg = load_config()
        except Exception:
            cfg = None
        try:
            folder = _resolve_data_folder_path(cfg)
        except Exception:
            folder = (BASE_DIR / 'incremental_data')

        output_path = (folder / 'output' / 'recent_focus.json').resolve()
        report = _load_json_path(output_path)
        if not isinstance(report, dict):
            return jsonify({'status': 'missing', 'report': None, 'report_path': str(output_path)}), 404

        focus = report.get('focus')
        if not isinstance(focus, dict):
            return jsonify({'error': 'Invalid recent focus report: missing focus object', 'report_path': str(output_path)}), 400

        topics = focus.get('topics')
        if not isinstance(topics, list):
            topics = []

        removed = None
        if isinstance(index, int) and 0 <= index < len(topics):
            removed = topics.pop(index)
        elif name:
            matches: list[int] = []
            for i, t in enumerate(topics):
                if not isinstance(t, dict):
                    continue
                n = t.get('name')
                if not isinstance(n, str) or not n.strip():
                    n = t.get('topic')
                if str(n or '').strip() == name:
                    matches.append(i)
            if len(matches) == 1:
                removed = topics.pop(matches[0])
            elif len(matches) > 1:
                return jsonify({'error': 'Multiple topics match name; provide index', 'matches': matches, 'report_path': str(output_path)}), 409

        if removed is None:
            return jsonify({'error': 'Topic not found', 'report_path': str(output_path)}), 404

        focus['topics'] = topics
        report['focus'] = focus
        _write_json_path(output_path, report)

        return jsonify({'status': 'ok', 'report': report, 'report_path': str(output_path), 'removed': removed})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _as_list(value):
    return value if isinstance(value, list) else []


def _normalize_event_type_keys(values) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        s = str(v or '').strip().lower()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _seed_disabled_event_types_by_list_from_config(ops: dict) -> bool:
    """Merge config-level default disabled event types into per-list disabled_event_types_by_list.

    Uses config key `disabled_event_type` (list) and applies to:
      - active|assignee
      - active|collaborator
      - active|observer

    Does not overwrite existing entries; only unions defaults in.
    Returns True if ops was modified.
    """
    try:
        cfg = load_config()
    except Exception:
        cfg = {}

    defaults = None
    if isinstance(cfg, dict):
        defaults = cfg.get('disabled_event_type')
        # Back-compat / typo-tolerance
        if defaults is None:
            defaults = cfg.get('disabled_event_types')

    default_keys = _normalize_event_type_keys(defaults)
    if not default_keys:
        return False

    det = ops.get('disabled_event_types_by_list')
    if not isinstance(det, dict):
        det = {}

    changed = False
    for list_key in ('active|assignee', 'active|collaborator', 'active|observer'):
        existing = det.get(list_key)
        if not isinstance(existing, list):
            existing = []
        normalized_existing = [str(x or '').strip().lower() for x in existing if str(x or '').strip()]
        existing_set = set(normalized_existing)

        merged = list(normalized_existing)
        for k in default_keys:
            if k not in existing_set:
                merged.append(k)
                existing_set.add(k)
                changed = True

        det[list_key] = merged

    if changed:
        ops['disabled_event_types_by_list'] = det
    return changed

def load_user_ops():
    user_op_path = _paths().user_operation_file()
    had_existing_file = user_op_path.exists()
    if user_op_path.exists():
        try:
            with open(user_op_path, 'r', encoding='utf-8') as f:
                ops = json.load(f)
            if not isinstance(ops, dict):
                ops = {}
        except Exception:
            ops = {}
    else:
        ops = {}

    # Back-compat + ensure expected keys exist
    ops.setdefault('completed', [])
    ops.setdefault('dismissed', [])
    ops.setdefault('promoted', [])
    # Subset lists: items whose state was applied by AI remapping
    ops.setdefault('completed_ai', [])
    ops.setdefault('dismissed_ai', [])

    # UI preferences persisted as user operations
    # Map of listKey ("active|assignee", "active|observer", "done|", etc.) -> [eventTypeKey]
    # eventTypeKey is normalized (lowercased, trimmed) and represents an excluded Outlook event_type.
    ops.setdefault('disabled_event_types_by_list', {})

    # UI preferences: pinned cards (stable card ids: "outlook|<key>" / "teams|<key>")
    ops.setdefault('pinned_cards', [])

    # Normalize to lists
    for k in ('completed', 'dismissed', 'promoted', 'completed_ai', 'dismissed_ai', 'pinned_cards'):
        if not isinstance(ops.get(k), list):
            ops[k] = []

    # Normalize disabled_event_types_by_list
    det = ops.get('disabled_event_types_by_list')
    if not isinstance(det, dict):
        det = {}
    norm_det = {}
    for lk, v in det.items():
        if not lk:
            continue
        if isinstance(v, list):
            norm_det[str(lk)] = [str(x) for x in v if x]
        else:
            norm_det[str(lk)] = []
    ops['disabled_event_types_by_list'] = norm_det

    # Seed defaults from pipeline config ONLY when creating user_operation.json.
    # Once the file exists, do not re-apply defaults (user edits should win).
    if not had_existing_file:
        try:
            _seed_disabled_event_types_by_list_from_config(ops)
        except Exception:
            pass
        try:
            save_user_ops(ops)
        except Exception:
            pass

    return ops

def save_user_ops(ops):
    user_op_path = _paths().user_operation_file()
    user_op_path.parent.mkdir(parents=True, exist_ok=True)
    with open(user_op_path, 'w', encoding='utf-8') as f:
        json.dump(ops, f, indent=2, ensure_ascii=False)


def _annotate_actions_with_ai_ops(cards, ops):
    """Mark AI-derived completed/dismissed actions inside the briefing payload.

    This mutates the in-memory cards list only (API response), not the file on disk.
    """
    if not isinstance(cards, list) or not isinstance(ops, dict):
        return

    completed_ai = set(str(x) for x in _as_list(ops.get('completed_ai')) if x)
    dismissed_ai = set(str(x) for x in _as_list(ops.get('dismissed_ai')) if x)

    def mark_item(it: dict):
        ui = it.get('_ui_id')
        if not ui:
            return
        ui = str(ui)
        if ui in completed_ai:
            it['user_op'] = {
                'status': 'completed',
                'source': 'ai',
                'label': 'AI-complete',
            }
        elif ui in dismissed_ai:
            it['user_op'] = {
                'status': 'dismissed',
                'source': 'ai',
                'label': 'AI-dismiss',
            }

    for card in cards:
        if not isinstance(card, dict):
            continue
        ctype = card.get('type')
        data = card.get('data')
        if not isinstance(data, dict):
            continue

        if ctype == 'Outlook':
            for bucket in ('todos', 'recommendations'):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict):
                        mark_item(it)
        elif ctype == 'Teams':
            for bucket in ('linked_items', 'unlinked_items'):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict):
                        mark_item(it)


def _load_persisted_ops_store():
    store_path = _paths().user_ops_store_file()
    if store_path.exists():
        try:
            with open(store_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get('ops_by_fingerprint'), dict):
                return data
        except Exception:
            pass
    return {
        'version': 1,
        'ops_by_fingerprint': {}
    }


def _save_persisted_ops_store(store):
    store_path = _paths().user_ops_store_file()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    with open(store_path, 'w', encoding='utf-8') as f:
        json.dump(store, f, indent=2, ensure_ascii=False)


def _utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


def _has_any_user_ops(ops: dict) -> bool:
    if not isinstance(ops, dict):
        return False
    for k in ('completed', 'dismissed', 'promoted', 'completed_ai', 'dismissed_ai'):
        v = ops.get(k)
        if isinstance(v, list) and len(v) > 0:
            return True
    return False


def _norm_text(value):
    if value is None:
        return ''
    s = str(value)
    s = ' '.join(s.split())
    return s.strip().lower()


def _get_item_text(item: dict) -> str:
    if not isinstance(item, dict):
        return ''
    od = item.get('original_data') if isinstance(item.get('original_data'), dict) else {}
    return (item.get('task') or item.get('description') or od.get('task') or od.get('description') or '').strip()


def _fingerprint_payload(payload: dict) -> str:
    import hashlib
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _outlook_container_key(event: dict) -> str:
    name = _norm_text(event.get('event_name') or event.get('summary') or '')
    start = _norm_text(event.get('start_time') or '')
    end = _norm_text(event.get('end_time') or '')
    web = _norm_text(event.get('web_link') or event.get('weblink') or '')
    return '|'.join([name, start, end, web])


def _teams_container_key(conversation: dict) -> str:
    chat_id = _norm_text(conversation.get('chat_id') or '')
    conv_id = _norm_text(conversation.get('conversation_id') or '')
    chat_name = _norm_text(conversation.get('chat_name') or '')
    return '|'.join([chat_id, conv_id, chat_name])


def _find_action_context_by_ui_id(briefing_data: dict, ui_id: str):
    """Best-effort lookup of an action item by _ui_id inside briefing_data.json.

    Returns a tuple: (card_type, container_key, item_bucket, item_dict, extra_container_fields)
    """
    cards = briefing_data.get('cards')
    if not isinstance(cards, list):
        return None

    for card in cards:
        if not isinstance(card, dict):
            continue
        ctype = card.get('type')
        data = card.get('data')
        if not isinstance(data, dict):
            continue

        if ctype == 'Outlook':
            container_key = _outlook_container_key(data)
            for bucket in ('todos', 'recommendations'):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict) and str(it.get('_ui_id')) == str(ui_id):
                        extra = {
                            'event_id': data.get('event_id'),
                            'event_name': data.get('event_name')
                        }
                        return ('Outlook', container_key, bucket, it, extra)

        if ctype == 'Teams':
            conv = data.get('conversation') if isinstance(data.get('conversation'), dict) else {}
            container_key = _teams_container_key(conv)
            for bucket in ('linked_items', 'unlinked_items'):
                items = data.get(bucket)
                if not isinstance(items, list):
                    continue
                for it in items:
                    if isinstance(it, dict) and str(it.get('_ui_id')) == str(ui_id):
                        extra = {
                            'chat_id': conv.get('chat_id'),
                            'chat_name': conv.get('chat_name'),
                            'conversation_id': conv.get('conversation_id')
                        }
                        return ('Teams', container_key, bucket, it, extra)

    return None


def _compute_action_fingerprint(card_type: str, container_key: str, item: dict, item_bucket: str) -> str:
    od = item.get('original_data') if isinstance(item.get('original_data'), dict) else {}
    base = {
        'v': 1,
        'source': _norm_text(card_type),
        'bucket': _norm_text(item_bucket),
        'container': _norm_text(container_key),
        'text': _norm_text(_get_item_text(item)),
        'deadline': _norm_text((item.get('deadline') if isinstance(item, dict) else None) or od.get('deadline') or ''),
        'owner': _norm_text((item.get('owner') if isinstance(item, dict) else None) or od.get('owner') or ''),
    }
    quote = _norm_text((item.get('original_quote') if isinstance(item, dict) else None) or od.get('original_quote') or '')
    if quote:
        base['quote'] = quote
    return _fingerprint_payload(base)


def _compute_action_fingerprint_from_context(ctx: dict) -> str:
    """Stable-ish key for a persisted user op.

    This key is only used to dedupe/merge repeated ops locally; matching is AI-based.
    Prefer stable identifiers (Teams chat_id/conversation_id, Outlook web_link) over times.
    """
    if not isinstance(ctx, dict):
        ctx = {}
    source = _norm_text(ctx.get('card_type') or ctx.get('source') or '')
    bucket = _norm_text(ctx.get('bucket') or '')
    text = _norm_text(ctx.get('text') or '')
    deadline = _norm_text(ctx.get('deadline') or '')
    owner = _norm_text(ctx.get('owner') or '')

    if source == 'teams':
        container = _norm_text(ctx.get('chat_id') or ctx.get('conversation_id') or ctx.get('chat_name') or '')
    else:
        # Outlook
        container = _norm_text(ctx.get('web_link') or ctx.get('weblink') or ctx.get('event_name') or '')

    base = {
        'v': 2,
        'source': source,
        'bucket': bucket,
        'container': container,
        'text': text,
        'deadline': deadline,
        'owner': owner,
    }

    quote = _norm_text(ctx.get('original_quote') or '')
    if quote:
        base['quote'] = quote
    return _fingerprint_payload(base)

@app.route('/')
def index():
    # Serve React app (built output under static/app)
    return _serve_app_index()


@app.route('/<path:path>')
def serve_static(path):
    # Serve static files or fallback to index.html for client-side routing
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return _serve_app_index()

@app.route('/api/briefing_data')
def get_briefing_data():
    # Load data from configured path or default
    print(f"[INFO] Received call to load briefing data")
    data_path = _resolve_briefing_data_path()
    if not data_path or not os.path.exists(data_path):
        tried = _resolve_configured_briefing_data_candidates()
        return jsonify({
            'error': 'Briefing data not found',
            'message': 'No data file found. Tried: ' + '; '.join(tried),
            'cards': []
        }), 404
        
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Load user operations
    ops = load_user_ops()

    # Annotate payload: AI-derived ops should be visible in briefing data.
    cards_payload = data.get('cards', [])
    _annotate_actions_with_ai_ops(cards_payload, ops)

    # Card-level (task-level) vectorization + focus scores (best-effort, dependency-free).
    vectors_by_id = {}
    focus_model = _load_focus_model()
    card_feedback = _load_card_feedback()
    try:
        if isinstance(cards_payload, list):
            vectors_by_id = _ensure_task_vectors_for_cards(cards_payload)

        # Keep focus model in sync with latest vectors/feedback.
        focus_vec, counts = compute_focus_vector(vectors_by_id, card_feedback, dim=TASK_VECTOR_DIM)
        focus_model = {
            'v': 1,
            'vector_dim': TASK_VECTOR_DIM,
            'counts': counts,
            'last_updated': _utc_now_iso(),
        }
        # Store the actual vector only on disk.
        _save_focus_model({**focus_model, 'vector': focus_vec})

        focus_scores = _compute_focus_scores(cards_payload, vectors_by_id, focus_vec)
    except Exception:
        focus_scores = {}

    # Load user profile preferences
    profile = load_user_profile()
    following = _profile_following_list(profile)
    
    # Return combined data
    return jsonify({
        'cards': cards_payload,
        'history_map': data.get('history_map', {}),
        'teams_history_map': data.get('teams_history_map', {}),
        'chat_lookup': data.get('chat_lookup', {}),
        'outlook_threads_lookup': data.get('outlook_threads_lookup', {}),
        'user_ops': ops,
        'user_profile': {
            'following': following,
        },
        'card_feedback': card_feedback,
        'focus_scores': focus_scores,
    })


@app.route('/api/card_feedback', methods=['POST'])
def post_card_feedback():
    """DEPRECATED: legacy per-card like/dislike feedback endpoint.

    This endpoint is kept for compatibility with older clients and stored data.

    Expected JSON:
      - card_type: 'Outlook'|'Teams'
      - card_key: stable key (same idea as frontend getStableCardKey)
      - feedback: 'like'|'dislike'|'none'
    """
    data = request.json or {}
    card_type = str(data.get('card_type') or '').strip()
    card_key = _sanitize_id_part(data.get('card_key'))
    feedback = str(data.get('feedback') or '').strip().lower()

    if card_type.lower() not in ('outlook', 'teams'):
        return jsonify({'error': 'Invalid card_type'}), 400
    if not card_key:
        return jsonify({'error': 'Missing card_key'}), 400
    if feedback not in ('like', 'dislike', 'none'):
        return jsonify({'error': 'Invalid feedback'}), 400

    cid = _card_id(card_type, card_key)

    fb = _load_card_feedback()
    if feedback == 'none':
        fb.pop(cid, None)
    else:
        fb[cid] = feedback
    _save_card_feedback(fb)

    # Deprecated legacy flow: recompute the stored focus model from card feedback.
    vectors = _load_task_vectors()

    # Best-effort: if vector missing, try to compute from current briefing_data.
    if cid not in vectors:
        try:
            data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())
            if data_path and os.path.exists(data_path):
                with open(data_path, 'r', encoding='utf-8') as f:
                    briefing = json.load(f) or {}
                cards_payload = briefing.get('cards', [])
                if isinstance(cards_payload, list):
                    vectors = _ensure_task_vectors_for_cards(cards_payload)
        except Exception:
            pass

    focus_vec, counts = compute_focus_vector(vectors, fb, dim=TASK_VECTOR_DIM)
    focus_model = {
        'v': 1,
        'vector_dim': TASK_VECTOR_DIM,
        'counts': counts,
        'last_updated': _utc_now_iso(),
        'vector': focus_vec,
    }
    _save_focus_model(focus_model)

    # Deprecated legacy response: refresh focus_scores for older clients still expecting them.
    focus_scores = {}
    try:
        data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())
        if data_path and os.path.exists(data_path):
            with open(data_path, 'r', encoding='utf-8') as f:
                briefing = json.load(f) or {}
            cards_payload = briefing.get('cards', [])
            if isinstance(cards_payload, list):
                vectors = _ensure_task_vectors_for_cards(cards_payload)
                focus_scores = _compute_focus_scores(cards_payload, vectors, focus_vec)
    except Exception:
        focus_scores = {}

    return jsonify({
        'status': 'ok',
        'card_feedback': fb,
        'focus_model': {k: v for k, v in focus_model.items() if k != 'vector'},
        'focus_scores': focus_scores,
    })


@app.route('/api/topics_state', methods=['GET'])
def get_topics_state():
    """Return merged topics state (base topics + user_topics override)."""
    try:
        base_path = _paths().topics_file()
        base = {}
        try:
            if base_path.exists():
                loaded = json_io.read_json(base_path)
                if isinstance(loaded, dict):
                    base = loaded
        except Exception:
            base = {}
        user = _ensure_user_topics()
        following, not_following = _merge_topics(base, user)
        return jsonify({
            'topics': base,
            'user_topics': user,
            'following': following,
            'not_following': not_following,
        })
    except Exception as e:
        return jsonify({'error': f'Failed to load topics: {str(e)}'}), 500


@app.route('/api/topics_state', methods=['POST'])
def post_topics_state():
    """Move a topic between following and not_following.

    Body: { topic: string, target: 'following'|'not_following' }
    Side effects:
    - Updates user_state/user_topics.json (user overrides)
    - Updates user_profile.json.following to the merged effective following list
    """
    data = request.json or {}
    topic = str(data.get('topic') or '').strip()
    target = str(data.get('target') or '').strip().lower()

    if not topic:
        return jsonify({'error': 'Missing topic'}), 400
    if target not in ('following', 'not_following'):
        return jsonify({'error': 'Invalid target'}), 400

    try:
        base_path = _paths().topics_file()
        base = {}
        try:
            if base_path.exists():
                loaded = json_io.read_json(base_path)
                if isinstance(loaded, dict):
                    base = loaded
        except Exception:
            base = {}
        user = _ensure_user_topics()

        uf = _clean_topic_list(user.get('following'))
        un = _clean_topic_list(user.get('not_following'))
        key = _norm_topic(topic)

        if target == 'following':
            if key not in {_norm_topic(x) for x in uf}:
                uf.append(topic)
            un = [x for x in un if _norm_topic(x) != key]
        else:
            if key not in {_norm_topic(x) for x in un}:
                un.append(topic)
            uf = [x for x in uf if _norm_topic(x) != key]

        user['following'] = _clean_topic_list(uf)
        user['not_following'] = _clean_topic_list(un)
        json_io.write_json(_paths().user_topics_file(), user)

        following, not_following = _merge_topics(base, user)

        profile = load_user_profile()
        profile = _save_profile_with_following(profile, following)

        return jsonify({
            'status': 'success',
            'topics': base,
            'user_topics': user,
            'following': following,
            'not_following': not_following,
            'profile': profile,
        })
    except Exception as e:
        return jsonify({'error': f'Failed to update topics: {str(e)}'}), 500


@app.route('/api/topics_state/replace_focus', methods=['POST'])
def replace_focus_topics():
    """Replace all focus-derived topics in user_topics.json.

    Body: { topics: string[] }

    This removes ALL non-base topics from user_topics.following and replaces
    them with the supplied list.  Prevents accumulation of stale focus topics
    across repeated analysis runs.
    """
    data = request.json or {}
    new_focus: list[str] = data.get('topics', [])
    if not isinstance(new_focus, list):
        return jsonify({'error': 'topics must be an array'}), 400
    new_focus = [str(t).strip() for t in new_focus if str(t).strip()]

    try:
        base_path = _paths().topics_file()
        base = {}
        try:
            if base_path.exists():
                loaded = json_io.read_json(base_path)
                if isinstance(loaded, dict):
                    base = loaded
        except Exception:
            base = {}

        base_keys = {_norm_topic(t) for t in _clean_topic_list(base.get('following'))}

        user = _ensure_user_topics()
        uf = _clean_topic_list(user.get('following'))
        un = _clean_topic_list(user.get('not_following'))

        # Keep only base-derived topics in user following; drop old focus topics
        uf_base = [t for t in uf if _norm_topic(t) in base_keys]
        # Add new focus topics (dedup against base keys already present)
        existing_keys = {_norm_topic(t) for t in uf_base}
        for t in new_focus:
            k = _norm_topic(t)
            if k not in existing_keys:
                uf_base.append(t)
                existing_keys.add(k)

        # Also remove new focus topics from not_following
        focus_keys = {_norm_topic(t) for t in new_focus}
        un = [t for t in un if _norm_topic(t) not in focus_keys]

        user['following'] = _clean_topic_list(uf_base)
        user['not_following'] = _clean_topic_list(un)
        json_io.write_json(_paths().user_topics_file(), user)

        following, not_following = _merge_topics(base, user)

        profile = load_user_profile()
        profile = _save_profile_with_following(profile, following)

        return jsonify({
            'status': 'success',
            'topics': base,
            'user_topics': user,
            'following': following,
            'not_following': not_following,
            'profile': profile,
        })
    except Exception as e:
        return jsonify({'error': f'Failed to replace focus topics: {str(e)}'}), 500


@app.route('/api/update_watch_item', methods=['POST'])
def update_watch_item():
    """Back-compat endpoint.

    like => move topic into following
    dislike => move topic into not_following
    Also updates user_state/user_topics.json so user overrides are preserved.
    """
    data = request.json or {}
    value = (data.get('value') or '').strip()
    action = (data.get('action') or '').strip().lower()

    if not value:
        return jsonify({'error': 'No value provided'}), 400
    if action not in ('like', 'dislike'):
        return jsonify({'error': 'Invalid action'}), 400

    target = 'following' if action == 'like' else 'not_following'

    # Reuse the same logic as /api/topics_state.
    try:
        base_path = _paths().topics_file()
        base = {}
        try:
            if base_path.exists():
                loaded = json_io.read_json(base_path)
                if isinstance(loaded, dict):
                    base = loaded
        except Exception:
            base = {}
        user = _ensure_user_topics()

        uf = _clean_topic_list(user.get('following'))
        un = _clean_topic_list(user.get('not_following'))
        key = _norm_topic(value)

        if target == 'following':
            if key not in {_norm_topic(x) for x in uf}:
                uf.append(value)
            un = [x for x in un if _norm_topic(x) != key]
        else:
            if key not in {_norm_topic(x) for x in un}:
                un.append(value)
            uf = [x for x in uf if _norm_topic(x) != key]

        user['following'] = _clean_topic_list(uf)
        user['not_following'] = _clean_topic_list(un)
        json_io.write_json(_paths().user_topics_file(), user)

        following, _ = _merge_topics(base, user)

        profile = load_user_profile()
        _save_profile_with_following(profile, following)

        return jsonify({'status': 'success', 'following': following})
    except Exception as e:
        return jsonify({'error': f'Failed to update watch item: {str(e)}'}), 500


@app.route('/api/version')
def get_version():
    data_path = _resolve_briefing_data_path()
    if not data_path:
        data_path = str(_paths().briefing_data_file())
    
    try:
        mtime = os.path.getmtime(data_path)
        return jsonify({'version': str(mtime)})
    except:
        return jsonify({'version': '0'})


@app.route('/api/file_preview', methods=['GET'])
def file_preview():
    """Read-only preview for files/directories under the repo root.

    Used by the Observation page to inspect pipeline artifacts.
    """
    raw = request.args.get('path', '')
    raw_max = request.args.get('max_bytes', '')
    try:
        target = _safe_resolve_repo_path(raw)

        if not target.exists():
            return jsonify({'error': 'Path not found', 'path': raw}), 404

        # Directory listing
        if target.is_dir():
            entries = []
            try:
                children = list(target.iterdir())
                children.sort(key=lambda p: (0 if p.is_dir() else 1, p.name.lower()))
                # Hard limit to keep the UI responsive.
                children = children[:400]
                root = Path(_repo_root_dir()).resolve()
                for p in children:
                    try:
                        rel = str(p.resolve().relative_to(root)).replace('\\', '/')
                    except Exception:
                        rel = str(p)
                    kind = 'dir' if p.is_dir() else 'file'
                    size = None
                    try:
                        if p.is_file():
                            size = int(p.stat().st_size)
                    except Exception:
                        size = None
                    entries.append({'name': p.name, 'path': rel, 'kind': kind, 'size': size})
            except Exception as e:
                return jsonify({'error': f'Failed to list directory: {e}', 'path': raw}), 500

            root = Path(_repo_root_dir()).resolve()
            rel_target = str(target.resolve().relative_to(root)).replace('\\', '/')
            return jsonify({'kind': 'dir', 'path': rel_target, 'entries': entries})

        # File preview
        if target.is_file():
            DEFAULT_MAX_BYTES = 600_000
            MAX_BYTES_CAP = 5_000_000
            max_bytes = DEFAULT_MAX_BYTES
            try:
                if raw_max is not None and str(raw_max).strip() != '':
                    req = int(str(raw_max).strip())
                    if req > 0:
                        max_bytes = min(req, MAX_BYTES_CAP)
            except Exception:
                max_bytes = DEFAULT_MAX_BYTES

            try:
                with target.open('rb') as f:
                    data = f.read(max_bytes + 1)
            except Exception as e:
                return jsonify({'error': f'Failed to read file: {e}', 'path': raw}), 500

            truncated = False
            if len(data) > max_bytes:
                data = data[:max_bytes]
                truncated = True

            is_text = _is_probably_text(data)
            text = data.decode('utf-8', errors='replace')

            root = Path(_repo_root_dir()).resolve()
            rel_target = str(target.resolve().relative_to(root)).replace('\\', '/')
            size = None
            try:
                size = int(target.stat().st_size)
            except Exception:
                size = None

            return jsonify({
                'kind': 'file',
                'path': rel_target,
                'size': size,
                'truncated': truncated,
                'is_text': bool(is_text),
                'content': text,
            })

        return jsonify({'error': 'Unsupported path type', 'path': raw}), 400
    except PermissionError as e:
        return jsonify({'error': str(e), 'path': raw}), 403
    except ValueError as e:
        return jsonify({'error': str(e), 'path': raw}), 400
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {e}', 'path': raw}), 500

@app.route('/api/pipeline_status')
def get_pipeline_status():
    # Determine whether the pipeline is actually running.
    # The status file can be stale across app restarts, so we treat it as
    # informational unless a live pipeline process exists.
    try:
        is_running = _pipeline_is_running()
    except Exception:
        is_running = False

    # If auto-start is enabled and the pipeline is not running, kick it off.
    # This supports the expectation: on restart, auto-start means pipeline starts.
    # But NEVER auto-start if the user explicitly stopped the pipeline.
    auto_start_enabled = False
    try:
        cfg = load_config()
        auto_start_enabled = _pipeline_auto_start_enabled(cfg)
    except Exception:
        auto_start_enabled = False
    if auto_start_enabled and not is_running and not _PIPELINE_USER_STOPPED:
        _kickoff_pipeline_autostart_background(once=False)

    status_path = _pipeline_status_path()
    if status_path.exists():
        try:
            with open(status_path, 'r') as f:
                status = json.load(f)

            # If the pipeline is not running, never claim sleeping/working.
            # This prevents confusing "Sleeping (Next in 0:00)" on app load.
            file_state = status.get('state', 'offline')
            if not is_running and file_state in ('working', 'sleeping'):
                state = 'offline'
                msg = 'Pipeline is offline'
                if auto_start_enabled and not _PIPELINE_USER_STOPPED:
                    state = 'working'
                    msg = 'Starting pipeline...'
                return jsonify({
                    'state': state,
                    'message': msg,
                    'next_run': '',
                    'status_path': str(status_path),
                    # Preserve last-run fields for Observation UI.
                    'last_updated': status.get('last_updated', ''),
                    'run_id': status.get('run_id', ''),
                    'current_step_id': '',
                    'steps': status.get('steps', []),
                })

            # If the pipeline finished a working cycle and is now sleeping,
            # the safety backup is no longer needed.
            try:
                curr_state = status.get('state', 'offline')
                with _PIPELINE_STATE_LOCK:
                    global _LAST_PIPELINE_STATE
                    prev_state = _LAST_PIPELINE_STATE
                    _LAST_PIPELINE_STATE = curr_state

                if prev_state == 'working' and curr_state == 'sleeping':
                    base_dir, _, backup_dirname = _incremental_backup_spec()
                    pipeline_state.delete_incremental_backup(
                        base_dir,
                        backup_dirname=backup_dirname,
                    )
            except Exception:
                pass

            # Preserve the status file even when offline so the Observation page
            # can continue to show the last run and allow artifact previews.

            return jsonify({
                'state': status.get('state', 'offline'),
                'message': status.get('message', ''),
                'next_run': status.get('next_run', ''),
                'status_path': str(status_path),
                # Extended (optional) fields used by the Observation page.
                'last_updated': status.get('last_updated', ''),
                'run_id': status.get('run_id', ''),
                'current_step_id': status.get('current_step_id', ''),
                'steps': status.get('steps', []),
            })
        except Exception as e:
            return jsonify({
                'state': 'offline',
                'message': f'Error reading status: {str(e)}'
            })
    else:
        snap = _load_latest_observation_snapshot() or {}
        snap_steps = snap.get('steps', []) if isinstance(snap, dict) else []
        snap_run_id = snap.get('run_id', '') if isinstance(snap, dict) else ''
        snap_last_updated = snap.get('last_updated', '') if isinstance(snap, dict) else ''
        state = 'offline'
        message = 'Pipeline status file not found (showing last completed run)'
        if auto_start_enabled and not is_running and not _PIPELINE_USER_STOPPED:
            state = 'working'
            message = 'Starting pipeline...'
        return jsonify({
            'state': state,
            'message': message,
            'next_run': '',
            'last_updated': snap_last_updated,
            'run_id': snap_run_id,
            'current_step_id': '',
            'steps': snap_steps if isinstance(snap_steps, list) else [],
        })


@app.route('/api/pipeline_start', methods=['POST'])
def pipeline_start():
    """Start the incremental pipeline (run_incremental_pipeline.py) as a background process."""
    global _PIPELINE_USER_STOPPED
    with _PIPELINE_LOCK:
        _PIPELINE_USER_STOPPED = False          # User explicitly started → allow auto-start logic again
        if _pipeline_is_running():
            return jsonify({'status': 'already_running', 'pid': _PIPELINE_PROCESS.pid}), 202
        try:
            # Server-managed safety: snapshot incremental_data before starting the pipeline.
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            pipeline_state.delete_stale_backup_if_coexists(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            pipeline_state.create_incremental_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            info = _start_pipeline_process()
            return jsonify({'status': 'started', **info})
        except Exception as e:
            return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/pipeline_stop', methods=['POST'])
def pipeline_stop():
    """Stop the incremental pipeline subprocess if it is running."""
    global _PIPELINE_USER_STOPPED
    with _PIPELINE_LOCK:
        _PIPELINE_USER_STOPPED = True           # Explicit user stop — suppress auto-restart
        info = {}
        try:
            if _pipeline_is_running():
                info = _stop_pipeline_process()
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            restored = pipeline_state.restore_incremental_from_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            return jsonify({'status': 'stopped', 'restored': restored, **info})
        except Exception as e:
            return jsonify({'status': 'error', 'error': str(e)}), 500
        finally:
            # Always clear any stale UI status when the user clicks Stop.
            try:
                p = _pipeline_status_path()
                if p.exists() and p.is_file():
                    p.unlink()
            except Exception:
                pass
            try:
                with _PIPELINE_STATE_LOCK:
                    global _LAST_PIPELINE_STATE
                    _LAST_PIPELINE_STATE = None
            except Exception:
                pass


def _clean_briefing_outputs() -> dict:
    """Delete generated briefing output files (best-effort)."""
    removed: list[str] = []
    errors: list[str] = []
    try:
        cfg = load_config()
        out_dir = _resolve_data_folder_path(cfg) / 'output'

        candidates: list[Path] = [out_dir / 'briefing_data.json']
        try:
            if out_dir.exists():
                candidates.extend(list(out_dir.glob('briefing_data_*.json')))
        except Exception:
            pass

        for p in candidates:
            try:
                if p.exists() and p.is_file():
                    p.unlink()
                    removed.append(str(p))
            except Exception as e:
                errors.append(f"{p}: {e}")
    except Exception as e:
        errors.append(str(e))

    return {'removed': removed, 'errors': errors}


def _find_user_profile_ext_candidates(inc_dir: Path) -> list[Path]:
    """Return existing user_profile_ext files inside an incremental data folder."""
    candidates = [
        inc_dir / 'user_profile_ext.json',
        inc_dir / 'output' / 'user_profile_ext.json',
        inc_dir / 'user_profile_ext_raw.json',
        inc_dir / 'output' / 'user_profile_ext_raw.json',
    ]
    return [p for p in candidates if p.exists() and p.is_file()]


def _find_dataset_user_profile_candidates(inc_dir: Path) -> list[Path]:
    """Return dataset-scoped user_profile.json inside the active data folder."""
    candidates = [inc_dir / 'user_profile.json']
    return [p for p in candidates if p.exists() and p.is_file()]


def _preserve_files_under_base_dir(files: list[Path], *, base_dir: Path, tmp_root: Path) -> dict:
    """Copy files into tmp_root preserving relative paths from base_dir."""
    preserved: list[dict] = []
    errors: list[str] = []

    for src in files:
        try:
            rel = src.relative_to(base_dir)
        except Exception:
            rel = Path(src.name)

        dst = tmp_root / rel
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            preserved.append({'src': str(src), 'rel': str(rel), 'tmp': str(dst)})
        except Exception as e:
            errors.append(f"{src}: {e}")

    return {'preserved': preserved, 'errors': errors}


def _restore_files_under_base_dir(preserved: list[dict], *, base_dir: Path) -> dict:
    """Restore previously preserved files back under base_dir."""
    restored: list[dict] = []
    errors: list[str] = []

    for item in preserved:
        rel_s = str(item.get('rel') or '').strip()
        tmp_s = str(item.get('tmp') or '').strip()
        if not rel_s or not tmp_s:
            continue
        rel = Path(rel_s)
        tmp_path = Path(tmp_s)
        dst = base_dir / rel
        try:
            if tmp_path.exists() and tmp_path.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(tmp_path, dst)
                restored.append({'tmp': str(tmp_path), 'dst': str(dst)})
        except Exception as e:
            errors.append(f"{tmp_path} -> {dst}: {e}")

    return {'restored': restored, 'errors': errors}


def _prepare_keep_inputs_only(index: int = 1) -> dict:
        """Keep only the two raw input files and rewrite them into emails_<index>.json/teams_<index>.json.

        Source files:
            - incremental_data/outlook/all_emails.json
            - incremental_data/teams/all_teams_messages.json

        After this runs, incremental_data contains only:
            - outlook/emails_<index>.json
            - teams/teams_<index>.json
        """
        base_dir = Path(_repo_root_dir())
        cfg = load_config()
        inc_dir = _resolve_data_folder_path(cfg)
        outlook_dir = inc_dir / 'outlook'
        teams_dir = inc_dir / 'teams'

        def _pick_source_file(preferred: Path, fallback_glob: str, label: str) -> Path:
            if preferred.exists() and preferred.is_file():
                return preferred
            try:
                candidates = [p for p in preferred.parent.glob(fallback_glob) if p.is_file()]
            except Exception:
                candidates = []
            if candidates:
                # Prefer the most recently modified file.
                candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                return candidates[0]
            raise FileNotFoundError(
                f"Missing raw {label} input. Looked for {preferred} and {fallback_glob} in {preferred.parent}."
            )

        src_emails = _pick_source_file(outlook_dir / 'all_emails.json', 'emails_*.json', 'Outlook')
        src_teams = _pick_source_file(teams_dir / 'all_teams_messages.json', 'teams_*.json', 'Teams')

        # Copy to a temp location first so we can safely delete incremental_data.
        tmp_root = base_dir / '_tmp' / 'rerun_keep_inputs'
        tmp_root.mkdir(parents=True, exist_ok=True)
        ts = str(int(time.time() * 1000))

        # Preserve dataset-scoped profile + ext files that may live inside the data folder.
        ext_candidates = _find_user_profile_ext_candidates(inc_dir)
        profile_candidates = _find_dataset_user_profile_candidates(inc_dir)
        preserve_candidates = [*profile_candidates, *ext_candidates]
        ext_tmp_root = tmp_root / f'preserve_ext_{ts}'
        ext_preserve_info = _preserve_files_under_base_dir(preserve_candidates, base_dir=base_dir, tmp_root=ext_tmp_root)

        tmp_emails = tmp_root / f'all_emails_{ts}.json'
        tmp_teams = tmp_root / f'all_teams_messages_{ts}.json'
        shutil.copy2(src_emails, tmp_emails)
        shutil.copy2(src_teams, tmp_teams)

        # Remove everything else.
        shutil.rmtree(inc_dir, ignore_errors=True)
        (inc_dir / 'outlook').mkdir(parents=True, exist_ok=True)
        (inc_dir / 'teams').mkdir(parents=True, exist_ok=True)

        dst_emails = inc_dir / 'outlook' / f'emails_{index}.json'
        dst_teams = inc_dir / 'teams' / f'teams_{index}.json'
        shutil.copy2(tmp_emails, dst_emails)
        shutil.copy2(tmp_teams, dst_teams)

        # Restore ext profile files (best-effort).
        ext_restore_info = _restore_files_under_base_dir(
            list(ext_preserve_info.get('preserved') or []),
            base_dir=base_dir,
        )

        # Best-effort cleanup: these temp files are only needed during the rewrite.
        try:
            if tmp_emails.exists():
                tmp_emails.unlink()
        except Exception:
            pass
        try:
            if tmp_teams.exists():
                tmp_teams.unlink()
        except Exception:
            pass
        try:
            # If the directory is now empty, remove it too.
            if tmp_root.exists() and tmp_root.is_dir() and not any(tmp_root.iterdir()):
                tmp_root.rmdir()
        except Exception:
            pass

        try:
            if ext_tmp_root.exists() and ext_tmp_root.is_dir():
                shutil.rmtree(ext_tmp_root, ignore_errors=True)
        except Exception:
            pass

        return {
                'kept': [str(dst_emails), str(dst_teams)],
                'index': index,
            'preserved_user_profile_ext': {
                'found': [str(p) for p in ext_candidates],
                'preserve': ext_preserve_info,
                'restore': ext_restore_info,
            },
        }


@app.route('/api/rerun_extract', methods=['POST'])
def rerun_extract():
    """Rerun the incremental pipeline after scoring changes.

    mode:
      - clean: delete generated briefing outputs first
      - keep: keep existing data and rerun
      - events: keep existing Outlook events and re-extract tasks
    """
    data = request.json or {}
    mode = str(data.get('mode') or '').strip().lower()
    if mode not in ('clean', 'keep', 'events'):
        return jsonify({'error': 'Invalid mode; expected clean, keep, or events'}), 400

    sources = str(data.get('sources') or '').strip().lower()
    if mode == 'events' and sources and sources not in ('outlook', 'teams', 'both'):
        return jsonify({'error': 'Invalid sources; expected outlook, teams, or both'}), 400

    cleanup_info = None
    extra_args: list[str] | None = None
    cmd_override: list[str] | None = None
    try:
        if mode == 'clean':
            cleanup_info = _clean_briefing_outputs()
        elif mode == 'keep':
            # Keep only the two raw input files and rerun extraction from index 1.
            cleanup_info = _prepare_keep_inputs_only(index=1)
            extra_args = ['--skip-fetch']
        else:
            # Keep existing events and rerun task extraction/validation.
            cleanup_info = {
                'kept': 'master_outlook_events_*.json',
                'note': 'Re-extract tasks from existing events',
            }
            base_dir = _repo_root_dir()
            script_path = os.path.join(base_dir, 'pipeline', 'rerun_tasks_from_events.py')
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"rerun_tasks_from_events.py not found: {script_path}")
            cmd_override = [sys.executable, script_path]
            if sources in ('outlook', 'teams', 'both'):
                cmd_override.extend(['--sources', sources])
                cleanup_info['sources'] = sources
    except Exception as e:
        # Important: return JSON so the frontend can show the real error.
        return jsonify({
            'status': 'error',
            'mode': mode,
            'error': f'Failed to prepare rerun_extract inputs: {str(e)}',
        }), 500

    with _PIPELINE_LOCK:
        if _pipeline_is_running():
            return jsonify({
                'status': 'already_running',
                'pid': _PIPELINE_PROCESS.pid,
                'mode': mode,
                'cleanup': cleanup_info,
            }), 202
        try:
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            pipeline_state.delete_stale_backup_if_coexists(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            pipeline_state.create_incremental_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            info = _start_pipeline_process(extra_args=extra_args, cmd_override=cmd_override)
            return jsonify({
                'status': 'started',
                'mode': mode,
                'cleanup': cleanup_info,
                **info,
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'mode': mode,
                'cleanup': cleanup_info,
                'error': str(e),
            }), 500


@app.route('/api/pipeline_reset', methods=['POST'])
def pipeline_reset():
    """Reset the pipeline: stop process, delete the active dataset folder, then start pipeline again."""
    global _PIPELINE_USER_STOPPED
    with _PIPELINE_LOCK:
        _PIPELINE_USER_STOPPED = True           # Treat reset like stop — no auto-restart
        info = {}
        try:
            payload = request.json or {}
            reset_config = bool(payload.get('reset_config'))

            repo_root = Path(_repo_root_dir())
            cfg = load_config()
            inc_dir = _resolve_data_folder_path(cfg)
            inc_base_dir = inc_dir.parent
            inc_dirname = inc_dir.name

            # Stop process if running (do NOT restore from backup).
            if _pipeline_is_running():
                info = _stop_pipeline_process()

            # Clear any leftover backup/status.
            try:
                p = _pipeline_status_path()
                if p.exists() and p.is_file():
                    p.unlink()
            except Exception:
                pass
            b_base_dir, _, b_backup_dirname = _incremental_backup_spec()
            pipeline_state.delete_incremental_backup(
                b_base_dir,
                backup_dirname=b_backup_dirname,
            )

            # By default, preserve pipeline_config.user.json so dataset selection
            # (active_data_folder_path) stays consistent after reset.
            deleted_config_files: dict[str, object] = {}
            if reset_config:
                for filename in ('pipeline_config.json', 'pipeline_config.user.json'):
                    path = repo_root / 'config' / filename
                    if not path.exists():
                        deleted_config_files[filename] = False
                        continue
                    try:
                        path.unlink()
                        deleted_config_files[filename] = True
                    except Exception as e:
                        deleted_config_files[filename] = f"error: {e}"
            else:
                deleted_config_files = {
                    'pipeline_config.json': 'preserved',
                    'pipeline_config.user.json': 'preserved',
                }

            # Preserve dataset-scoped user_profile.json and any user_profile_ext files
            # before deleting the active dataset folder.
            preserved_profile = None
            restored_profile = None
            preserved_ext = None
            restored_ext = None
            ext_tmp_root = None
            try:
                ext_candidates = _find_user_profile_ext_candidates(inc_dir)
                profile_candidates = _find_dataset_user_profile_candidates(inc_dir)
                preserve_candidates = [*profile_candidates, *ext_candidates]
                if preserve_candidates:
                    ts = str(int(time.time() * 1000))
                    ext_tmp_root = repo_root / '_tmp' / 'pipeline_reset_preserve' / ts
                    preserved_all = _preserve_files_under_base_dir(preserve_candidates, base_dir=repo_root, tmp_root=ext_tmp_root)
                    # Keep backward-compatible keys while also exposing profile preservation.
                    preserved_ext = preserved_all
                    preserved_profile = preserved_all
            except Exception as e:
                preserved_ext = {'preserved': [], 'errors': [str(e)]}
                preserved_profile = {'preserved': [], 'errors': [str(e)]}

            # Delete incremental_data as requested.
            deleted = pipeline_state.delete_incremental_data_dir(
                inc_base_dir,
                incremental_dirname=inc_dirname,
            )

            # Clear focus-derived topics from user_topics.json.
            # recent_focus.json lives inside incremental_data (now deleted),
            # but the topics it produced were saved into user_state/user_topics.json
            # which survives the reset.  Remove non-base topics so they don't
            # persist as stale entries.
            try:
                _clear_focus_topics_from_user_topics()
            except Exception:
                pass  # best-effort

            # Clear AI error log so stale errors from the previous run don't
            # appear in the dashboard after a reset.
            try:
                ai_errors_path = BASE_DIR / 'user_state' / 'ai_errors.jsonl'
                if ai_errors_path.exists():
                    ai_errors_path.unlink()
            except Exception:
                pass  # best-effort

            # Restore preserved dataset profile/ext files (best-effort).
            try:
                if preserved_ext and preserved_ext.get('preserved'):
                    restored_ext = _restore_files_under_base_dir(list(preserved_ext.get('preserved') or []), base_dir=repo_root)
            except Exception as e:
                restored_ext = {'restored': [], 'errors': [str(e)]}
            try:
                if preserved_profile and preserved_profile.get('preserved'):
                    restored_profile = _restore_files_under_base_dir(list(preserved_profile.get('preserved') or []), base_dir=repo_root)
            except Exception as e:
                restored_profile = {'restored': [], 'errors': [str(e)]}
            try:
                if ext_tmp_root and ext_tmp_root.exists() and ext_tmp_root.is_dir():
                    shutil.rmtree(ext_tmp_root, ignore_errors=True)
            except Exception:
                pass

            # Do NOT auto-start the pipeline after reset — let the user
            # trigger it manually (or go through onboarding again).

            # After reset, automatically restore user operations from the persisted store.
            # This runs asynchronously and will appear as a step in Observation.
            try:
                _start_auto_restore_after_reset(inc_dir)
            except Exception:
                pass

            return jsonify({
                'status': 'reset_complete',
                'active_data_folder': str(inc_dir),
                'deleted_incremental_data': deleted,
                'deleted_config_files': deleted_config_files,
                'stopped': bool(info),
                'preserved_user_profile': {
                    'preserve': preserved_profile,
                    'restore': restored_profile,
                },
                'preserved_user_profile_ext': {
                    'preserve': preserved_ext,
                    'restore': restored_ext,
                },
                **info,
            })
        except Exception as e:
            return jsonify({'status': 'error', 'error': str(e)}), 500
        finally:
            try:
                with _PIPELINE_STATE_LOCK:
                    global _LAST_PIPELINE_STATE
                    _LAST_PIPELINE_STATE = None
            except Exception:
                pass


@app.route('/api/app_config')
def get_app_config():
    """Expose small, safe config needed by the frontend."""
    try:
        config = load_config()
        active_dir = _resolve_data_folder_path(config)
        output_dir = (active_dir / 'output').resolve()
        output_export_dir = (output_dir / 'exports').resolve()

        # Best-effort display name from the dataset-scoped user_profile.json.
        user_display_name = ''
        try:
            profile = load_user_profile()
            if isinstance(profile, dict):
                for key in (
                    'USER_NAME',
                    'user_name',
                    'name',
                    'display_name',
                    'USER_ALIAS',
                    'USER_EMAIL',
                    'user_email',
                ):
                    v = profile.get(key)
                    if isinstance(v, str) and v.strip():
                        user_display_name = v.strip()
                        break
        except Exception:
            user_display_name = ''

        onedrive_root = _resolve_onedrive_root()
        onedrive_dir = _onedrive_adaptive_card_dir() if onedrive_root is not None else None
        onedrive_target = _onedrive_top_tasks_path()

        onedrive_available = bool(onedrive_root is not None)
        onedrive_error = ''
        if not onedrive_available:
            onedrive_error = 'OneDrive folder not found (env var OneDrive/OneDriveCommercial/OneDriveConsumer missing)'

        return jsonify({
            'bug_report_recipient_email': config.get('bug_report_recipient_email', 'you@example.com'),
            'app_update_poll_interval_seconds': config.get('app_update_poll_interval_seconds', 30),
            'active_data_folder': str(active_dir),
            'output_dir': str(output_dir),
            'output_export_dir': str(output_export_dir),
            'onedrive_adaptive_card_path': str(onedrive_target) if onedrive_target else '',
            'onedrive_adaptive_card_dir': str(onedrive_dir) if onedrive_dir else '',
            'onedrive_adaptive_card_available': bool(onedrive_available),
            'onedrive_adaptive_card_error': str(onedrive_error),
            'user_display_name': user_display_name,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/app_update_status', methods=['GET'])
def get_app_update_status():
    """Return the latest git update availability info.

    This is updated by a background thread started at server boot.
    """
    with _APP_UPDATE_LOCK:
        return jsonify(dict(_APP_UPDATE_STATE))


@app.route('/api/update_app', methods=['POST'])
def update_app():
    """Start a background `git pull` to update the app."""
    with _APP_UPDATE_LOCK:
        if _APP_UPDATE_STATE.get('updating'):
            return jsonify({'status': 'already_updating'}), 202

    _start_git_pull_thread()
    return jsonify({'status': 'started'}), 202


# ── AI models list ─────────────────────────────────────────

def _fetch_azure_models() -> list[str]:
    """Return chat-capable model IDs from the Azure OpenAI resource. Empty list on failure."""
    try:
        from lib.ai_utils import AZURE_ENDPOINT, API_VERSION
        import httpx
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
        token = credential.get_token("https://cognitiveservices.azure.com/.default").token
        url = AZURE_ENDPOINT.rstrip('/') + f"/openai/models?api-version={API_VERSION}"
        resp = httpx.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return sorted(
            m["id"] for m in data.get("data", [])
            if m.get("id") and m.get("capabilities", {}).get("chat_completion")
        )
    except Exception as exc:
        print(f"[INFO] Azure model discovery failed: {exc}")
        return []


def _fetch_copilot_models() -> list[str]:
    """Return model IDs from the Copilot API. Empty list on failure."""
    try:
        from lib.copilot_auth import get_copilot_credentials, COPILOT_HEADERS
        import httpx
        creds = get_copilot_credentials(interactive=False)
        if not creds:
            return []
        url = creds["base_url"].rstrip('/') + "/models"
        resp = httpx.get(
            url,
            headers={**COPILOT_HEADERS, "Authorization": f"Bearer {creds['token']}"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return sorted(m["id"] for m in data.get("data", []) if m.get("id"))
    except Exception as exc:
        print(f"[INFO] Copilot model discovery failed: {exc}")
        return []


@app.route('/api/ai/models', methods=['GET'])
def ai_models():
    """Return available model options for each AI provider, detected at runtime."""
    provider = str(request.args.get('provider') or '').strip().lower()
    if provider == 'azure':
        return jsonify({
            'azure': _fetch_azure_models(),
            'copilot': [],
        })
    if provider == 'copilot':
        return jsonify({
            'azure': [],
            'copilot': _fetch_copilot_models(),
        })
    return jsonify({
        'azure': _fetch_azure_models(),
        'copilot': _fetch_copilot_models(),
    })


@app.route('/api/ai_errors', methods=['GET'])
def get_ai_errors():
    """Return recent AI call errors logged to user_state/ai_errors.jsonl.

    Query param ``since`` (ISO timestamp) can be used to return only newer entries.
    """
    MAX_LINES = 100
    log_path = BASE_DIR / 'user_state' / 'ai_errors.jsonl'
    if not log_path.exists():
        return jsonify({'errors': []})
    try:
        lines = log_path.read_text(encoding='utf-8').strip().splitlines()
        since = (request.args.get('since') or '').strip()
        entries = []
        for line in lines[-MAX_LINES:]:
            try:
                entry = json.loads(line)
                if since and entry.get('timestamp', '') <= since:
                    continue
                entries.append(entry)
            except Exception:
                pass
        return jsonify({'errors': entries})
    except Exception as e:
        return jsonify({'errors': [], 'error': str(e)})


@app.route('/api/ai/ping', methods=['POST'])
def ai_ping():
    """Send a tiny test prompt to the active AI backend and return the response.

    Accepts optional JSON body ``{"provider": "azure"|"copilot", "model": "..."}``
    to test a specific provider/model without modifying the saved config.
    """
    import time as _time
    body = request.get_json(silent=True) or {}
    provider = (body.get('provider') or '').strip().lower()
    model = (body.get('model') or '').strip()

    try:
        if provider == 'copilot':
            # Use the full Copilot client (includes required Editor-Version headers)
            from lib.ai_utils import _get_copilot_openai_client
            client = _get_copilot_openai_client()
            use_model = model or 'gpt-4o'
        elif provider == 'azure':
            from lib.ai_utils import _get_azure_openai_client, DEPLOYMENT_NAME
            client = _get_azure_openai_client()
            use_model = model or DEPLOYMENT_NAME
        else:
            # Fall back to active backend from config
            from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME
            client = get_azure_openai_client()
            use_model = model or DEPLOYMENT_NAME

        t0 = _time.time()
        resp = client.chat.completions.create(
            model=use_model,
            messages=[{'role': 'user', 'content': 'Reply with exactly: pong'}],
            max_completion_tokens=50,
            timeout=15,
        )
        elapsed = round(_time.time() - t0, 2)
        text = (resp.choices[0].message.content or '').strip() if resp.choices else ''
        return jsonify({'ok': True, 'reply': text, 'model': use_model, 'elapsed_s': elapsed})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/azure/status', methods=['GET'])
def azure_status():
    """Check whether Azure CLI is available and logged in.

    Returns 200 with:
      - logged_in: bool
      - account: dict (when logged in)
      - error: str (when not logged in / az missing / other failure)
    """
    try:
        import shutil

        az_exe = shutil.which('az')
        if not az_exe:
            return jsonify({'logged_in': False, 'error': 'Azure CLI (az) not found in PATH'})

        cp = subprocess.run(
            [az_exe, 'account', 'show', '--output', 'json'],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if cp.returncode == 0:
            try:
                account = json.loads(cp.stdout or '{}')
            except Exception:
                account = {}
            return jsonify({'logged_in': True, 'account': account})

        err = (cp.stderr or cp.stdout or '').strip()
        if not err:
            err = 'Azure CLI is not logged in (run: az login)'
        return jsonify({'logged_in': False, 'error': err})
    except subprocess.TimeoutExpired:
        return jsonify({'logged_in': False, 'error': 'Timed out checking Azure CLI login'})
    except Exception as e:
        return jsonify({'logged_in': False, 'error': str(e)})


@app.route('/api/azure/login', methods=['POST'])
def azure_login():
    """Trigger `az login` which opens the default browser for Azure authentication.

    Blocks until login completes or times out (3 minutes).
    Returns the same shape as /api/azure/status.
    """
    try:
        import shutil
        az_exe = shutil.which('az')
        if not az_exe:
            return jsonify({'logged_in': False, 'error': 'Azure CLI (az) not found in PATH'})

        cp = subprocess.run(
            [az_exe, 'login', '--output', 'json'],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if cp.returncode == 0:
            try:
                accounts = json.loads(cp.stdout or '[]')
                account = accounts[0] if isinstance(accounts, list) and accounts else {}
            except Exception:
                account = {}
            return jsonify({'logged_in': True, 'account': account})

        err = (cp.stderr or cp.stdout or '').strip()
        return jsonify({'logged_in': False, 'error': err or 'az login failed'})
    except subprocess.TimeoutExpired:
        return jsonify({'logged_in': False, 'error': 'Login timed out (3 min). Please try again.'})
    except Exception as e:
        return jsonify({'logged_in': False, 'error': str(e)})


# ── Copilot auth endpoints ────────────────────────────────


@app.route('/api/copilot/status', methods=['GET'])
def copilot_status():
    """Check whether GitHub Copilot credentials are available (cached tokens only).

    Uses cached token check to avoid blocking on slow network exchanges.
    """
    try:
        from lib.copilot_auth import _load_cached_copilot_token, _load_cached_github_token
        # Fast check: do we have a valid cached Copilot token?
        cached = _load_cached_copilot_token()
        if cached:
            return jsonify({'logged_in': True})
        # If we have a cached GitHub token, we can exchange on demand (but don't block here)
        gh = _load_cached_github_token()
        if gh:
            # Try a quick exchange; if it fails, report not logged in
            try:
                from lib.copilot_auth import _exchange_copilot_token
                _exchange_copilot_token(gh)
                return jsonify({'logged_in': True})
            except Exception as exc:
                return jsonify({'logged_in': False, 'error': f'Copilot token exchange failed: {exc}'})
        return jsonify({'logged_in': False})
    except Exception as e:
        return jsonify({'logged_in': False, 'error': str(e)})


@app.route('/api/copilot/login', methods=['POST'])
def copilot_login():
    """Start GitHub device-flow login for Copilot.

    Returns:
      - user_code: code for the user to enter on GitHub
      - verification_uri: URL to open in browser
      - device_code: opaque code for polling via /api/copilot/login_poll
      - interval: recommended poll interval (seconds)
    """
    try:
        import httpx as _httpx
        from lib.copilot_auth import COPILOT_CLIENT_ID, DEVICE_CODE_URL

        with _httpx.Client(timeout=15) as http:
            resp = http.post(
                DEVICE_CODE_URL,
                data={'client_id': COPILOT_CLIENT_ID, 'scope': 'read:user copilot'},
                headers={'Accept': 'application/json'},
            )
            resp.raise_for_status()
            device = resp.json()

        return jsonify({
            'user_code': device['user_code'],
            'verification_uri': device['verification_uri'],
            'device_code': device['device_code'],
            'interval': max(device.get('interval', 5), 2),
            'expires_in': device.get('expires_in', 900),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/copilot/login_poll', methods=['POST'])
def copilot_login_poll():
    """Poll GitHub for device-flow completion then exchange for a Copilot token.

    Expected JSON body: ``{ "device_code": "..." }``

    Returns ``{ "status": "pending" | "complete" | "slow_down" | "error", ... }``
    """
    try:
        payload = request.json or {}
        device_code = (payload.get('device_code') or '').strip()
        if not device_code:
            return jsonify({'status': 'error', 'error': 'Missing device_code'}), 400

        import httpx as _httpx
        from lib.copilot_auth import (
            COPILOT_CLIENT_ID,
            ACCESS_TOKEN_URL,
            _save_cached_github_token,
            _exchange_copilot_token,
        )

        with _httpx.Client(timeout=15) as http:
            resp = http.post(
                ACCESS_TOKEN_URL,
                data={
                    'client_id': COPILOT_CLIENT_ID,
                    'device_code': device_code,
                    'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                },
                headers={'Accept': 'application/json'},
            )
            resp.raise_for_status()
            result = resp.json()

        if 'access_token' in result:
            token = result['access_token']
            _save_cached_github_token(token)
            try:
                _exchange_copilot_token(token)
            except Exception as exc:
                return jsonify({'status': 'error', 'error': f'Copilot token exchange failed: {exc}'}), 500
            return jsonify({'status': 'complete', 'logged_in': True})

        error = result.get('error', 'unknown')
        if error == 'authorization_pending':
            return jsonify({'status': 'pending'})
        if error == 'slow_down':
            return jsonify({'status': 'slow_down'})
        if error in ('expired_token', 'access_denied'):
            return jsonify({'status': 'error', 'error': error})
        return jsonify({'status': 'error', 'error': f'Unexpected: {error}'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/copilot/login_wait', methods=['POST'])
def copilot_login_wait():
    """Long-poll: block until the device-flow completes or times out.

    Polls GitHub every ``interval`` seconds on the server side so the
    browser only needs a single fetch() call.

    Expected JSON: ``{ "device_code": "...", "interval": 5, "expires_in": 900 }``

    Returns ``{ "status": "complete" | "error", ... }``
    """
    import time as _time
    import httpx as _httpx
    from lib.copilot_auth import (
        COPILOT_CLIENT_ID,
        ACCESS_TOKEN_URL,
        _save_cached_github_token,
        _exchange_copilot_token,
    )

    payload = request.json or {}
    device_code = (payload.get('device_code') or '').strip()
    if not device_code:
        return jsonify({'status': 'error', 'error': 'Missing device_code'}), 400

    interval = max(int(payload.get('interval', 5)), 2)
    expires_in = min(int(payload.get('expires_in', 300)), 300)  # cap at 5 min
    deadline = _time.time() + expires_in

    while _time.time() < deadline:
        _time.sleep(interval)
        try:
            with _httpx.Client(timeout=15) as http:
                resp = http.post(
                    ACCESS_TOKEN_URL,
                    data={
                        'client_id': COPILOT_CLIENT_ID,
                        'device_code': device_code,
                        'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                    },
                    headers={'Accept': 'application/json'},
                )
                resp.raise_for_status()
                result = resp.json()
        except Exception as exc:
            return jsonify({'status': 'error', 'error': f'Network error: {exc}'}), 500

        if 'access_token' in result:
            token = result['access_token']
            _save_cached_github_token(token)
            try:
                _exchange_copilot_token(token)
            except Exception as exc:
                return jsonify({
                    'status': 'error',
                    'error': (
                        f'GitHub login succeeded but Copilot token exchange failed: {exc}. '
                        'Make sure your GitHub account has an active Copilot subscription.'
                    ),
                }), 500
            return jsonify({'status': 'complete', 'logged_in': True})

        error = result.get('error', 'unknown')
        if error == 'authorization_pending':
            continue
        if error == 'slow_down':
            interval = min(interval + 2, 15)
            continue
        if error in ('expired_token', 'access_denied'):
            return jsonify({'status': 'error', 'error': f'Device flow {error}'})
        return jsonify({'status': 'error', 'error': f'Unexpected GitHub error: {error}'})

    return jsonify({'status': 'error', 'error': 'Timed out waiting for authorization — please try again.'})


@app.route('/api/scoring_rubrics')
def get_scoring_rubrics():
    """Expose scoring rubric metadata (max points per factor) for UI display."""
    try:
        cfg = load_config()
        base_dir = os.path.dirname(os.path.abspath(__file__))

        def load_rubric(path_value: str):
            if not isinstance(path_value, str) or not path_value.strip():
                return None
            path = os.path.join(base_dir, path_value)
            if not os.path.exists(path):
                return None
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return None

        outlook = load_rubric(cfg.get('scoring_system_outlook_path', 'config/scoring_system.json'))
        teams = load_rubric(cfg.get('scoring_system_teams_path', 'config/scoring_system.json'))
        return jsonify({
            'outlook': outlook,
            'teams': teams
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _load_json_path(path: Path):
    try:
        if not path.exists():
            return None
        with path.open('r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception:
        return None


def _write_json_path(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
        f.write('\n')


def _resolve_data_folder_path(cfg: dict | None = None) -> Path:
    """Resolve the configured data folder path.

    This folder is expected to be the equivalent of the repo's `incremental_data/`.
    Accepts either absolute paths or paths relative to the repo root.
    """
    try:
        if cfg is None:
            cfg = load_config()
        chosen: str | None = None
        if isinstance(cfg, dict):
            active = cfg.get('active_data_folder_path')
            if isinstance(active, str) and active.strip():
                chosen = active

            if chosen is None:
                paths = cfg.get('data_folder_paths')
                if isinstance(paths, list):
                    for x in paths:
                        s = str(x or '').strip()
                        if s:
                            chosen = s
                            break

            if chosen is None:
                legacy = cfg.get('data_folder_path')
                if isinstance(legacy, str) and legacy.strip():
                    chosen = legacy

        if not chosen:
            chosen = 'incremental_data'

        p = Path(chosen)
        if not p.is_absolute():
            p = Path(_repo_root_dir()) / p
        return p
    except Exception:
        return Path(_repo_root_dir()) / 'incremental_data'


def _resolve_configured_briefing_data_candidates() -> list[str]:
    """Return candidate paths (best-first) for briefing_data.json."""
    cfg = load_config()

    # If server was started with `--data`, treat it as a hard override.
    try:
        if app.config.get('BRIEFING_DATA_PATH_SOURCE') == 'cli':
            configured = app.config.get('BRIEFING_DATA_PATH')
            if isinstance(configured, str) and configured.strip():
                return [configured]
    except Exception:
        pass

    candidates: list[str] = []

    # Primary: folder setting (data_folder_path)
    folder = _resolve_data_folder_path(cfg)
    candidates.append(str(folder / 'output' / 'briefing_data.json'))
    # Allow pointing directly at an output folder too.
    candidates.append(str(folder / 'briefing_data.json'))

    # Secondary: legacy file setting (briefing_data_path)
    bdp = cfg.get('briefing_data_path') if isinstance(cfg, dict) else None
    if isinstance(bdp, str) and bdp.strip():
        p = Path(bdp)
        if not p.is_absolute():
            p = Path(_repo_root_dir()) / p
        candidates.append(str(p))

    # Fallbacks
    candidates.append(str(_paths().briefing_data_file()))
    candidates.append('briefing_data.json')

    # De-duplicate while preserving order.
    seen = set()
    uniq: list[str] = []
    for c in candidates:
        if not c or c in seen:
            continue
        uniq.append(c)
        seen.add(c)
    return uniq


def _resolve_briefing_data_path() -> str | None:
    """Best-effort locate briefing_data.json (same logic as /api/briefing_data)."""
    try:
        candidates = _resolve_configured_briefing_data_candidates()
        for p in candidates:
            if not p or not isinstance(p, str):
                continue
            if os.path.exists(p):
                return p
        return None
    except Exception:
        return None


def _resolve_user_profile_ext_path(cfg: dict | None = None) -> Path | None:
    """Locate user_profile_ext.json based on the active data folder selection."""
    try:
        if cfg is None:
            cfg = load_config()
        folder = _resolve_data_folder_path(cfg)

        candidates = [
            folder / 'user_profile_ext.json',
            folder / 'output' / 'user_profile_ext.json',
            folder / 'user_profile_ext_raw.json',
            folder / 'output' / 'user_profile_ext_raw.json',
        ]
        for p in candidates:
            try:
                if p.exists() and p.is_file():
                    return p
            except Exception:
                continue
        return None
    except Exception:
        return None


def _normalize_priority_label(label: str) -> str:
    if label in ('High', 'Medium', 'Low'):
        return label
    s = str(label or '').strip().lower()
    if s == 'high':
        return 'High'
    if s == 'medium':
        return 'Medium'
    if s == 'low':
        return 'Low'
    # Unknown bucket; keep UI stable.
    return 'Low'


def _pick_priority_label(score: int, rubric: dict) -> str:
    pls = rubric.get('priority_levels') if isinstance(rubric, dict) else None
    if not isinstance(pls, list) or not pls:
        return 'Medium'

    rows = []
    for p in pls:
        if not isinstance(p, dict):
            continue
        label = p.get('label')
        ms = p.get('min_score')
        if not isinstance(label, str) or not label.strip():
            continue
        if not isinstance(ms, (int, float)):
            continue
        rows.append((label, int(ms)))

    if not rows:
        return 'Medium'

    # Pick highest threshold satisfied.
    rows.sort(key=lambda x: x[1], reverse=True)
    for label, min_score in rows:
        if score >= min_score:
            return _normalize_priority_label(label)

    # If no threshold met, fall back to the lowest label.
    rows.sort(key=lambda x: x[1])
    return _normalize_priority_label(rows[0][0])


def _recompute_item_priority(item: dict, rubric: dict) -> tuple[bool, bool, dict | None]:
    """Recompute priority fields for a single action item.

    Returns: (changed, had_breakdown)
    """
    if not isinstance(item, dict):
        return False, False, None

    original = item.get('original_data')
    if not isinstance(original, dict):
        original = None

    breakdown = item.get('scoring_breakdown')
    if not isinstance(breakdown, dict) and isinstance(original, dict):
        breakdown = original.get('scoring_breakdown')

    if not isinstance(breakdown, dict) or not breakdown:
        return False, False, None

    before_priority = item.get('priority')
    before_score = item.get('priority_score')
    before_score_max = item.get('priority_score_max')
    if isinstance(original, dict):
        if before_priority is None:
            before_priority = original.get('priority') or original.get('priority_level')
        if before_score is None:
            before_score = original.get('priority_score')
        if before_score_max is None:
            before_score_max = original.get('priority_score_max')

    # Factor max points map.
    factor_max: dict[str, int] = {}
    factors = rubric.get('factors') if isinstance(rubric, dict) else None
    if isinstance(factors, list):
        for f in factors:
            if not isinstance(f, dict):
                continue
            key = f.get('key')
            mp = f.get('max_points')
            if isinstance(key, str) and key.strip() and isinstance(mp, (int, float)):
                factor_max[key] = int(mp)

    def clamp(val: int, key: str) -> int:
        mp = factor_max.get(key)
        if isinstance(mp, int):
            if val < 0:
                return 0
            if val > mp:
                return mp
        return val

    total = 0
    for k, v in breakdown.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if not isinstance(v, (int, float)):
            continue
        total += clamp(int(v), k)

    total_max = rubric.get('total_max_points')
    total_max_int = int(total_max) if isinstance(total_max, (int, float)) else None

    new_priority = _pick_priority_label(int(total), rubric)

    changed = False
    for target in (item, original):
        if not isinstance(target, dict):
            continue

        if target.get('priority_score') != int(total):
            target['priority_score'] = int(total)
            changed = True

        if isinstance(total_max_int, int) and total_max_int > 0:
            if target.get('priority_score_max') != total_max_int:
                target['priority_score_max'] = total_max_int
                changed = True

        if target.get('priority') != new_priority:
            target['priority'] = new_priority
            changed = True

        # Some payloads use priority_level naming.
        if target.get('priority_level') is not None and target.get('priority_level') != new_priority:
            target['priority_level'] = new_priority
            changed = True

    if not changed:
        return False, True, None

    after_priority = item.get('priority')
    after_score = item.get('priority_score')
    after_score_max = item.get('priority_score_max')
    if isinstance(original, dict):
        # Prefer top-level values, but fall back to original_data if needed.
        if after_priority is None:
            after_priority = original.get('priority') or original.get('priority_level')
        if after_score is None:
            after_score = original.get('priority_score')
        if after_score_max is None:
            after_score_max = original.get('priority_score_max')

    ui_id = item.get('_ui_id') or (original.get('_ui_id') if isinstance(original, dict) else None)
    title = (
        item.get('title')
        or item.get('task')
        or item.get('action')
        or item.get('summary')
        or item.get('text')
        or item.get('name')
        or (original.get('title') if isinstance(original, dict) else None)
        or (original.get('task') if isinstance(original, dict) else None)
        or (original.get('action') if isinstance(original, dict) else None)
        or (original.get('summary') if isinstance(original, dict) else None)
        or (original.get('text') if isinstance(original, dict) else None)
        or (original.get('name') if isinstance(original, dict) else None)
    )

    change = {
        'ui_id': str(ui_id) if ui_id is not None else None,
        'title': str(title) if title is not None else None,
        'before': {
            'priority': before_priority,
            'priority_score': before_score,
            'priority_score_max': before_score_max,
        },
        'after': {
            'priority': after_priority,
            'priority_score': after_score,
            'priority_score_max': after_score_max,
        },
    }
    return True, True, change


def _recompute_priorities_in_cards(cards: list, rubric: dict) -> dict:
    updated = 0
    seen = 0
    missing_breakdown = 0
    changes: list[dict] = []

    def handle_item(obj: Any, *, source: str):
        nonlocal updated, seen, missing_breakdown
        if not isinstance(obj, dict):
            return
        seen += 1
        changed, had_breakdown, change = _recompute_item_priority(obj, rubric)
        if not had_breakdown:
            missing_breakdown += 1
        if changed:
            updated += 1
            if isinstance(change, dict):
                # Attach source (Outlook/Teams + list name)
                changes.append({**change, 'source': source})

    for card in cards:
        if not isinstance(card, dict):
            continue
        ctype = card.get('type')
        data = card.get('data')
        if not isinstance(data, dict):
            continue

        if ctype == 'Outlook':
            todos = data.get('todos')
            recs = data.get('recommendations')
            if isinstance(todos, list):
                for t in todos:
                    handle_item(t, source='Outlook/todos')
            if isinstance(recs, list):
                for r in recs:
                    handle_item(r, source='Outlook/recommendations')
        elif ctype == 'Teams':
            linked = data.get('linked_items')
            unlinked = data.get('unlinked_items')
            if isinstance(linked, list):
                for it in linked:
                    handle_item(it, source='Teams/linked_items')
            if isinstance(unlinked, list):
                for it in unlinked:
                    handle_item(it, source='Teams/unlinked_items')

            conv = data.get('conversation')
            if isinstance(conv, dict):
                conv_tasks = conv.get('tasks')
                if isinstance(conv_tasks, list):
                    for it in conv_tasks:
                        handle_item(it, source='Teams/conversation.tasks')

    return {
        'items_seen': seen,
        'items_updated': updated,
        'items_missing_breakdown': missing_breakdown,
        'changes': changes,
    }


def _ensure_scoring_system_outlook_versions(repo_root: Path):
    # Active/effective rubric is shared by Outlook + Teams.
    active_path = repo_root / 'config' / 'scoring_system.json'

    # Store versioned files in config/ (similar to pipeline_config.* pattern).
    default_path = repo_root / 'config' / 'scoring_system.default.json'
    user_path = repo_root / 'config' / 'scoring_system.user.json'

    # Ensure default exists; copy from active if needed.
    if not default_path.exists() and active_path.exists():
        try:
            default_path.write_text(active_path.read_text(encoding='utf-8-sig'), encoding='utf-8')
        except Exception:
            pass

    # Ensure active exists; copy from default if needed.
    if not active_path.exists() and default_path.exists():
        try:
            active_path.write_text(default_path.read_text(encoding='utf-8-sig'), encoding='utf-8')
        except Exception:
            pass

    return default_path, user_path, active_path


def _minimize_scoring_user_override(rubric: dict | None) -> dict:
    """Return a minimal user override rubric.

    The override should only include numeric/tunable fields. Descriptive text
    (label/description/examples/evidence/name) is always carried from default.
    """
    if not isinstance(rubric, dict):
        return {}

    out: dict = {}

    # Priority levels: override min_score by label.
    pls = rubric.get('priority_levels')
    if isinstance(pls, list):
        prios = []
        for p in pls:
            if not isinstance(p, dict):
                continue
            label = p.get('label')
            ms = p.get('min_score')
            if not isinstance(label, str) or not label.strip():
                continue
            if not isinstance(ms, (int, float)):
                continue
            prios.append({'label': label, 'min_score': int(ms)})
        if prios:
            out['priority_levels'] = prios

    # Factors: override max_points by key.
    factors = rubric.get('factors')
    if isinstance(factors, list):
        facs = []
        for f in factors:
            if not isinstance(f, dict):
                continue
            key = f.get('key')
            mp = f.get('max_points')
            if not isinstance(key, str) or not key.strip():
                continue
            if not isinstance(mp, (int, float)):
                continue
            facs.append({'key': key, 'max_points': int(mp)})
        if facs:
            out['factors'] = facs

    # Keep version if present (not required, but harmless).
    v = rubric.get('version')
    if isinstance(v, (int, float)):
        out['version'] = int(v)

    return out


def _merge_scoring_rubric(default_rubric: dict, user_rubric: dict | None) -> dict:
    """Best-effort merge for scoring_system rubric.

    - Top-level keys: user overrides default when present.
    - factors: merge by factor.key, user overrides individual fields.
    """
    if not isinstance(default_rubric, dict):
        default_rubric = {}
    if not isinstance(user_rubric, dict):
        user_rubric = {}

    # Only allow numeric/tunable overrides.
    merged = dict(default_rubric)

    # Priority levels: override min_score by label, keep default labels/order.
    def_pl = default_rubric.get('priority_levels') if isinstance(default_rubric.get('priority_levels'), list) else []
    usr_pl = user_rubric.get('priority_levels') if isinstance(user_rubric.get('priority_levels'), list) else []
    usr_pl_by_label = {}
    for p in usr_pl:
        if isinstance(p, dict) and isinstance(p.get('label'), str) and p.get('label').strip() and isinstance(p.get('min_score'), (int, float)):
            usr_pl_by_label[p['label']] = int(p['min_score'])

    merged_pl = []
    for p in def_pl:
        if not isinstance(p, dict) or not isinstance(p.get('label'), str):
            continue
        out_p = dict(p)
        label = out_p.get('label')
        if label in usr_pl_by_label:
            out_p['min_score'] = usr_pl_by_label[label]
        merged_pl.append(out_p)
    if merged_pl:
        merged['priority_levels'] = merged_pl

    def_list = default_rubric.get('factors') if isinstance(default_rubric.get('factors'), list) else []
    usr_list = user_rubric.get('factors') if isinstance(user_rubric.get('factors'), list) else []

    usr_by_key = {}
    for f in usr_list:
        if isinstance(f, dict) and isinstance(f.get('key'), str) and f.get('key').strip():
            usr_by_key[f['key']] = f

    merged_factors = []
    seen = set()
    for f in def_list:
        if not isinstance(f, dict):
            continue
        key = f.get('key')
        if not isinstance(key, str) or not key.strip():
            continue
        seen.add(key)
        override = usr_by_key.get(key)
        if isinstance(override, dict):
            out = dict(f)
            # Only accept max_points override; keep descriptive text from default.
            mp = override.get('max_points')
            if isinstance(mp, (int, float)):
                out['max_points'] = int(mp)
            merged_factors.append(out)
        else:
            merged_factors.append(f)

    # Ignore user-only factors (keep default rubric authoritative).

    merged['factors'] = merged_factors

    # Keep total_max_points consistent with factors if possible.
    try:
        total = 0
        for f in merged_factors:
            mp = f.get('max_points') if isinstance(f, dict) else None
            if isinstance(mp, (int, float)):
                total += int(mp)
        if total > 0:
            merged['total_max_points'] = total
    except Exception:
        pass

    return merged


@app.route('/api/scoring_system_outlook', methods=['GET'])
def get_scoring_system_outlook():
    """Return the effective (active) Outlook scoring_system rubric."""
    try:
        repo_root = Path(_repo_root_dir())
        default_path, user_path, active_path = _ensure_scoring_system_outlook_versions(repo_root)

        default_rubric = _load_json_path(default_path)
        user_rubric = _load_json_path(user_path)
        if isinstance(user_rubric, dict) and user_path.exists():
            # Normalize on read so user file stays minimal.
            minimized = _minimize_scoring_user_override(user_rubric)
            _write_json_path(user_path, minimized)
            user_rubric = minimized
        if not isinstance(default_rubric, dict):
            return jsonify({'error': 'Default scoring rubric missing or invalid'}), 500

        effective = _merge_scoring_rubric(default_rubric, user_rubric)
        _write_json_path(active_path, effective)

        return jsonify({
            'active': effective,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scoring_system_outlook', methods=['POST'])
def save_scoring_system_outlook():
    """Save scoring_system.user.json (override) and regenerate active scoring_system.json."""
    try:
        payload = request.json or {}
        rubric = payload.get('rubric')
        if not isinstance(rubric, dict):
            return jsonify({'error': 'Invalid rubric'}), 400

        repo_root = Path(_repo_root_dir())
        default_path, user_path, active_path = _ensure_scoring_system_outlook_versions(repo_root)

        # Store a minimal user override; descriptive fields always come from default.
        minimized = _minimize_scoring_user_override(rubric)
        _write_json_path(user_path, minimized)

        default_rubric = _load_json_path(default_path)
        if not isinstance(default_rubric, dict):
            return jsonify({'error': 'Default scoring rubric missing or invalid'}), 500

        effective = _merge_scoring_rubric(default_rubric, minimized)
        _write_json_path(active_path, effective)

        return jsonify({
            'status': 'saved',
            'active': effective,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scoring_system_outlook/reset_default', methods=['POST'])
def reset_scoring_system_outlook_to_default():
    """Reset active scoring_system.json to default and remove user override."""
    try:
        repo_root = Path(_repo_root_dir())
        default_path, user_path, active_path = _ensure_scoring_system_outlook_versions(repo_root)

        default_rubric = _load_json_path(default_path)
        if not isinstance(default_rubric, dict):
            return jsonify({'error': 'Default scoring_system.default.json missing or invalid'}), 400

        _write_json_path(active_path, default_rubric)
        try:
            if user_path.exists():
                user_path.unlink()
        except Exception:
            pass

        return jsonify({
            'status': 'reset',
            'active': _load_json_path(active_path),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recompute_priorities', methods=['POST'])
def recompute_priorities_existing_data():
    """Recompute priority_score and priority on existing briefing data.

    This is best-effort and only updates items that already have scoring_breakdown.
    """
    try:
        data_path = _resolve_briefing_data_path()
        if not data_path:
            return jsonify({
                'error': 'Briefing data not found',
                'items_seen': 0,
                'items_updated': 0,
                'items_missing_breakdown': 0,
            }), 404

        # Load effective rubric.
        repo_root = Path(_repo_root_dir())
        default_path, user_path, active_path = _ensure_scoring_system_outlook_versions(repo_root)
        default_rubric = _load_json_path(default_path)
        user_rubric = _load_json_path(user_path)
        if not isinstance(default_rubric, dict):
            return jsonify({'error': 'Default scoring rubric missing or invalid'}), 500
        if not isinstance(user_rubric, dict):
            user_rubric = {}
        effective = _merge_scoring_rubric(default_rubric, user_rubric)
        _write_json_path(active_path, effective)

        with _BRIEFING_DATA_LOCK:
            with open(data_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)

            cards = payload.get('cards')
            if not isinstance(cards, list):
                return jsonify({'error': 'Invalid briefing data: cards missing'}), 400

            stats = _recompute_priorities_in_cards(cards, effective)
            json_io.write_json_atomic(data_path, payload)

        return jsonify({
            'status': 'ok',
            'data_path': data_path,
            **stats,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/save_operation', methods=['POST'])
def save_operation():
    data = request.json or {}
    op_type = data.get('type')
    item_id = data.get('id')
    is_active = data.get('active')
    op_context = data.get('context') if isinstance(data.get('context'), dict) else None
    
    if not item_id:
        return jsonify({'error': 'No item ID provided'}), 400
        
    ops = load_user_ops()
    # Ensure keys exist for back-compat
    ops.setdefault('completed_ai', [])
    ops.setdefault('dismissed_ai', [])

    # Persist per-list disabled event types (UI filter preference)
    if op_type == 'disable_event_type':
        list_key = None
        if op_context and isinstance(op_context, dict):
            list_key = op_context.get('list_key') or op_context.get('listKey')

        if not list_key or not str(list_key).strip():
            return jsonify({'error': 'Missing list_key for disable_event_type'}), 400

        event_type_key = str(item_id).strip().lower()
        det = ops.get('disabled_event_types_by_list')
        if not isinstance(det, dict):
            det = {}

        disabled_list = det.get(str(list_key))
        if not isinstance(disabled_list, list):
            disabled_list = []

        if is_active:
            if event_type_key and event_type_key not in disabled_list:
                disabled_list.append(event_type_key)
        else:
            if event_type_key in disabled_list:
                disabled_list.remove(event_type_key)

        det[str(list_key)] = disabled_list
        ops['disabled_event_types_by_list'] = det

        save_user_ops(ops)
        return jsonify({'status': 'ok', 'ops': ops})

    # Persist pinned cards (UI preference)
    if op_type == 'pin_card':
        pinned = ops.get('pinned_cards')
        if not isinstance(pinned, list):
            pinned = []

        card_id = str(item_id).strip()
        if is_active:
            if card_id and card_id not in pinned:
                pinned.append(card_id)
        else:
            if card_id in pinned:
                pinned.remove(card_id)

        ops['pinned_cards'] = pinned
        save_user_ops(ops)
        return jsonify({'status': 'ok', 'ops': ops})
    
    if op_type == 'complete':
        if is_active:
            if item_id not in ops['completed']:
                ops['completed'].append(item_id)
            # Manual action overrides any AI-labeled state.
            if item_id in ops.get('completed_ai', []):
                ops['completed_ai'].remove(item_id)
            if item_id in ops.get('dismissed_ai', []):
                ops['dismissed_ai'].remove(item_id)
            if item_id in ops['dismissed']:
                ops['dismissed'].remove(item_id)
        else:
            if item_id in ops['completed']:
                ops['completed'].remove(item_id)
            if item_id in ops.get('completed_ai', []):
                ops['completed_ai'].remove(item_id)
                
    elif op_type == 'dismiss':
        if is_active:
            if item_id not in ops['dismissed']:
                ops['dismissed'].append(item_id)
            # Manual action overrides any AI-labeled state.
            if item_id in ops.get('dismissed_ai', []):
                ops['dismissed_ai'].remove(item_id)
            if item_id in ops.get('completed_ai', []):
                ops['completed_ai'].remove(item_id)
            if item_id in ops['completed']:
                ops['completed'].remove(item_id)
        else:
            if item_id in ops['dismissed']:
                ops['dismissed'].remove(item_id)
            if item_id in ops.get('dismissed_ai', []):
                ops['dismissed_ai'].remove(item_id)
                
    elif op_type == 'promote':
        if is_active:
            if item_id not in ops['promoted']:
                ops['promoted'].append(item_id)
        else:
            if item_id in ops['promoted']:
                ops['promoted'].remove(item_id)
                
    save_user_ops(ops)

    # Also persist a stable record (outside incremental_data) so we can rebuild
    # completed/dismissed after extraction resets (action ids may change).
    _persist_user_operation_to_store(str(op_type), str(item_id), bool(is_active), op_context)

    return jsonify({'status': 'success', 'ops': ops})


@app.route('/api/user_ops_restore_status', methods=['GET'])
def user_ops_restore_status():
    """Whether we can restore user_operation.json from the persisted ops store."""
    try:
        store_exists = os.path.exists(PERSISTED_USER_OP_FILE)
        store_count = 0
        if store_exists:
            store = _load_persisted_ops_store()
            ops_by_fp = store.get('ops_by_fingerprint')
            store_count = len(ops_by_fp) if isinstance(ops_by_fp, dict) else 0

        ops = load_user_ops()
        user_op_exists = os.path.exists(USER_OP_FILE)
        user_op_has_any = _has_any_user_ops(ops)

        can_restore = store_count > 0 and (not user_op_exists or not user_op_has_any)
        can_backup = (not store_exists) and bool(user_op_exists) and bool(user_op_has_any)
        return jsonify({
            'can_restore': bool(can_restore),
            'can_backup': bool(can_backup),
            'store_exists': bool(store_exists),
            'store_count': store_count,
            'user_op_exists': bool(user_op_exists),
            'user_op_has_any': bool(user_op_has_any),
        })
    except Exception as e:
        return jsonify({'can_restore': False, 'error': str(e)}), 500


@app.route('/api/backup_user_operation_to_store', methods=['POST'])
def backup_user_operation_to_store():
    """Create user_state/user_ops_store.json from incremental_data/user_operation.json.

    This is intended for the UI button: only useful when the store is missing but
    user_operation.json has meaningful data.
    """
    try:
        if os.path.exists(PERSISTED_USER_OP_FILE):
            return jsonify({'status': 'already_exists', 'store_exists': True}), 200

        if not os.path.exists(USER_OP_FILE):
            return jsonify({'error': f'Missing user ops file: {USER_OP_FILE}'}), 400

        ops = load_user_ops()
        if not _has_any_user_ops(ops):
            return jsonify({'error': 'user_operation.json has no valid operations'}), 400

        base_dir = os.path.dirname(os.path.abspath(__file__))
        converter = os.path.join(base_dir, 'pipeline', 'convert_user_operations_to_store.py')
        if not os.path.exists(converter):
            return jsonify({'error': f'Missing converter script: {converter}'}), 500

        data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())

        cmd = [
            sys.executable,
            converter,
            '--user-ops', USER_OP_FILE,
            '--store-out', PERSISTED_USER_OP_FILE,
            '--backup-existing-store',
        ]
        if data_path and os.path.exists(data_path):
            cmd += ['--briefing', data_path]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({
                'error': 'Backup failed',
                'returncode': result.returncode,
                'stdout': (result.stdout or '').strip(),
                'stderr': (result.stderr or '').strip(),
            }), 500

        store_exists = os.path.exists(PERSISTED_USER_OP_FILE)
        store_count = 0
        if store_exists:
            store = _load_persisted_ops_store()
            ops_by_fp = store.get('ops_by_fingerprint')
            store_count = len(ops_by_fp) if isinstance(ops_by_fp, dict) else 0

        return jsonify({
            'status': 'success',
            'store_exists': bool(store_exists),
            'store_count': store_count,
            'stdout': (result.stdout or '').strip(),
            'stderr': (result.stderr or '').strip(),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/restore_user_ops_via_ai', methods=['POST'])
def restore_user_ops_via_ai():
    """Rebuild incremental_data/user_operation.json from user_state/user_ops_store.json.

    This calls match_user_ops_to_briefing_ai.py (LLM-based matching) and writes primary
    completed/dismissed lists so the UI immediately reflects restored state.
    """
    try:
        # Guardrail: only allow when status says restore is appropriate.
        status = user_ops_restore_status().get_json()  # type: ignore
        if not status or not status.get('can_restore'):
            return jsonify({'error': 'Restore not available', 'status': status}), 400

        base_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(base_dir, 'pipeline', 'match_user_ops_to_briefing_ai.py')
        if not os.path.exists(script_path):
            return jsonify({'error': f'Missing script: {script_path}'}), 500

        data_path = _resolve_briefing_data_path() or str(_paths().briefing_data_file())
        if not os.path.exists(data_path):
            return jsonify({'error': f'Briefing data not found: {data_path}'}), 404

        cmd = [
            sys.executable,
            script_path,
            '--ops-store', PERSISTED_USER_OP_FILE,
            '--briefing', data_path,
            '--user-ops-out', USER_OP_FILE,
            '--write-user-ops',
            '--prune-unmatched-store',
            '--ops-batch-size', '5',
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({
                'error': 'Restore failed',
                'returncode': result.returncode,
                'stdout': (result.stdout or '').strip(),
                'stderr': (result.stderr or '').strip(),
            }), 500

        ops = load_user_ops()
        return jsonify({
            'status': 'success',
            'ops': ops,
            'stdout': (result.stdout or '').strip(),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/report_bug', methods=['POST'])
def report_bug():
    """Save bug report with all relevant raw data"""
    try:
        bug_data = request.json
        
        # Create bug_reports directory under the active dataset folder.
        cfg = load_config()
        bug_reports_dir = str((_resolve_data_folder_path(cfg) / _paths().bug_reports_dirname).resolve())
        os.makedirs(bug_reports_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bug_type = bug_data.get('bug_type', 'unknown')
        
        # Map internal bug_type values to readable names
        bug_type_names = {
            'duplicated_actions': 'duplicated_actions',
            'unrelated_source_combined': 'unrelated_sources',
            'missing_info': 'others_tasks',
            'not_assigned_to_me': 'not_assigned_to_me',
            'incorrect_priority': 'incorrect_priority',
            'wrong_description': 'wrong_description',
            'other': 'other'
        }
        
        bug_type_readable = bug_type_names.get(bug_type, bug_type)
        report_context = bug_data.get('report_context', {})
        card_index = report_context.get('cardIndex', 'card')
        
        # Generate filename with readable bug type
        filename = f"bug_{timestamp}_{bug_type_readable}_card{card_index}.json"
        filepath = os.path.join(bug_reports_dir, filename)
        
        # Save bug report
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(bug_data, f, indent=2, ensure_ascii=False)
        
        print(f"[BUG REPORT] Saved to {filepath}")
        print(f"  Type: {bug_type}")
        print(f"  Context: {report_context.get('type')} - {report_context.get('description', 'N/A')}")
        
        return jsonify({
            'status': 'success',
            'filename': filename,
            'filepath': filepath
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to save bug report: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/output_export', methods=['POST'])
def save_output_export():
    """Persist an Output export JSON file locally and return the full path.

    Frontend-generated exports can't reliably know the OS download folder path.
    This endpoint saves the JSON server-side (desktop app runs locally) so the UI
    can show a deterministic filepath.
    """
    try:
        payload = request.json or {}
        filename = str(payload.get('filename') or '').strip()
        data = payload.get('data')

        if not filename:
            return jsonify({'error': 'Missing filename'}), 400

        # Allow only a safe, simple filename.
        if not re.fullmatch(r'[A-Za-z0-9._-]{1,200}', filename) or not filename.lower().endswith('.json'):
            return jsonify({'error': 'Invalid filename'}), 400

        # Write under the active dataset folder's output/exports.
        cfg = load_config()
        out_dir = (_resolve_data_folder_path(cfg) / 'output' / 'exports')
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = (out_dir / filename).resolve()

        is_adaptive_card = (
            isinstance(data, dict)
            and isinstance(data.get('$schema'), str)
            and 'adaptivecards.io' in (data.get('$schema') or '')
        )

        with open(out_path, 'w', encoding='utf-8') as f:
            if is_adaptive_card:
                json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
            else:
                json.dump(data, f, indent=2, ensure_ascii=False)

        copy_to_onedrive = bool(payload.get('copy_to_onedrive') or payload.get('copyToOneDrive'))
        onedrive_dir_override_raw = payload.get('onedrive_dir') or payload.get('oneDriveDir')

        onedrive_filepath: str | None = None
        onedrive_error: str | None = None
        if copy_to_onedrive:
            try:
                override_dir: Path | None = None
                if isinstance(onedrive_dir_override_raw, str) and onedrive_dir_override_raw.strip():
                    override_dir = Path(onedrive_dir_override_raw.strip())
                    if not override_dir.is_absolute():
                        raise RuntimeError('Invalid OneDrive folder path (must be absolute)')

                if override_dir is not None:
                    onedrive_root = _resolve_onedrive_root()
                    if onedrive_root is not None:
                        try:
                            _ = override_dir.resolve().relative_to(onedrive_root.resolve())
                        except Exception:
                            raise RuntimeError('Invalid OneDrive folder path (must be under your OneDrive root)')

                    # If user pasted a file path, honor it; otherwise write top-tasks.json into the directory.
                    if str(override_dir).lower().endswith('.json'):
                        target = override_dir.resolve()
                    else:
                        target = (override_dir / 'top-tasks.json').resolve()
                else:
                    target = _onedrive_top_tasks_path()
                    if target is None:
                        raise RuntimeError(
                            'OneDrive folder not found (env var OneDrive/OneDriveCommercial/OneDriveConsumer missing)'
                        )
                target.parent.mkdir(parents=True, exist_ok=True)

                # Avoid overwriting: write a timestamped copy.
                target = _append_timestamp_to_filename(target, timestamp_iso=_utc_now_iso())
                with open(target, 'w', encoding='utf-8') as f2:
                    if is_adaptive_card:
                        json.dump(data, f2, separators=(',', ':'), ensure_ascii=False)
                    else:
                        json.dump(data, f2, indent=2, ensure_ascii=False)
                onedrive_filepath = str(target)
            except Exception as e:
                # Best-effort: do not fail the export if OneDrive isn't available.
                onedrive_error = str(e)

        return jsonify({
            'status': 'success',
            'filepath': str(out_path),
            'onedrive_filepath': onedrive_filepath,
            'onedrive_error': onedrive_error,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/bug_reports/<path:filename>', methods=['GET'])
def download_bug_report(filename):
    """Download a previously saved bug report JSON.

    This supports the 'Option A' flow: include a download link in an Outlook draft.
    """
    try:
        cfg = load_config()
        bug_reports_dir = str((_resolve_data_folder_path(cfg) / _paths().bug_reports_dirname).resolve())
        # send_from_directory protects against directory traversal
        return send_from_directory(bug_reports_dir, filename, as_attachment=True)
    except Exception as e:
        print(f"[ERROR] Failed to download bug report: {e}")
        return jsonify({'error': str(e)}), 500


def _mock_send_email_with_attachment(to_email: str, subject: str, body: str, attachment_filename: str, attachment_bytes: bytes):
    """Mock email sender.

    For now this persists a JSON payload under incremental_data/sent_emails and logs it.
    This keeps the flow testable without SMTP/Graph configuration.
    """
    cfg = load_config()
    sent_dir = str((_resolve_data_folder_path(cfg) / _paths().sent_emails_dirname).resolve())
    os.makedirs(sent_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_to = (to_email or 'unknown').replace('@', '_at_').replace('.', '_')
    out_name = f"email_{timestamp}_{safe_to}.json"
    out_path = os.path.join(sent_dir, out_name)

    payload = {
        'to': to_email,
        'subject': subject,
        'body': body,
        'attachment': {
            'filename': attachment_filename,
            'content_b64': base64.b64encode(attachment_bytes).decode('ascii')
        },
        'timestamp': datetime.now().isoformat()
    }

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print('[MOCK EMAIL] Saved')
    print(f'  To: {to_email}')
    print(f'  Subject: {subject}')
    print(f'  Attachment: {attachment_filename} ({len(attachment_bytes)} bytes)')
    print(f'  File: {out_path}')

    return out_path


@app.route('/api/email_bug_report', methods=['POST'])
def email_bug_report():
    """Send (mock) the bug report JSON file via email.

    Expected JSON:
      - filepath: server-side path returned from /api/report_bug
      - to: recipient email (mock)
      - subject/body: optional
    """
    try:
        data = request.json or {}
        filepath = data.get('filepath')
        to_email = data.get('to', 'mock@example.com')
        subject = data.get('subject') or 'AI Secretary Bug Report'
        body = data.get('body') or 'Attached is the bug report JSON from AI Secretary.'

        if not filepath:
            return jsonify({'error': 'Missing filepath'}), 400

        if not os.path.exists(filepath):
            return jsonify({'error': f'File not found: {filepath}'}), 404

        with open(filepath, 'rb') as f:
            attachment_bytes = f.read()

        attachment_filename = os.path.basename(filepath)
        out_path = _mock_send_email_with_attachment(
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_filename=attachment_filename,
            attachment_bytes=attachment_bytes,
        )

        return jsonify({'status': 'success', 'mock_email_file': out_path})
    except Exception as e:
        print(f"[ERROR] Failed to mock-send email: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/copy_bug_report_to_clipboard', methods=['POST'])
def copy_bug_report_to_clipboard():
    """Copy a bug report JSON file reference to the Windows clipboard.

    This enables the user to Ctrl+V in Outlook to attach the file.
    Note: This is Windows-only and requires the server to run in the same desktop session.

    Expected JSON:
      - filename: bug report filename under incremental_data/bug_reports
        OR
      - filepath: full/relative path to the saved bug report JSON
    """
    try:
        data = request.json or {}
        filename = data.get('filename')
        filepath = data.get('filepath')

        if not filename and not filepath:
            return jsonify({'error': 'Missing filename or filepath'}), 400

        if filename and not filepath:
            cfg = load_config()
            filepath = str((_resolve_data_folder_path(cfg) / _paths().bug_reports_dirname / filename).resolve())

        abs_path = os.path.abspath(filepath)
        if not os.path.exists(abs_path):
            return jsonify({'error': f'File not found: {abs_path}'}), 404

        if os.name != 'nt':
            return jsonify({'error': 'Clipboard file copy is only supported on Windows'}), 400

        # Use PowerShell Set-Clipboard -Path to place a file reference (CF_HDROP) in the clipboard.
        escaped_path = abs_path.replace("'", "''")
        cmd = [
            'powershell',
            '-NoProfile',
            '-NonInteractive',
            '-Command',
            f"Set-Clipboard -Path '{escaped_path}'"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({'error': 'Failed to set clipboard', 'details': result.stderr.strip()}), 500

        print('[CLIPBOARD] Copied file path for attachment')
        print(f'  File: {abs_path}')

        return jsonify({'status': 'success', 'filepath': abs_path})
    except Exception as e:
        print(f"[ERROR] Failed to copy file to clipboard: {e}")
        return jsonify({'error': str(e)}), 500

def load_config():
    base_dir = Path(_repo_root_dir())
    # If the effective file doesn't exist yet (fresh clone), generate it from
    # pipeline_config.default.json + pipeline_config.user.json.
    try:
        default_path, _, effective_path = get_config_paths(base_dir)
        if not effective_path.exists():
            cfg = ensure_effective_config(base_dir)
        else:
            with effective_path.open('r', encoding='utf-8') as f:
                cfg = json.load(f) or {}

            # If defaults gained new keys since this file was generated,
            # regenerate the effective config so UI/settings stay in sync.
            try:
                if default_path.exists():
                    with default_path.open('r', encoding='utf-8') as df:
                        defaults = json.load(df) or {}
                    if isinstance(defaults, dict) and isinstance(cfg, dict):
                        if any(k not in cfg for k in defaults.keys()):
                            cfg = ensure_effective_config(base_dir)
            except Exception:
                pass
    except Exception:
        cfg = {}

    if not isinstance(cfg, dict):
        cfg = {}

    # Minimal fallback defaults (kept for backward safety)
    default_config = {
        'server_host': 'localhost',
        'server_port': 5000,
        'briefing_data_path': 'incremental_data/output/briefing_data.json'
    }
    return {**default_config, **cfg}


def _redact_config(obj):
    """Best-effort redaction for config values that look like secrets."""
    try:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                key = str(k).lower()
                if any(s in key for s in ('secret', 'token', 'password', 'api_key', 'apikey', 'key')):
                    out[k] = '[REDACTED]'
                else:
                    out[k] = _redact_config(v)
            return out
        if isinstance(obj, list):
            return [_redact_config(x) for x in obj]
        return obj
    except Exception:
        return '[REDACTED]'


def _redact_config_with_keys(obj):
    """Return (redacted_obj, redacted_keys_set) for top-level keys."""
    redacted_keys = set()
    if not isinstance(obj, dict):
        return _redact_config(obj), redacted_keys

    out = {}
    for k, v in obj.items():
        key_lower = str(k).lower()
        if any(s in key_lower for s in ('secret', 'token', 'password', 'api_key', 'apikey', 'key')):
            out[k] = '[REDACTED]'
            redacted_keys.add(str(k))
        else:
            out[k] = _redact_config(v)
    return out, redacted_keys


def _get_ui_setting_keys(cfg):
    """Return a stable list of top-level keys allowed to be edited in UI.

    If cfg contains a non-empty list `ui_setting_keys`, those are used.
    Otherwise, return an empty list (meaning "no restriction").
    """
    try:
        if not isinstance(cfg, dict):
            return []
        keys = cfg.get('ui_setting_keys')
        if isinstance(keys, list):
            out = []
            for k in keys:
                if isinstance(k, str) and k.strip():
                    out.append(k)
            # De-duplicate while preserving order
            seen = set()
            uniq = []
            for k in out:
                if k not in seen:
                    uniq.append(k)
                    seen.add(k)
            return uniq
        return []
    except Exception:
        return []


def _filter_config_for_ui(cfg, allowed_keys):
    """Return a shallow dict with only allowed_keys (or full cfg when allowed_keys is empty)."""
    if not isinstance(cfg, dict):
        return {}
    if not allowed_keys:
        return {k: v for k, v in cfg.items() if k != 'ui_setting_keys'}
    return {k: cfg.get(k) for k in allowed_keys if k != 'ui_setting_keys'}


@app.route('/api/pipeline_config', methods=['GET'])
def get_pipeline_config():
    """Return pipeline_config.json contents for UI display (with basic redaction)."""
    try:
        cfg = load_config()
        allowed_keys = _get_ui_setting_keys(cfg)
        ui_cfg = _filter_config_for_ui(cfg, allowed_keys)
        display_cfg, redacted_keys = _redact_config_with_keys(ui_cfg)
        return jsonify({
            'config': ui_cfg,
            'display_config': display_cfg,
            'redacted_keys': sorted(list(redacted_keys)),
            'allowed_keys': allowed_keys,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipeline_config', methods=['POST'])
def save_pipeline_config():
    """Persist settings as user overrides and regenerate pipeline_config.json.

    Expected JSON:
      { "updates": { "key": <value>, ... } }
    """
    try:
        payload = request.json or {}
        updates = payload.get('updates')
        if not isinstance(updates, dict):
            return jsonify({'error': 'Invalid updates'}), 400

        current = load_config()

        # Only accept keys explicitly allowed by ui_setting_keys (when present)
        # and never accept secret-like keys from UI.
        allowed_keys = _get_ui_setting_keys(current)
        cleaned_updates = {}
        for k, v in updates.items():
            if str(k) == 'ui_setting_keys':
                continue
            if allowed_keys and str(k) not in allowed_keys:
                continue
            key_lower = str(k).lower()
            if any(s in key_lower for s in ('secret', 'token', 'password', 'api_key', 'apikey', 'key')):
                continue
            cleaned_updates[k] = v

        base_dir = Path(_repo_root_dir())
        save_effective_from_updates(base_dir, cleaned_updates)

        if _pipeline_auto_start_enabled(cleaned_updates):
            global _PIPELINE_USER_STOPPED
            _PIPELINE_USER_STOPPED = False
            _kickoff_pipeline_autostart_background(once=False)

        cfg = load_config()
        
        allowed_keys = _get_ui_setting_keys(cfg)
        ui_cfg = _filter_config_for_ui(cfg, allowed_keys)
        display_cfg, redacted_keys = _redact_config_with_keys(ui_cfg)
        return jsonify({
            'status': 'saved',
            'config': ui_cfg,
            'display_config': display_cfg,
            'redacted_keys': sorted(list(redacted_keys)),
            'allowed_keys': allowed_keys,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.before_request
def _pipeline_autostart_hook():
    # Ensure we attempt auto-start only once, and only once requests begin
    # (works for WSGI servers like waitress where __main__ isn't executed).
    global _PIPELINE_AUTOSTART_ATTEMPTED
    if not _PIPELINE_AUTOSTART_ATTEMPTED:
        _kickoff_pipeline_autostart_background()


@app.route('/api/heartbeat', methods=['POST'])
def _api_heartbeat():
    """Lightweight keepalive from browser clients (no-op, kept for backward compat)."""
    return jsonify({'ok': True})


@app.route('/api/client_closing', methods=['POST'])
def _api_client_closing():
    """Legacy endpoint — no-op. Pipeline keeps running regardless of tab state."""
    return jsonify({'ok': True})


@app.route('/api/extension_zip')
def _api_extension_zip():
    """Serve the browser_extension/ folder as a .zip download."""
    import zipfile, io
    ext_dir = Path(BASE_DIR) / 'browser_extension'
    if not ext_dir.is_dir():
        return jsonify({'error': 'browser_extension folder not found'}), 404
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in ext_dir.rglob('*'):
            if f.is_file() and '__pycache__' not in str(f):
                zf.write(f, f'browser_extension/{f.relative_to(ext_dir)}')
    buf.seek(0)
    return send_file(buf, mimetype='application/zip', as_attachment=True,
                     download_name='ai_secretary_extension.zip')


@app.route('/api/install_extension', methods=['POST', 'OPTIONS'])
def _api_install_extension():
    """Run the direct Edge extension installer and return the result."""
    if request.method == 'OPTIONS':
        return '', 204
    script_candidates = [
        Path(BASE_DIR) / 'install_extension.py',
        Path(BASE_DIR) / 'pipeline' / 'install_extension_playwright.py',
    ]
    script = next((candidate for candidate in script_candidates if candidate.exists()), None)
    if script is None:
        searched = [str(candidate) for candidate in script_candidates]
        return jsonify({'ok': False, 'error': 'No extension installer script found', 'searched': searched}), 404

    # Optional URL to reopen in Edge after install
    body = request.get_json(silent=True) or {}
    relaunch_url = body.get('relaunch_url', 'http://localhost:5000')

    def _relaunch_edge(url: str):
        """Relaunch the app or open a new Edge window after a short pause."""
        import time as _t
        _t.sleep(1)
        if str(url or '').lower().startswith('ai-secretary://'):
            try:
                os.startfile(url)  # type: ignore[attr-defined]
                return
            except Exception:
                try:
                    subprocess.Popen(['cmd', '/c', 'start', '', url], shell=False)
                    return
                except Exception:
                    pass
        candidates = [
            r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
            r'C:\Program Files\Microsoft\Edge\Application\msedge.exe',
        ]
        for edge_exe in candidates:
            if os.path.exists(edge_exe):
                try:
                    subprocess.Popen([edge_exe, '--no-first-run', url])
                    return
                except Exception:
                    pass
        try:
            subprocess.Popen(['cmd', '/c', 'start', '', 'microsoft-edge:' + url], shell=False)
        except Exception:
            pass

    try:
        try:
            subprocess.run(
                ['taskkill', '/IM', 'msedge.exe', '/F'],
                capture_output=True,
                text=True,
                timeout=15,
                cwd=str(BASE_DIR),
            )
        except Exception:
            pass

        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True, text=True, timeout=120,
            cwd=str(BASE_DIR),
        )
        output = (result.stdout or '') + (result.stderr or '')
        success = result.returncode == 0
        threading.Thread(target=_relaunch_edge, args=(relaunch_url,), daemon=True).start()
        return jsonify({'ok': success, 'output': output, 'returncode': result.returncode, 'script': str(script)})
    except subprocess.TimeoutExpired:
        threading.Thread(target=_relaunch_edge, args=(relaunch_url,), daemon=True).start()
        return jsonify({'ok': False, 'error': 'Installation timed out after 120 s.', 'output': ''})
    except Exception as exc:
        threading.Thread(target=_relaunch_edge, args=(relaunch_url,), daemon=True).start()
        return jsonify({'ok': False, 'error': str(exc), 'output': ''})


# ---------- Git update check ----------

_SERVER_GIT_COMMIT = None  # captured at startup

def _capture_startup_commit():
    """Capture the current git commit hash when the server starts."""
    global _SERVER_GIT_COMMIT
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, timeout=5,
            cwd=str(BASE_DIR)
        )
        if result.returncode == 0:
            _SERVER_GIT_COMMIT = result.stdout.strip()
    except Exception:
        pass

_capture_startup_commit()


@app.route('/api/check_update')
def _api_check_update():
    """Compatibility wrapper around the background git update checker state."""
    with _APP_UPDATE_LOCK:
        state = dict(_APP_UPDATE_STATE)
    return jsonify({
        'has_update': bool(state.get('update_available')),
        'server_stale': bool(state.get('server_stale')),
        'current': state.get('current'),
        'latest': state.get('latest'),
        'server_commit': state.get('server_commit'),
        'message': state.get('message'),
        'behind_by': state.get('behind_by', 0),
        'checking': bool(state.get('checking')),
        'upstream': state.get('upstream'),
        'error': state.get('error'),
    })


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Start the AI Secretary web server with React frontend.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default data location
  python server_react.py

  # Specify custom data file
  python server_react.py --data C:\\my_data\\briefing_data.json
  
  # Use relative path
  python server_react.py --data incremental_data/output/briefing_data.json

Data loading priority:
  1. --data command-line argument
  2. briefing_data_path in pipeline_config.json
  3. Default: incremental_data/output/briefing_data.json

The server will serve:
  - React app at http://localhost:5000 (or configured port)
  - API endpoints at http://localhost:5000/api/*
        """
    )
    parser.add_argument('--data', help='Path to briefing_data.json file')
    args = parser.parse_args()
    
    config = load_config()
    
    # Azure App Service requires binding to 0.0.0.0 and typically provides the port via env.
    # Keep localhost defaults for local dev.
    is_azure = bool(os.environ.get('WEBSITE_SITE_NAME') or os.environ.get('WEBSITE_INSTANCE_ID'))
    host_default = '0.0.0.0' if is_azure else 'localhost'
    host = config.get('server_host', host_default)

    env_port = os.environ.get('PORT') or os.environ.get('WEBSITES_PORT')
    if env_port:
        try:
            port = int(str(env_port).strip())
        except Exception:
            port = int(config.get('server_port', 5000))
    else:
        port = int(config.get('server_port', 5000))
    
    # Use command-line arg if provided, otherwise use config, otherwise use default
    if args.data:
        data_path = args.data
        app.config['BRIEFING_DATA_PATH_SOURCE'] = 'cli'
    else:
        data_path = config.get('briefing_data_path')
        if not data_path:
            data_path = 'incremental_data/output/briefing_data.json'
        app.config['BRIEFING_DATA_PATH_SOURCE'] = 'config'
    
    print(f"Starting server on http://{host}:{port}")
    print(f"Loading data from: {data_path}")
    
    # Make data_path globally accessible
    app.config['BRIEFING_DATA_PATH'] = data_path

    # Start background update checker (git fetch/behind count).
    # Under Flask debug reloader, only run in the main worker.
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not os.environ.get('FLASK_DEBUG'):
        try:
            raw = config.get('app_update_poll_interval_seconds', 30)
            interval_sec = int(raw) if raw is not None else 30
        except Exception:
            interval_sec = 30

        # Keep it reasonable (avoid hammering git, but keep UI responsive).
        interval_sec = max(10, min(3600, interval_sec))
        _ensure_update_checker_started(interval_sec=interval_sec)

    # Auto-trigger the pipeline after server start (opt-in).
    _kickoff_pipeline_autostart_background()
    
    # In production, serve built React app.
    # In development, React dev server runs on port 3000 and proxies API calls.
    # On Windows, Werkzeug's debug reloader can fail with WinError 10038 when
    # reusing the inherited listener socket, so keep the debugger but disable
    # the reloader there.
    debug_enabled = not is_azure
    use_reloader = debug_enabled and os.name != 'nt'
    app.run(host=host, port=port, debug=debug_enabled, use_reloader=use_reloader)
