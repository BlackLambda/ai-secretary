import atexit
import argparse
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.pipeline_config_manager import ensure_effective_config

from ai_secretary_core import pipeline_state
from ai_secretary_core.paths import RepoPaths

BASE_DIR = Path(__file__).resolve().parent.parent
PATHS = RepoPaths(BASE_DIR)

# Configuration (kept as strings only where they represent module-level behavior)
INCREMENTAL_DATA_DIR = PATHS.incremental_data_dirname


DEFAULT_AZURE_OPENAI_TIMEOUT_SECONDS = 20.0
_RUN_COMMAND_BASE_ENV: dict[str, str] | None = None


def _load_schedule_config() -> dict:
    """Load schedule config from the effective pipeline_config.json."""
    try:
        cfg_path = BASE_DIR / 'pipeline_config.json'
        if cfg_path.exists():
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            if isinstance(cfg, dict):
                return {
                    'schedule_enabled': bool(cfg.get('schedule_enabled', False)),
                    'schedule_days': cfg.get('schedule_days', [1, 2, 3, 4, 5]),
                    'schedule_start_hour': int(cfg.get('schedule_start_hour', 8)),
                    'schedule_start_minute': int(cfg.get('schedule_start_minute', 0)),
                    'schedule_end_hour': int(cfg.get('schedule_end_hour', 17)),
                    'schedule_end_minute': int(cfg.get('schedule_end_minute', 0)),
                }
    except Exception as e:
        print(f"[WARN] Failed to load schedule config: {e}")
    return {'schedule_enabled': False, 'schedule_days': [1, 2, 3, 4, 5], 'schedule_start_hour': 8, 'schedule_start_minute': 0, 'schedule_end_hour': 17, 'schedule_end_minute': 0}


def _is_within_schedule(sched: dict) -> bool:
    """Check if current local time falls within the configured schedule window."""
    if not sched.get('schedule_enabled', False):
        return True  # No schedule restriction — always allowed
    now = datetime.now()
    # Python: Monday=0, but we store Monday=1..Sunday=7 (ISO weekday)
    day_of_week = now.isoweekday()  # 1=Monday .. 7=Sunday
    allowed_days = sched.get('schedule_days', [1, 2, 3, 4, 5])
    if day_of_week not in allowed_days:
        return False
    start_hour = sched.get('schedule_start_hour', 8)
    start_minute = sched.get('schedule_start_minute', 0)
    end_hour = sched.get('schedule_end_hour', 17)
    end_minute = sched.get('schedule_end_minute', 0)
    current_minutes = now.hour * 60 + now.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute
    return start_minutes <= current_minutes < end_minutes


def _seconds_until_next_schedule_window(sched: dict) -> int:
    """Calculate seconds until the next schedule window opens."""
    now = datetime.now()
    allowed_days = sched.get('schedule_days', [1, 2, 3, 4, 5])
    start_hour = sched.get('schedule_start_hour', 8)
    start_minute = sched.get('schedule_start_minute', 0)
    if not allowed_days:
        return 3600  # Fallback: 1 hour

    # Try today first (if today is an allowed day and start time hasn't passed)
    from datetime import timedelta
    for offset in range(8):  # Check up to 7 days ahead
        candidate = now + timedelta(days=offset)
        if candidate.isoweekday() in allowed_days:
            # Build the target datetime at start_hour:start_minute
            target = candidate.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
            if target > now:
                delta = (target - now).total_seconds()
                return max(int(delta), 60)
    return 3600  # Fallback


def _calculate_next_aligned_run(sched: dict, interval_minutes: int) -> datetime:
    """Calculate next clock-aligned run time based on schedule start time and interval.
    
    If schedule is enabled, align to start_time + N * interval.
    Otherwise, just return now + interval.
    """
    now = datetime.now()
    
    if not sched.get('schedule_enabled', False):
        # No alignment — just add interval
        from datetime import timedelta
        return now + timedelta(minutes=interval_minutes)
    
    start_hour = sched.get('schedule_start_hour', 8)
    start_minute = sched.get('schedule_start_minute', 0)
    end_hour = sched.get('schedule_end_hour', 17)
    end_minute = sched.get('schedule_end_minute', 0)
    allowed_days = sched.get('schedule_days', [1, 2, 3, 4, 5])
    
    # Start from today's schedule start time
    from datetime import timedelta
    base = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    
    print(f"[SCHEDULE DEBUG] Now: {now.strftime('%a %H:%M:%S')}, Base: {base.strftime('%a %H:%M:%S')}, Interval: {interval_minutes}m")
    
    # If we're before today's start time and today is allowed, use today's base
    if now < base and now.isoweekday() in allowed_days:
        print(f"[SCHEDULE DEBUG] Before start time today, returning base: {base.strftime('%a %H:%M:%S')}")
        return base
    
    # Otherwise, find the next aligned slot
    interval_delta = timedelta(minutes=interval_minutes)
    candidate = base
    
    # Walk forward in interval steps until we find a slot > now
    max_iterations = 1000  # Safety limit
    iteration_count = 0
    for _ in range(max_iterations):
        iteration_count += 1
        if candidate > now:
            # Check if this candidate is within the schedule window
            cand_day = candidate.isoweekday()
            if cand_day in allowed_days:
                cand_minutes = candidate.hour * 60 + candidate.minute
                end_minutes = end_hour * 60 + end_minute
                if cand_minutes < end_minutes:
                    print(f"[SCHEDULE DEBUG] Found next slot after {iteration_count} iterations: {candidate.strftime('%a %H:%M:%S')}")
                    return candidate
            # If not in window, jump to next day's start
            candidate = (candidate + timedelta(days=1)).replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
            print(f"[SCHEDULE DEBUG] Candidate outside window, jumping to next day: {candidate.strftime('%a %H:%M:%S')}")
        else:
            candidate += interval_delta
    
    # Fallback: just return now + interval
    print(f"[SCHEDULE DEBUG] Max iterations reached, using fallback")
    return now + interval_delta


def _resolve_configured_data_folder_relpath(cfg: dict | None, *, base_dir: Path) -> str:
    """Resolve the configured dataset folder relative to the repo root.

    This mirrors server_react.py's active_data_folder_path/data_folder_paths/data_folder_path
    logic so the pipeline and UI stay consistent.
    """
    chosen: str | None = None
    if isinstance(cfg, dict):
        active = cfg.get('active_data_folder_path')
        if isinstance(active, str) and active.strip():
            chosen = active.strip()

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
                chosen = legacy.strip()

    if not chosen:
        return 'incremental_data'

    p = Path(chosen)

    # If absolute, only accept if it's inside the repo root.
    if p.is_absolute():
        try:
            rel = p.resolve().relative_to(base_dir.resolve())
            return str(rel).replace('\\', '/')
        except Exception:
            print(f"[WARN] Config active_data_folder_path is absolute and outside repo root: {p}. Falling back to incremental_data")
            return 'incremental_data'

    # Normalize relative path.
    return str(p).replace('\\', '/')


def _configure_active_data_folder(cfg: dict | None, *, base_dir: Path) -> None:
    """Apply dataset folder selection by re-binding PATHS and dependent constants."""
    global PATHS
    global INCREMENTAL_DATA_DIR
    global USER_PROFILE_FILE
    global MASTER_THREADS_FILE
    global MASTER_TEAMS_DIR
    global MASTER_TEAMS_SUMMARY
    global PRUNE_LOG_FILE

    chosen_rel = _resolve_configured_data_folder_relpath(cfg, base_dir=base_dir)
    if chosen_rel and chosen_rel != INCREMENTAL_DATA_DIR:
        INCREMENTAL_DATA_DIR = chosen_rel
        PATHS = RepoPaths(BASE_DIR, incremental_data_dirname=INCREMENTAL_DATA_DIR)

        # Recompute dataset-derived constants so downstream steps stay aligned.
        MASTER_THREADS_FILE = str(PATHS.incremental_data_dir() / PATHS.outlook_dirname / "master_threads.json")
        MASTER_TEAMS_DIR = str(PATHS.incremental_data_dir() / PATHS.teams_dirname / "master_teams_conversations")
        MASTER_TEAMS_SUMMARY = str(PATHS.incremental_data_dir() / PATHS.teams_dirname / "master_teams_analysis_summary.json")
        PRUNE_LOG_FILE = str(PATHS.prune_log_file())

    # Dataset-scoped profile (keeps alias/email consistent per dataset).
    # IMPORTANT: do this unconditionally so we never default to repo-root user_profile.json.
    USER_PROFILE_FILE = str(Path(INCREMENTAL_DATA_DIR) / 'user_profile.json')


def _get_outlook_event_timeout_seconds(cfg: dict | None) -> float:
    """Return the Azure OpenAI timeout for Outlook event extraction.

    Uses the configured timeout (default 60s) which is also used as the base.
    """
    # Default is 60s unless overridden via config.
    v = 60.0
    try:
        if isinstance(cfg, dict) and cfg.get('azure_openai_timeout_seconds') is not None:
            v = float(cfg.get('azure_openai_timeout_seconds'))
    except Exception:
        v = 60.0

    if v < 1:
        v = 1.0
    if v > 600:
        v = 600.0
    return v


def _configure_pipeline_subprocess_env(cfg: dict | None) -> float:
    """Configure base env for pipeline subprocesses and return event timeout.

    Sets PYTHONUNBUFFERED (live worker output) and AZURE_OPENAI_TIMEOUT_SECONDS
    for all subprocesses so every step uses the configured timeout.
    Model/backend are intentionally NOT locked here — each subprocess re-reads
    pipeline_config.json via _resolve_ai_backend() so mid-run config changes
    (e.g. switching model) take effect on the next subprocess that starts.
    """
    global _RUN_COMMAND_BASE_ENV

    configured_timeout = _get_outlook_event_timeout_seconds(cfg)

    _RUN_COMMAND_BASE_ENV = {
        'AZURE_OPENAI_TIMEOUT_SECONDS': str(configured_timeout),
        'PYTHONUNBUFFERED': '1',  # force line-buffered output when captured via PIPE
    }
    return configured_timeout

# AI Scripts
AI_EXTRACT_EVENTS_SCRIPT = os.path.join("outlook_v2", "ai_extract_events.py")
AI_DEDUP_EVENTS_SCRIPT = os.path.join("outlook_v2", "ai_dedup_events.py")
AI_EXTRACT_ACTIONS_SCRIPT = os.path.join("outlook_v2", "ai_extract_actions.py")
AI_VALIDATE_ACTIONS_SCRIPT = os.path.join("outlook_v2", "ai_validate_actions.py")
AI_DEDUP_TODOS_SCRIPT = os.path.join("outlook_v2", "ai_dedup_todos.py")
ANALYZE_TEAMS_SCRIPT = os.path.join("teams", "analyze_teams_conversations.py")

USER_PROFILE_FILE = str(Path(INCREMENTAL_DATA_DIR) / 'user_profile.json')
EVENT_GUIDE = str(Path("outlook_v2") / "event_extraction_guide.md")
RELATIONSHIP_GUIDE = str(Path("outlook_v2") / "event_relationship_guide.md")
DEDUP_GUIDE = str(Path("outlook_v2") / "event_deduplication_guide.md")
ACTION_GUIDE = str(Path("outlook_v2") / "action_extraction_guide.md")
VALIDATION_GUIDE = str(Path("outlook_v2") / "action_validation_guide.md")
TEAMS_GUIDE = str(Path("teams") / "Teams_Chat.md")

SYNC_TOPICS_SCRIPT = "pipeline/sync_topics.py"
TOPICS_FILE = str(PATHS.topics_file())
USER_TOPICS_FILE = str(PATHS.user_topics_file())

MASTER_THREADS_FILE = str(PATHS.incremental_data_dir() / PATHS.outlook_dirname / "master_threads.json")
MASTER_TEAMS_DIR = str(PATHS.incremental_data_dir() / PATHS.teams_dirname / "master_teams_conversations")
MASTER_TEAMS_SUMMARY = str(PATHS.incremental_data_dir() / PATHS.teams_dirname / "master_teams_analysis_summary.json")

PIPELINE_STATUS_FILE = "pipeline_status.json"

# Allow server to override where pipeline_status.json is stored.
# This prevents mixed status when multiple datasets are used.
_STATUS_PATH_OVERRIDE = os.environ.get('AI_SECRETARY_PIPELINE_STATUS_PATH') or os.environ.get('PIPELINE_STATUS_PATH')
if isinstance(_STATUS_PATH_OVERRIDE, str) and _STATUS_PATH_OVERRIDE.strip():
    PIPELINE_STATUS_FILE = _STATUS_PATH_OVERRIDE.strip()

# Persisted Observation snapshots (so prior runs can be viewed after restart).
OBSERVATION_SNAPSHOT_DIRNAME = "observation_runs"

# Backup folder used to protect incremental_data during a working run.
INCREMENTAL_DATA_BACKUP_DIR = "incremental_data_backup"  # legacy default; dynamic per-dataset backup is computed at runtime

PRUNE_LOG_FILE = str(PATHS.prune_log_file())

# ---------------------------------------------------------------------------
# Tee-style writer: mirrors stdout/stderr to a log file
# ---------------------------------------------------------------------------

PIPELINE_LOG_DIR = "user_state"
PIPELINE_LOG_FILE = "pipeline.log"
_MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MB — rotate when exceeded


class _TeeWriter:
    """Write to both the original stream and a log file."""

    def __init__(self, original_stream: object, log_file_handle: object) -> None:
        self._original = original_stream
        self._log = log_file_handle

    def write(self, text: str) -> int:
        try:
            self._original.write(text)
        except Exception:
            pass
        try:
            # Strip ANSI colour codes for the log file
            clean = re.sub(r'\033\[[0-9;]*m', '', text)
            self._log.write(clean)
            self._log.flush()
        except Exception:
            pass
        return len(text)

    def flush(self) -> None:
        try:
            self._original.flush()
        except Exception:
            pass
        try:
            self._log.flush()
        except Exception:
            pass

    # Forward attribute lookups (encoding, fileno, etc.) to the original stream.
    def __getattr__(self, name: str) -> object:
        return getattr(self._original, name)


def _setup_tee_logging() -> object | None:
    """Redirect stdout and stderr to both console and a log file.

    Returns the open file handle (caller should close on exit) or None.
    """
    try:
        log_dir = BASE_DIR / PIPELINE_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / PIPELINE_LOG_FILE

        # Rotate: if the log exceeds _MAX_LOG_BYTES, rename to .prev
        try:
            if log_path.exists() and log_path.stat().st_size > _MAX_LOG_BYTES:
                prev = log_path.with_suffix('.prev.log')
                if prev.exists():
                    prev.unlink()
                log_path.rename(prev)
        except Exception:
            pass

        fh = open(log_path, 'a', encoding='utf-8', errors='replace')
        fh.write(f"\n{'=' * 60}\n")
        fh.write(f"Pipeline started at {datetime.now().isoformat()}\n")
        fh.write(f"{'=' * 60}\n")

        sys.stdout = _TeeWriter(sys.stdout, fh)  # type: ignore[assignment]
        sys.stderr = _TeeWriter(sys.stderr, fh)  # type: ignore[assignment]
        return fh
    except Exception as e:
        print(f"[WARN] Failed to set up tee logging: {e}")
        return None


YELLOW = "\033[93m"
RESET = "\033[0m"

# Per-worker colours for concurrent log output.
_WORKER_COLORS = [
    "\033[96m",   # cyan     — Worker-1
    "\033[95m",   # magenta  — Worker-2
    "\033[37m",   # grey     — Worker-3
    "\033[94m",   # blue     — Worker-4
    "\033[33m",   # dark yellow — Worker-5
]


def _worker_color(worker_id: int) -> str:
    """Return an ANSI colour code for the given 1-based worker id."""
    return _WORKER_COLORS[(worker_id - 1) % len(_WORKER_COLORS)]


def _yellow(text: str) -> str:
    # Match existing repo convention (see apply_filters_to_existing_data.py, outlook_v2/*)
    return f"{YELLOW}{text}{RESET}"


def _safe_filename_part(raw: object, *, fallback: str) -> str:
    s = str(raw or '').strip()
    if not s:
        return fallback
    s = re.sub(r'[^a-zA-Z0-9_-]+', '-', s)
    s = re.sub(r'-{2,}', '-', s).strip('-')
    return s[:80] if s else fallback


def _observation_artifacts_dir() -> Path:
    try:
        d = PATHS.incremental_data_dir() / OBSERVATION_SNAPSHOT_DIRNAME / 'artifacts'
        d.mkdir(parents=True, exist_ok=True)
        return d
    except Exception:
        d = Path('incremental_data') / OBSERVATION_SNAPSHOT_DIRNAME / 'artifacts'
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        return d


def _abs_path(path_str: str, cwd: str | Path | None = None) -> Path:
    p = Path(str(path_str))
    if p.is_absolute():
        return p
    base = Path(str(cwd)) if cwd else _pipeline_base_dir()
    return (base / p).resolve()


def _capture_before_outputs(*, step_id: str | None, output_files: list[str] | None, cwd: str | Path | None, run_id: str | None) -> dict[str, str]:
    # Snapshot pre-step output files that already exist so we can show a diff later.
    out: dict[str, str] = {}
    if not output_files:
        return out
    sid = str(step_id or '').strip()
    if not sid:
        return out

    artifacts_dir = _observation_artifacts_dir()
    MAX_CAPTURE_BYTES = 10_000_000

    rid = _safe_filename_part(run_id, fallback='run')
    step_part = _safe_filename_part(sid, fallback='step')
    ts = int(time.time())

    for raw in output_files:
        try:
            key = str(raw).strip()
            if not key:
                continue
            ap = _abs_path(key, cwd)
            if not ap.exists() or not ap.is_file():
                continue
            try:
                size = int(ap.stat().st_size)
            except Exception:
                size = 0
            if size > MAX_CAPTURE_BYTES:
                continue

            base = _safe_filename_part(Path(key).name, fallback='file')
            dest = artifacts_dir / f"before_{rid}_{step_part}_{base}_{ts}.json"
            shutil.copy2(str(ap), str(dest))
            out[key] = str(dest)
        except Exception:
            continue
    return out

# Global list to track child processes
child_processes = []

# Lock for thread-safe pipeline_status.json writes (used by parallel workers).
_STATUS_LOCK = threading.Lock()

# Track whether we're currently in a "working" section (i.e. modifying incremental_data).
_PIPELINE_IN_WORKING_STATE = False

# When true, the pipeline will not create/restore/delete incremental_data_backup.
# This is intended for server-managed start/stop flows.
_BACKUP_ENABLED = True


def _pipeline_base_dir() -> Path:
    return BASE_DIR


def _pipeline_status_path() -> Path:
    return PATHS.pipeline_status_file()


def _incremental_data_path() -> Path:
    return PATHS.incremental_data_dir()


def _incremental_backup_path() -> Path:
    # IMPORTANT: incremental_data may be a symlink/junction to a dataset folder.
    # Always derive the backup next to the *resolved* dataset folder to avoid
    # collisions when multiple datasets are used.
    try:
        src = _incremental_data_path().resolve()
        return src.parent / f"{src.name}_backup"
    except Exception:
        # Best-effort fallback to legacy behavior.
        return _pipeline_base_dir() / INCREMENTAL_DATA_BACKUP_DIR


def _incremental_backup_spec() -> tuple[Path, str, str]:
    """Return (base_dir, incremental_dirname, backup_dirname) for pipeline_state.* calls.

    The backup is always a sibling of the resolved incremental_data directory:
      <dataset>/<name>            (e.g., .../incremental_data)
      <dataset>/<name>_backup     (e.g., .../incremental_data_backup)
    """
    try:
        src = _incremental_data_path().resolve()
    except Exception:
        src = _incremental_data_path()
    base_dir = src.parent
    incremental_dirname = src.name
    backup_dirname = f"{incremental_dirname}_backup"
    return base_dir, incremental_dirname, backup_dirname


def delete_pipeline_status_file() -> None:
    """Best-effort deletion of pipeline_status.json (used on interruption)."""
    try:
        candidates = {
            Path.cwd() / PIPELINE_STATUS_FILE,
            _pipeline_status_path(),
            Path(str(PIPELINE_STATUS_FILE)),
        }
        for p in candidates:
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                # continue trying other candidates
                pass
    except Exception as e:
        print(f"[WARN] Failed to delete pipeline status file: {e}")

def cleanup_processes():
    """Kill all child processes when the script exits."""
    global child_processes
    if child_processes:
        print("\n[INFO] Cleaning up child processes...")
        for p in child_processes:
            try:
                if p.poll() is None:  # Process is still running
                    if os.name == 'nt':
                        # Windows: Kill process tree
                        subprocess.call(['taskkill', '/F', '/T', '/PID', str(p.pid)], 
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    else:
                        # Unix: Send SIGTERM
                        p.terminate()
                        try:
                            p.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            p.kill()
                    print(f"[INFO] Killed process {p.pid}")
            except Exception as e:
                print(f"[WARN] Failed to kill process {p.pid}: {e}")
        child_processes.clear()

def signal_handler(sig, frame):
    """Handle Ctrl+C and other signals."""
    global _PIPELINE_IN_WORKING_STATE
    print("\n[INFO] Received interrupt signal. Cleaning up...")
    # If interrupted during a working run, restore incremental_data from backup.
    if _BACKUP_ENABLED and _PIPELINE_IN_WORKING_STATE:
        src = _incremental_data_path()
        backup = _incremental_backup_path()
        if backup.exists():
            print(f"[WARN] Restoring incremental data from backup: {backup} -> {src}")
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            ok = pipeline_state.restore_incremental_from_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            if not ok:
                print("[ERROR] Failed to restore incremental_data from backup")

    cleanup_processes()
    delete_pipeline_status_file()
    sys.exit(0)

# Register cleanup handlers
atexit.register(cleanup_processes)
signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)
if os.name == 'nt' and hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)

def start_servers(base_dir):
    global child_processes
    print("\n[INFO] Starting Web Servers...")
    processes = []
    
    # Flask
    flask_script = base_dir / "server_react.py"
    if os.name == 'nt':
        p = subprocess.Popen(
            ["cmd", "/k", "python", str(flask_script)],
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        processes.append(p)
        child_processes.append(p)
    else:
        p = subprocess.Popen([sys.executable, str(flask_script)])
        processes.append(p)
        child_processes.append(p)

    # React
    frontend_dir = base_dir / "frontend"
    if frontend_dir.exists():
        # Check if npm is available
        try:
            subprocess.run(["npm", "--version"], capture_output=True, check=True, shell=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("[WARN] npm is not installed or not in PATH. Skipping frontend server.")
            print("[WARN] Please install Node.js from https://nodejs.org/ to enable the web UI.")
            return processes
        
        # Check node_modules
        if not (frontend_dir / "node_modules").exists():
            print("[INFO] Installing frontend dependencies...")
            try:
                subprocess.check_call(["npm", "install"], cwd=frontend_dir, shell=True)
            except subprocess.CalledProcessError as e:
                print(f"[WARN] Failed to install frontend dependencies: {e}")
                print("[WARN] Frontend server will not be started.")
                return processes

        if os.name == 'nt':
            try:
                p = subprocess.Popen(
                    ["cmd", "/k", "npm", "run", "dev"],
                    cwd=str(frontend_dir),
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )
                processes.append(p)
                child_processes.append(p)
            except Exception as e:
                print(f"[WARN] Failed to start frontend server: {e}")
        else:
            try:
                p = subprocess.Popen(["npm", "run", "dev"], cwd=frontend_dir)
                processes.append(p)
                child_processes.append(p)
            except Exception as e:
                print(f"[WARN] Failed to start frontend server: {e}")
    else:
        print(f"[WARN] Frontend directory not found: {frontend_dir}")
    
    return processes


# --- Pipeline observation support (queued + stable step ids) ---

_CURRENT_RUN_ID = None  # type: ignore
_STEP_COUNTER = 0
_DESC_TO_STEP_ID = {}  # type: ignore
_CURRENT_INDEX = None  # type: ignore


def _new_run_id() -> str:
    return f"run-{int(time.time() * 1000)}"


def _reset_step_plan(run_id: str) -> None:
    global _CURRENT_RUN_ID, _STEP_COUNTER, _DESC_TO_STEP_ID, _CURRENT_INDEX
    _CURRENT_RUN_ID = str(run_id)
    _STEP_COUNTER = 0
    _DESC_TO_STEP_ID = {}
    _CURRENT_INDEX = None


def set_current_index(index: int) -> None:
    """Set the current run index and backfill it into already-planned steps for this run."""
    global _CURRENT_INDEX
    try:
        _CURRENT_INDEX = int(index)
    except Exception:
        _CURRENT_INDEX = None

    # Backfill on-disk status so early steps (sync/fetch) can be grouped.
    try:
        update_pipeline_status(
            'working',
            f'Processing index {index}',
            run_id=_CURRENT_RUN_ID,
            step={
                'phase': 'meta',
                'run_index': _CURRENT_INDEX,
            },
        )
    except Exception:
        pass


def _alloc_step_id() -> str:
    global _STEP_COUNTER
    _STEP_COUNTER += 1
    rid = str(_CURRENT_RUN_ID or 'run')
    return f"{rid}:{_STEP_COUNTER:04d}"


def plan_step(
    description: str,
    command: str | None = None,
    *,
    input_files: list[str] | None = None,
    output_files: list[str] | None = None,
) -> str:
    """Register a step as queued (best-effort) and return its stable id."""
    desc = str(description or '').strip()
    if not desc:
        return ''
    global _DESC_TO_STEP_ID
    sid = _DESC_TO_STEP_ID.get(desc)
    if not sid:
        sid = _alloc_step_id()
        _DESC_TO_STEP_ID[desc] = sid

    try:
        update_pipeline_status(
            'working',
            desc,
            run_id=_CURRENT_RUN_ID,
            step={
                'id': sid,
                'phase': 'plan',
                'name': desc,
                'status': 'queued',
                **({'index': _CURRENT_INDEX} if _CURRENT_INDEX is not None else {}),
                **({'command': command} if command else {}),
                **({'input_files': input_files} if input_files else {}),
                **({'output_files': output_files} if output_files else {}),
            },
        )
    except Exception:
        pass
    return sid

def update_pipeline_status(state, message=None, next_run=None, *, run_id=None, step=None, clear_steps=False):
    """Update pipeline_status.json (best-effort).

    Backward compatible keys:
    - state
    - message
    - next_run

    Extended keys (optional):
    - last_updated
    - run_id
    - current_step_id
    - steps: [{id,name,status,started_at,ended_at,command,exit_code,error}]
    """
    with _STATUS_LOCK:
        _update_pipeline_status_locked(state, message, next_run, run_id=run_id, step=step, clear_steps=clear_steps)


def _update_pipeline_status_locked(state, message=None, next_run=None, *, run_id=None, step=None, clear_steps=False):
    """Inner implementation — must be called while holding _STATUS_LOCK."""

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Load existing status to preserve step history (best-effort).
    existing = {}
    try:
        if os.path.exists(PIPELINE_STATUS_FILE):
            with open(PIPELINE_STATUS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                existing = loaded
    except Exception:
        existing = {}

    def _observation_snapshots_dir() -> Path:
        try:
            d = PATHS.incremental_data_dir() / OBSERVATION_SNAPSHOT_DIRNAME
            d.mkdir(parents=True, exist_ok=True)
            return d
        except Exception:
            # Best-effort fallback.
            d = Path("incremental_data") / OBSERVATION_SNAPSHOT_DIRNAME
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            return d

    def _save_observation_snapshot(snapshot: dict, *, reason: str) -> None:
        try:
            d = _observation_snapshots_dir()
            run_id_for_file = _safe_filename_part(snapshot.get('run_id'), fallback='run')
            ts = _safe_filename_part(snapshot.get('last_updated') or now_iso, fallback=str(int(time.time())))
            path = d / f"observation_{ts}_{run_id_for_file}.json"
            payload = {
                **snapshot,
                'saved_at': now_iso,
                'save_reason': reason,
            }
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(payload, f)
        except Exception:
            pass

    steps = []
    if not clear_steps:
        prev_steps = existing.get('steps')
        if isinstance(prev_steps, list):
            steps = [x for x in prev_steps if isinstance(x, dict)]

    status = {
        **({} if not isinstance(existing, dict) else existing),
        "state": state,
        "message": message,
        "last_updated": now_iso,
        "next_run": next_run,
    }

    if run_id:
        status['run_id'] = str(run_id)

    # Step event handling
    if isinstance(step, dict):
        def _normalize_file_list(v: object) -> list[str] | None:
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

        def _normalize_before_files(v: object) -> dict[str, str] | None:
            if v is None:
                return None
            if isinstance(v, dict):
                out: dict[str, str] = {}
                for k, val in v.items():
                    ks = str(k).strip()
                    vs = str(val).strip()
                    if ks and vs:
                        out[ks] = vs
                return out if out else None
            return None

        def _apply_step_files(dst: dict, src: dict) -> None:
            in_files = _normalize_file_list(src.get('input_files'))
            out_files = _normalize_file_list(src.get('output_files'))
            before_files = _normalize_before_files(src.get('before_files'))
            if in_files is not None:
                dst['input_files'] = in_files
            if out_files is not None:
                dst['output_files'] = out_files
            if before_files is not None:
                dst['before_files'] = before_files

        sid = step.get('id')
        if sid:
            sid = str(sid)
        phase = str(step.get('phase') or '').strip().lower()

        # Run-level metadata update (no step id required)
        if phase == 'meta':
            try:
                run_index = step.get('run_index')
                if run_index is not None:
                    status['current_index'] = run_index

                    # Backfill index onto steps belonging to this run.
                    rid = str(status.get('run_id') or '')
                    if rid:
                        prefix = f"{rid}:"
                        for rec in steps:
                            if not isinstance(rec, dict):
                                continue
                            if str(rec.get('id') or '').startswith(prefix):
                                if rec.get('index') is None:
                                    rec['index'] = run_index
            except Exception:
                pass
            # Continue to write the file even if meta handling fails.

        def find_step_index(step_id: str) -> int | None:
            for i, rec in enumerate(steps):
                if str(rec.get('id')) == step_id:
                    return i
            return None

        if sid and phase in ('plan', 'queued'):
            existing_idx = find_step_index(sid)
            rec = steps[existing_idx] if existing_idx is not None else {}
            if not isinstance(rec, dict):
                rec = {}
            rec['id'] = sid
            rec['name'] = str(step.get('name') or rec.get('name') or '')
            rec['status'] = str(step.get('status') or rec.get('status') or 'queued')
            _apply_step_files(rec, step)
            if step.get('index') is not None:
                rec['index'] = step.get('index')
            if step.get('command'):
                rec['command'] = str(step.get('command'))
            if existing_idx is None:
                steps.append(rec)
            else:
                steps[existing_idx] = rec

        if sid and phase == 'start':
            # Update queued record in-place if present; otherwise append.
            existing_idx = find_step_index(sid)
            prev = steps[existing_idx] if existing_idx is not None else {}
            if not isinstance(prev, dict):
                prev = {}
            rec = {
                **prev,
                'id': sid,
                'name': str(step.get('name') or prev.get('name') or ''),
                'status': 'running',
                'started_at': str(step.get('started_at') or now_iso),
            }
            _apply_step_files(rec, step)
            if step.get('index') is not None:
                rec['index'] = step.get('index')
            cmd = step.get('command')
            if cmd:
                rec['command'] = str(cmd)
            if existing_idx is None:
                steps.append(rec)
            else:
                steps[existing_idx] = rec
            status['current_step_id'] = sid
        elif sid and phase == 'end':
            # Update existing record if present; otherwise append a minimal record.
            updated = False
            for rec in reversed(steps):
                if str(rec.get('id')) == sid:
                    rec['ended_at'] = str(step.get('ended_at') or now_iso)
                    rec['status'] = str(step.get('status') or rec.get('status') or 'ok')
                    _apply_step_files(rec, step)
                    if step.get('index') is not None:
                        rec['index'] = step.get('index')
                    if step.get('exit_code') is not None:
                        rec['exit_code'] = step.get('exit_code')
                    if step.get('error'):
                        rec['error'] = str(step.get('error'))
                    updated = True
                    break
            if not updated:
                rec = {
                    'id': sid,
                    'name': str(step.get('name') or ''),
                    'status': str(step.get('status') or 'ok'),
                    'started_at': str(step.get('started_at') or now_iso),
                    'ended_at': str(step.get('ended_at') or now_iso),
                    **({'exit_code': step.get('exit_code')} if step.get('exit_code') is not None else {}),
                    **({'error': str(step.get('error'))} if step.get('error') else {}),
                }
                _apply_step_files(rec, step)
                steps.append(rec)
            # If the current step ended, clear pointer.
            if str(status.get('current_step_id') or '') == sid:
                status['current_step_id'] = ''

    # Keep the last N steps to avoid unbounded growth.
    MAX_STEPS = 200
    if len(steps) > MAX_STEPS:
        steps = steps[-MAX_STEPS:]
    status['steps'] = steps

    # If we just completed a working run, persist the Observation data.
    try:
        prev_state = str((existing or {}).get('state') or '').strip().lower()
        curr_state = str(state or '').strip().lower()
        if prev_state == 'working' and curr_state in {'sleeping', 'offline'}:
            _save_observation_snapshot(status, reason=f"{prev_state}->{curr_state}")
    except Exception:
        pass

    try:
        # Keep compatibility with server_react.py, which reads this file from CWD.
        try:
            Path(str(PIPELINE_STATUS_FILE)).parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        with open(PIPELINE_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f)
    except Exception as e:
        print(f"[WARN] Failed to update pipeline status: {e}")

def get_user_info(profile_path):
    p = Path(profile_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Dataset user_profile.json not found: {p}")
    with open(p, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"user_profile.json is not a JSON object: {p}")

    raw_alias = data.get("USER_ALIAS")
    raw_email = data.get("USER_EMAIL")
    alias = (raw_alias[0] if isinstance(raw_alias, list) and raw_alias else (raw_alias if isinstance(raw_alias, str) else '')).strip()
    email = (raw_email[0] if isinstance(raw_email, list) and raw_email else (raw_email if isinstance(raw_email, str) else '')).strip()
    if not alias:
        raise ValueError(f"Missing USER_ALIAS in {p}")
    if not email:
        raise ValueError(f"Missing USER_EMAIL in {p}")
    return alias, email

def update_log_stats(log_file, index, stats):
    """Updates the fetch log with processing stats."""
    if not os.path.exists(log_file):
        return

    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = json.load(f)
        
        updated = False
        for entry in logs:
            if entry.get('index') == index:
                entry.update(stats)
                updated = True
                break
        
        if updated:
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, indent=2)
            print(f"[INFO] Updated log {log_file} for index {index}")
    except Exception as e:
        print(f"[WARN] Failed to update log {log_file}: {e}")

def run_command(command, cwd=None, description=None, *, input_files=None, output_files=None, env: dict[str, str] | None = None, tag: str | None = None):
    """Runs a command and prints status.

    tag — optional prefix (e.g. "[Worker-1]") added to every console line
          produced by this call, including subprocess stdout/stderr.
    """
    # Extract a tag from the description if not explicitly provided.
    # Convention: description contains "[Worker-N]" at the end.
    if tag is None and description:
        import re as _re
        _m = _re.search(r'\[Worker-(\d+)\]', description)
        if _m:
            wid = int(_m.group(1))
            color = _worker_color(wid)
            tag = f"{color}[Worker-{wid}]{RESET} "
    _pfx = tag or ''

    if description:
        print(f"\n{_pfx}[STEP] {description}")

    # Emit step start (best-effort) so the UI can observe pipeline progress.
    step_id = None
    before_files: dict[str, str] = {}
    try:
        if description:
            # Prefer a planned step id if available, so queued->running transitions are stable.
            desc_key = str(description)
            global _DESC_TO_STEP_ID
            step_id = _DESC_TO_STEP_ID.get(desc_key) or f"{int(time.time() * 1000)}"
            before_files = _capture_before_outputs(
                step_id=step_id,
                output_files=output_files,
                cwd=cwd,
                run_id=_CURRENT_RUN_ID,
            )
            cmd_str = ' '.join(command) if isinstance(command, list) else str(command)
            update_pipeline_status(
                "working",
                description,
                run_id=_CURRENT_RUN_ID,
                step={
                    'id': step_id,
                    'phase': 'start',
                    'name': description,
                    'command': cmd_str,
                    **({'index': _CURRENT_INDEX} if _CURRENT_INDEX is not None else {}),
                    **({'input_files': input_files} if input_files else {}),
                    **({'output_files': output_files} if output_files else {}),
                    **({'before_files': before_files} if before_files else {}),
                    'started_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                },
            )
    except Exception:
        pass
    
    cmd_str = ' '.join(command) if isinstance(command, list) else command
    print(f"{_pfx}Running: {cmd_str}")
    
    try:
        effective_env = None
        try:
            base_env = _RUN_COMMAND_BASE_ENV
            if base_env or env:
                effective_env = os.environ.copy()
                if base_env:
                    for k, v in base_env.items():
                        if v is None:
                            continue
                        effective_env[str(k)] = str(v)
                if env:
                    for k, v in env.items():
                        if v is None:
                            continue
                        effective_env[str(k)] = str(v)
        except Exception:
            effective_env = None

        if _pfx:
            # Run with Popen so we can prefix each output line with the worker tag.
            proc = subprocess.Popen(
                command,
                cwd=cwd,
                env=effective_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors='replace',
            )
            for line in proc.stdout:  # type: ignore[union-attr]
                print(f"{_pfx}{line}", end='')
            returncode = proc.wait()
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, command)
        else:
            subprocess.check_call(command, cwd=cwd, env=effective_env)
        print(f"{_pfx}[OK] Success")

        try:
            if step_id:
                update_pipeline_status(
                    "working",
                    description,
                    run_id=_CURRENT_RUN_ID,
                    step={
                        'id': step_id,
                        'phase': 'end',
                        'name': description,
                        'status': 'ok',
                        **({'index': _CURRENT_INDEX} if _CURRENT_INDEX is not None else {}),
                        **({'input_files': input_files} if input_files else {}),
                        **({'output_files': output_files} if output_files else {}),
                        **({'before_files': before_files} if before_files else {}),
                        'ended_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                        'exit_code': 0,
                    },
                )
        except Exception:
            pass
        return True
    except subprocess.CalledProcessError as e:
        print(f"{_pfx}[ERROR] Command failed with exit code {e.returncode}")

        try:
            if step_id:
                update_pipeline_status(
                    "working",
                    f"{description}: error",
                    run_id=_CURRENT_RUN_ID,
                    step={
                        'id': step_id,
                        'phase': 'end',
                        'name': description,
                        'status': 'error',
                        **({'index': _CURRENT_INDEX} if _CURRENT_INDEX is not None else {}),
                        **({'input_files': input_files} if input_files else {}),
                        **({'output_files': output_files} if output_files else {}),
                        **({'before_files': before_files} if before_files else {}),
                        'ended_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                        'exit_code': e.returncode,
                        'error': f"Command failed with exit code {e.returncode}",
                    },
                )
        except Exception:
            pass
        return False


def maybe_sync_topics(base_dir: Path) -> None:
    """Best-effort: sync topics.json + user_topics.json into user_profile.json.following.

    Intentionally non-blocking so the pipeline can still run.
    """
    try:
        topics_path = base_dir / TOPICS_FILE
        user_topics_path = base_dir / USER_TOPICS_FILE
        profile_path = base_dir / USER_PROFILE_FILE
        script_path = base_dir / SYNC_TOPICS_SCRIPT

        if not script_path.exists():
            return
        if not topics_path.exists():
            return
        if not profile_path.exists():
            return

        run_command(
            [
                sys.executable,
                str(script_path),
                "--topics",
                str(topics_path),
                "--user-topics",
                str(user_topics_path),
                "--profile",
                str(profile_path),
            ],
            cwd=base_dir,
            description="Syncing topics into user_profile.following",
            input_files=[str(topics_path), str(user_topics_path), str(profile_path)],
            output_files=[str(profile_path)],
        )
    except Exception as e:
        print(f"[WARN] Failed to sync topics into user_profile.following: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return False

def find_latest_files(incremental_dir):
    """Finds the latest emails and teams files based on the highest index."""
    path = Path(incremental_dir)
    if not path.exists():
        return None, None, 0

    max_index = 0
    latest_emails = None
    latest_teams = None
    
    # Regex to match filenames like emails_1.json or emails_1_20251128_073725.json
    # Supports both old (timestamped) and new (clean) formats
    pattern = re.compile(r"^(emails|teams)_(\d+)(?:_\d{8}_\d{6})?\.json$")
    
    files_to_check = []
    
    outlook_dir = path / "outlook"
    if outlook_dir.exists():
        files_to_check.extend(outlook_dir.glob("emails_*.json"))
        
    teams_dir = path / "teams"
    if teams_dir.exists():
        files_to_check.extend(teams_dir.glob("teams_*.json"))
    
    for file_path in files_to_check:
        match = pattern.match(file_path.name)
        if match:
            file_type = match.group(1)
            index = int(match.group(2))
            
            if index > max_index:
                max_index = index
                latest_emails = None
                latest_teams = None
            
            if index == max_index:
                if file_type == "emails":
                    latest_emails = file_path
                elif file_type == "teams":
                    latest_teams = file_path
                    
    return latest_emails, latest_teams, max_index


# ---------------------------------------------------------------------------
# Outlook parallel-worker helpers
# ---------------------------------------------------------------------------

_OUTLOOK_NUM_WORKERS = 3  # default; overridden by config outlook_parallel_workers


def _split_threads_file(threads_file: Path, index: int, num_workers: int) -> list[Path]:
    """Split a threads JSON array into *num_workers* chunk files.

    Returns a list of chunk file paths (may be fewer than *num_workers* if the
    input contains fewer threads than workers).
    """
    with open(threads_file, 'r', encoding='utf-8') as f:
        threads = json.load(f)
    if not isinstance(threads, list) or len(threads) == 0:
        return []
    chunk_size = math.ceil(len(threads) / num_workers)
    chunk_files: list[Path] = []
    for i in range(num_workers):
        chunk = threads[i * chunk_size : (i + 1) * chunk_size]
        if not chunk:
            break
        chunk_path = threads_file.parent / f"threads_{index}_chunk{i + 1}.json"
        with open(chunk_path, 'w', encoding='utf-8') as f:
            json.dump(chunk, f)
        chunk_files.append(chunk_path)
    return chunk_files


def _merge_outlook_snapshots(snapshot_files: list[Path], output: Path) -> None:
    """Merge multiple worker event snapshots into a single snapshot file."""
    all_events: list[dict] = []
    for sf in snapshot_files:
        if not sf.exists():
            continue
        try:
            with open(sf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            all_events.extend(data.get('events', []))
        except Exception as exc:
            print(f"[WARN] Failed to read worker snapshot {sf.name}: {exc}")
    with open(output, 'w', encoding='utf-8') as f:
        json.dump({"events": all_events}, f, indent=2)
    print(f"[INFO] Merged {len(all_events)} events from {len(snapshot_files)} workers into {output.name}")


def _outlook_worker(
    *,
    worker_id: int,
    chunk_file: Path,
    snapshot_file: Path,
    sub_index: int,
    existing_events_arg: list[str],
    base_dir: Path,
    recent_focus_file: Path,
    outlook_event_timeout_seconds: float,
) -> Path | None:
    """Run the per-worker portion of the Outlook AI chain.

    Steps: Extract Events → Extract Actions → Validate Actions
    Returns the worker snapshot path on success, or None on total failure.
    """
    worker_tag = f"[Worker-{worker_id}]"

    # --- Extract Events ---
    cmd_events = [
        sys.executable, "-m", "outlook_v2.ai_extract_events",
        "--input", str(chunk_file),
        "--output", str(snapshot_file),
        "--user-profile", str(base_dir / USER_PROFILE_FILE),
        "--recent-focus", str(recent_focus_file),
        "--guide", str(base_dir / EVENT_GUIDE),
        "--relationship-guide", str(base_dir / RELATIONSHIP_GUIDE),
        "--index", str(sub_index),
    ] + (existing_events_arg if worker_id == 1 else [])

    events_ok = run_command(
        cmd_events,
        cwd=base_dir,
        description=f"Extracting Outlook Events {worker_tag}",
        env={'AZURE_OPENAI_TIMEOUT_SECONDS': str(outlook_event_timeout_seconds)},
    )
    if not events_ok and not snapshot_file.exists():
        return None
    if not events_ok:
        print(f"[WARN] {worker_tag} Event extraction had failures — continuing with partial snapshot")

    # --- Extract Actions ---
    cmd_actions = [
        sys.executable, "-m", "outlook_v2.ai_extract_actions",
        "--input", str(snapshot_file),
        "--threads", str(chunk_file),
        "--user-profile", str(base_dir / USER_PROFILE_FILE),
        "--recent-focus", str(recent_focus_file),
        "--guide", str(base_dir / ACTION_GUIDE),
    ]
    run_command(cmd_actions, cwd=base_dir, description=f"Extracting Outlook Actions {worker_tag}")

    # --- Validate Actions ---
    cmd_validate = [
        sys.executable, "-m", "outlook_v2.ai_validate_actions",
        "--input", str(snapshot_file),
        "--output", str(snapshot_file),
        "--threads", str(chunk_file),
        "--user-profile", str(base_dir / USER_PROFILE_FILE),
        "--recent-focus", str(recent_focus_file),
        "--guide", str(base_dir / VALIDATION_GUIDE),
    ]
    run_command(cmd_validate, cwd=base_dir, description=f"Validating Outlook Actions {worker_tag}")

    return snapshot_file if snapshot_file.exists() else None


def _cleanup_worker_files(chunk_files: list[Path], snapshot_files: list[Path]) -> None:
    """Remove temporary chunk and worker snapshot files (best-effort)."""
    for f in [*chunk_files, *snapshot_files]:
        try:
            if f.exists():
                f.unlink()
        except Exception:
            pass


def run_pipeline(open_browser=False, enable_dedup=False, skip_fetch=False):
    run_id = _new_run_id()
    base_dir = _pipeline_base_dir()

    # Ensure pipeline_config.json exists and apply dataset folder selection
    # before emitting any step plans / path references.
    cfg = {}
    try:
        loaded = ensure_effective_config(base_dir)
        if isinstance(loaded, dict):
            cfg = loaded
    except Exception:
        pass
    _configure_active_data_folder(cfg, base_dir=base_dir)
    outlook_event_timeout_seconds = _configure_pipeline_subprocess_env(cfg)

    # Read concurrency worker count from config (1-5, default 3)
    global _OUTLOOK_NUM_WORKERS
    try:
        _w = int(cfg.get('outlook_parallel_workers', 3))
        _OUTLOOK_NUM_WORKERS = max(1, min(5, _w))
    except (ValueError, TypeError):
        _OUTLOOK_NUM_WORKERS = 3
    print(f"[CONFIG] Outlook parallel workers: {_OUTLOOK_NUM_WORKERS}")

    _reset_step_plan(run_id)

    # Seed an initial plan so the UI can show queued steps immediately.
    plan_step(
        "Syncing topics into user_profile.following",
        input_files=[str(Path(TOPICS_FILE)), str(Path(USER_TOPICS_FILE)), str(Path(USER_PROFILE_FILE))],
        output_files=[str(Path(USER_PROFILE_FILE))],
    )
    plan_step(
        "Running Incremental Fetch",
        input_files=[str(Path(USER_PROFILE_FILE))],
        output_files=[str(Path(INCREMENTAL_DATA_DIR) / "outlook"), str(Path(INCREMENTAL_DATA_DIR) / "teams")],
    )

    pipeline_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 0. Sync topics into profile before running anything else
    maybe_sync_topics(base_dir)
    
    # Load User Info
    try:
        user_alias, user_email = get_user_info(base_dir / USER_PROFILE_FILE)
    except Exception as e:
        msg = f"Missing/invalid dataset user_profile.json: {e}"
        print(f"[ERROR] {msg}")
        try:
            update_pipeline_status('offline', msg, run_id=run_id)
        except Exception:
            pass
        return
    
    # 1. Run Incremental Fetch (skip if reset just ran)
    if not skip_fetch:
        fetch_script = base_dir / "pipeline" / "incremental_fetch.py"
        if not fetch_script.exists():
            print(f"[ERROR] Fetch script not found: {fetch_script}")
            return
            
        if not run_command(
            [sys.executable, str(fetch_script)],
            cwd=base_dir,
            description="Running Incremental Fetch",
            input_files=[str(base_dir / USER_PROFILE_FILE)],
            output_files=[str(base_dir / INCREMENTAL_DATA_DIR / "outlook"), str(base_dir / INCREMENTAL_DATA_DIR / "teams")],
        ):
            return
    else:
        print("\n[INFO] Skipping incremental fetch (using reset data)")

    # 2. Identify New Data Files
    incremental_dir = base_dir / INCREMENTAL_DATA_DIR
    latest_emails, latest_teams, index = find_latest_files(incremental_dir)

    # Update the Incremental Fetch step to reference the concrete output files
    # (emails_<index>.json / teams_<index>.json) rather than only the parent folders.
    # NOTE: the step id mapping is in-memory; to be robust we also scan the on-disk
    # pipeline_status.json for the most recent matching step record.
    try:
        fetch_desc = "Running Incremental Fetch"

        out_files: list[str] = []
        if latest_emails and getattr(latest_emails, 'exists', None) and latest_emails.exists():
            out_files.append(str(Path(latest_emails).resolve()))
        if latest_teams and getattr(latest_teams, 'exists', None) and latest_teams.exists():
            out_files.append(str(Path(latest_teams).resolve()))

        def _find_step_record_by_name(step_name: str) -> dict | None:
            try:
                if not os.path.exists(PIPELINE_STATUS_FILE):
                    return None
                with open(PIPELINE_STATUS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                steps = data.get('steps') if isinstance(data, dict) else None
                if not isinstance(steps, list):
                    return None
                for rec in reversed(steps):
                    if not isinstance(rec, dict):
                        continue
                    if str(rec.get('name') or '') == step_name:
                        return rec
                return None
            except Exception:
                return None

        rec = _find_step_record_by_name(fetch_desc)

        global _DESC_TO_STEP_ID
        fetch_step_id = _DESC_TO_STEP_ID.get(fetch_desc) or (str(rec.get('id')) if isinstance(rec, dict) and rec.get('id') else None)

        if fetch_step_id and out_files:
            step_update = {
                'id': fetch_step_id,
                'phase': 'end',
                'name': fetch_desc,
                'output_files': out_files,
            }
            # Preserve existing status/timestamps if we can read them.
            if isinstance(rec, dict):
                if rec.get('status'):
                    step_update['status'] = rec.get('status')
                if rec.get('ended_at'):
                    step_update['ended_at'] = rec.get('ended_at')
                if rec.get('exit_code') is not None:
                    step_update['exit_code'] = rec.get('exit_code')
                if rec.get('index') is not None:
                    step_update['index'] = rec.get('index')
            update_pipeline_status(
                'working',
                fetch_desc,
                run_id=_CURRENT_RUN_ID,
                step=step_update,
            )
    except Exception:
        pass
    
    if index == 0 and not (latest_emails or latest_teams):
        print("[INFO] No incremental data found.")
        return
        
    print(f"\n[INFO] Processing data for Index {index}")

    # Attach index to this run for UI grouping.
    try:
        set_current_index(index)
    except Exception:
        pass

    # Plan remaining steps now that we know index and inputs (best-effort).
    if latest_emails and latest_emails.exists():
        plan_step(f"Processing Email Threads (Index {index})")
        plan_step("Merging Email Threads")
        for _w in range(1, _OUTLOOK_NUM_WORKERS + 1):
            plan_step(f"Extracting Outlook Events [Worker-{_w}]")
            plan_step(f"Extracting Outlook Actions [Worker-{_w}]")
            plan_step(f"Validating Outlook Actions [Worker-{_w}]")
        if enable_dedup:
            plan_step("Deduplicating Outlook Events")
        plan_step("Deduplicating Outlook Todos")
        plan_step("Preparing Briefing Data")

    if latest_teams and latest_teams.exists():
        plan_step(f"Processing Teams Messages (Index {index})")
        plan_step("Merging Teams Conversations")
        plan_step("Analyzing Teams Conversations")
        plan_step("Deduplicating Teams Todos")
        plan_step("Preparing Briefing Data")

    # Prune step is planned later once prune_days_ago is known.
    
    # 3. Process Emails (Threads)
    temp_threads_file = None
    if latest_emails and latest_emails.exists():
        threads_script = base_dir / Path("outlook_v2") / "process_threads.py"
        if threads_script.exists():
            temp_threads_file = incremental_dir / "outlook" / f"temp_threads_{index}.json"
            
            # Load max_threads from config
            max_threads = cfg.get("max_threads") if isinstance(cfg, dict) else None
            
            cmd = [
                sys.executable,
                str(threads_script),
                "--input", str(latest_emails),
                "--output", str(temp_threads_file)
            ]
            
            if max_threads:
                cmd.extend(["--max-threads", str(max_threads)])
            
            run_command(
                cmd,
                cwd=base_dir,
                description=f"Processing Email Threads (Index {index})",
                input_files=[str(latest_emails)],
                output_files=[str(temp_threads_file)],
            )
        else:
            print(f"[WARN] Process threads script not found: {threads_script}")
    else:
        print(f"[INFO] No emails file found for index {index}")

    # 4. Process Teams Messages
    temp_teams_dir = None
    if latest_teams and latest_teams.exists():
        teams_script = base_dir / Path("teams") / "process_teams_messages.py"
        if teams_script.exists():
            # Create a temp output folder for this run's teams conversations
            temp_teams_dir = incremental_dir / "teams" / f"temp_teams_conversations_{index}"
            
            # Load max_conversations from config
            max_conversations = cfg.get("max_conversations") if isinstance(cfg, dict) else None
            
            cmd = [
                sys.executable,
                str(teams_script),
                str(latest_teams),
                str(temp_teams_dir)
            ]
            
            if max_conversations:
                cmd.extend(["--max-conversations", str(max_conversations)])
            
            run_command(
                cmd,
                cwd=base_dir,
                description=f"Processing Teams Messages (Index {index})",
                input_files=[str(latest_teams)],
                output_files=[str(temp_teams_dir)],
            )
        else:
            print(f"[WARN] Process teams script not found: {teams_script}")
    else:
        print(f"[INFO] No teams file found for index {index}")

    # 5. Merge Data (Identify New/Updated)
    merge_script = base_dir / "pipeline" / "merge_incremental_data.py"
    apply_filters_script = base_dir / "pipeline" / "apply_filters_to_existing_data.py"

    if merge_script.exists():
        # Merge Threads
        if temp_threads_file and temp_threads_file.exists():
            final_threads_file = incremental_dir / "outlook" / f"threads_{index}.json"
            
            cmd = [
                sys.executable,
                str(merge_script),
                "--new-threads", str(temp_threads_file),
                "--master-threads", str(base_dir / MASTER_THREADS_FILE),
                "--output-threads", str(final_threads_file)
            ]
            if run_command(
                cmd,
                cwd=base_dir,
                description="Merging Email Threads",
                input_files=[str(temp_threads_file), str(base_dir / MASTER_THREADS_FILE)],
                output_files=[str(final_threads_file)],
            ):
                # Keep temp_threads_file for Observation review.
                pass
        
        # Apply Filters to Master Data (Post-Merge Cleanup)
        if apply_filters_script.exists():
             run_command(
                 [sys.executable, str(apply_filters_script)],
                 cwd=base_dir,
                 description="Applying Filters to Master Data",
                 input_files=[str(base_dir / MASTER_THREADS_FILE), str(base_dir / MASTER_TEAMS_DIR)],
                 output_files=[str(base_dir / MASTER_THREADS_FILE), str(base_dir / MASTER_TEAMS_DIR)],
             )

        # Merge Teams
        if temp_teams_dir and temp_teams_dir.exists():
            final_teams_dir = incremental_dir / "teams" / f"teams_conversations_{index}"
            
            cmd = [
                sys.executable,
                str(merge_script),
                "--new-teams-dir", str(temp_teams_dir),
                "--master-teams-dir", str(base_dir / MASTER_TEAMS_DIR),
                "--output-teams-dir", str(final_teams_dir)
            ]
            if run_command(
                cmd,
                cwd=base_dir,
                description="Merging Teams Conversations",
                input_files=[str(temp_teams_dir), str(base_dir / MASTER_TEAMS_DIR)],
                output_files=[str(final_teams_dir)],
            ):
                # Keep temp_teams_dir for Observation review.
                pass
    else:
        print(f"[WARN] Merge script not found: {merge_script}")

    # 5b. Prune old master data BEFORE AI analysis (so it runs before AI client initializes)
    prune_days_ago = 3
    try:
        if isinstance(cfg, dict):
            prune_days_ago = int(cfg.get("prune_days_ago", prune_days_ago))
    except Exception as e:
        print(f"[WARN] Failed to read prune_days_ago from config: {e}")

    prune_script = base_dir / "pipeline" / "prune_old_data.py"
    if prune_script.exists():
        plan_step(f"Pruning master data older than {prune_days_ago} days")
        run_command(
            [
                sys.executable,
                str(prune_script),
                "--incremental-dir",
                str(base_dir / INCREMENTAL_DATA_DIR),
                "--user-alias",
                user_alias,
                "--days",
                str(prune_days_ago),
            ],
            cwd=base_dir,
            description=f"Pruning master data older than {prune_days_ago} days",
            input_files=[str(base_dir / INCREMENTAL_DATA_DIR)],
            output_files=[str(base_dir / PRUNE_LOG_FILE)],
        )
        print(_yellow(f"[INFO] Prune log: {PRUNE_LOG_FILE}"))

        # Quick visibility: show counts after prune
        try:
            master_threads_path = base_dir / MASTER_THREADS_FILE
            if master_threads_path.exists():
                with open(master_threads_path, 'r', encoding='utf-8') as f:
                    threads_after = json.load(f)
                if isinstance(threads_after, list):
                        print(_yellow(f"[INFO] Master threads after prune: {len(threads_after)}"))
        except Exception as e:
            print(f"[WARN] Could not read master threads after prune: {e}")

        try:
            master_teams_dir_path = base_dir / MASTER_TEAMS_DIR
            if master_teams_dir_path.exists():
                conv_files = list(Path(master_teams_dir_path).glob('conversation_*.json'))
                print(_yellow(f"[INFO] Master teams conversations after prune: {len(conv_files)} files"))
        except Exception as e:
            print(f"[WARN] Could not count master teams conversations after prune: {e}")

        # Also prune the current run's AI input artifacts so AI doesn't process already-obsolete data.
        try:
            delta_threads_path = incremental_dir / "outlook" / f"threads_{index}.json"
            if delta_threads_path.exists():
                run_command(
                    [
                        sys.executable,
                        str(prune_script),
                        "--incremental-dir",
                        str(base_dir / INCREMENTAL_DATA_DIR),
                        "--user-alias",
                        user_alias,
                        "--days",
                        str(prune_days_ago),
                        "--threads-file",
                        str(delta_threads_path),
                        "--no-log",
                    ],
                    cwd=base_dir,
                    description="Pruning delta threads input (pre-AI)",
                )
        except Exception as e:
            print(f"[WARN] Failed to prune delta threads input: {e}")

        try:
            delta_teams_dir = incremental_dir / "teams" / f"teams_conversations_{index}"
            if delta_teams_dir.exists():
                run_command(
                    [
                        sys.executable,
                        str(prune_script),
                        "--incremental-dir",
                        str(base_dir / INCREMENTAL_DATA_DIR),
                        "--user-alias",
                        user_alias,
                        "--days",
                        str(prune_days_ago),
                        "--teams-dir",
                        str(delta_teams_dir),
                        "--no-log",
                    ],
                    cwd=base_dir,
                    description="Pruning delta Teams conversations input (pre-AI)",
                )
        except Exception as e:
            print(f"[WARN] Failed to prune delta Teams conversations input: {e}")
    else:
        print(f"[WARN] Prune script not found: {prune_script}")

    # 6. AI Analysis (Outlook) — parallel workers
    final_threads_file = incremental_dir / "outlook" / f"threads_{index}.json"
    if final_threads_file.exists():
        recent_focus_file = incremental_dir / "output" / "recent_focus.json"
        # Define paths
        delta_events_file = incremental_dir / "outlook" / f"outlook_events_{user_alias}_{index}.json"
        temp_snapshot_file = incremental_dir / "outlook" / f"temp_snapshot_{index}.json"
        master_events_file = incremental_dir / "outlook" / f"master_outlook_events_{user_alias}.json"
        
        # Prepare existing events argument (Master)
        existing_events_arg: list[str] = []
        if master_events_file.exists():
            existing_events_arg = ["--existing-events", str(master_events_file)]
            print(f"[INFO] Using Master Outlook Events: {master_events_file.name}")
        else:
            # Fallback: Try to find previous run's file if master doesn't exist (Migration)
            if index > 1:
                prev_events_file = incremental_dir / "outlook" / f"outlook_events_{user_alias}_{index-1}.json"
                if prev_events_file.exists():
                    existing_events_arg = ["--existing-events", str(prev_events_file)]
                    print(f"[INFO] Master events not found. Using previous run snapshot: {prev_events_file.name}")

        # Decide whether to use parallel workers based on thread count
        with open(final_threads_file, 'r', encoding='utf-8') as f:
            _thread_count = len(json.load(f))

        _OUTLOOK_PARALLEL_THRESHOLD = 30  # only parallelize if more than this many threads

        if _thread_count > _OUTLOOK_PARALLEL_THRESHOLD:
            chunk_files = _split_threads_file(final_threads_file, index, _OUTLOOK_NUM_WORKERS)
        else:
            chunk_files = [final_threads_file]  # single chunk — run serially
        num_workers = len(chunk_files)

        if num_workers == 0:
            print("[INFO] No threads to process — skipping Outlook AI analysis")
        elif num_workers == 1:
            # Single worker — no need for thread pool overhead
            is_split = (chunk_files[0] != final_threads_file)
            print(f"[INFO] {_thread_count} threads (≤{_OUTLOOK_PARALLEL_THRESHOLD}) — running single Outlook worker")
            worker_snapshot = incremental_dir / "outlook" / f"temp_snapshot_{index}_w1.json"
            result = _outlook_worker(
                worker_id=1,
                chunk_file=chunk_files[0],
                snapshot_file=worker_snapshot,
                sub_index=index * 10 + 1,
                existing_events_arg=existing_events_arg,
                base_dir=base_dir,
                recent_focus_file=recent_focus_file,
                outlook_event_timeout_seconds=outlook_event_timeout_seconds,
            )
            if result and result.exists():
                shutil.copy2(str(result), str(temp_snapshot_file))
            _cleanup_worker_files(chunk_files if is_split else [], [worker_snapshot])
        else:
            # Parallel workers
            print(f"[INFO] {_thread_count} threads (>{_OUTLOOK_PARALLEL_THRESHOLD}) — splitting into {num_workers} chunks for parallel Outlook AI")
            worker_snapshots: list[Path] = []
            for i in range(num_workers):
                worker_snapshots.append(incremental_dir / "outlook" / f"temp_snapshot_{index}_w{i + 1}.json")

            with ThreadPoolExecutor(max_workers=num_workers) as pool:
                futures = {}
                for i in range(num_workers):
                    fut = pool.submit(
                        _outlook_worker,
                        worker_id=i + 1,
                        chunk_file=chunk_files[i],
                        snapshot_file=worker_snapshots[i],
                        sub_index=index * 10 + (i + 1),
                        existing_events_arg=existing_events_arg,
                        base_dir=base_dir,
                        recent_focus_file=recent_focus_file,
                        outlook_event_timeout_seconds=outlook_event_timeout_seconds,
                    )
                    futures[fut] = i + 1

                completed_snapshots: list[Path] = []
                for fut in as_completed(futures):
                    wid = futures[fut]
                    try:
                        result = fut.result()
                        if result:
                            completed_snapshots.append(result)
                            print(f"[OK] Outlook Worker-{wid} finished")
                        else:
                            print(f"[WARN] Outlook Worker-{wid} produced no snapshot")
                    except Exception as exc:
                        print(f"[ERROR] Outlook Worker-{wid} failed: {exc}")

            if completed_snapshots:
                _merge_outlook_snapshots(completed_snapshots, temp_snapshot_file)
            else:
                print("[WARN] All Outlook workers failed — no snapshot to merge")
            _cleanup_worker_files(chunk_files, worker_snapshots)

        # Post-merge: Dedup Events and Dedup Todos run on the merged snapshot (serial)
        if temp_snapshot_file.exists():
            # Dedup Events -> Creates separate deduped file
            if enable_dedup:
                deduped_snapshot_file = incremental_dir / "outlook" / f"temp_snapshot_{index}_deduped.json"
                
                # Load max_dedup_run from config
                max_dedup_run = 3
                try:
                    if isinstance(cfg, dict):
                        max_dedup_run = int(cfg.get("max_dedup_run", 3))
                except Exception as e:
                    print(f"[WARN] Failed to read max_dedup_run from config: {e}")
                
                cmd_dedup = [
                    sys.executable,
                    "-m",
                    "outlook_v2.ai_dedup_events",
                    "--input", str(temp_snapshot_file),
                    "--output", str(deduped_snapshot_file),
                    "--guide", str(base_dir / DEDUP_GUIDE),
                    "--max-dedup-run", str(max_dedup_run)
                ]
                if run_command(
                    cmd_dedup,
                    cwd=base_dir,
                    description="Deduplicating Outlook Events",
                    input_files=[str(temp_snapshot_file), str(base_dir / DEDUP_GUIDE)],
                    output_files=[str(deduped_snapshot_file)],
                ):
                    temp_snapshot_file = deduped_snapshot_file

            # Dedup Todos -> Updates Temp Snapshot in-place
            cmd_dedup_todos = [
                sys.executable,
                "-m",
                "outlook_v2.ai_dedup_todos",
                "--input", str(temp_snapshot_file),
                "--output", str(temp_snapshot_file),
            ]
            run_command(
                cmd_dedup_todos,
                cwd=base_dir,
                description="Deduplicating Outlook Todos",
                input_files=[str(temp_snapshot_file)],
                output_files=[str(temp_snapshot_file)],
            )

            # Process Snapshot: Create Delta and Update Master
            try:
                with open(temp_snapshot_file, 'r', encoding='utf-8') as f:
                    snapshot_data = json.load(f)
                
                all_events = snapshot_data.get('events', [])
                current_run_deduped = snapshot_data.get('deduped_events', [])
                
                # Load existing master to preserve accumulated deduped_events
                accumulated_deduped = []
                existing_ids = set()
                if master_events_file.exists():
                    try:
                        with open(master_events_file, 'r', encoding='utf-8') as f:
                            old_master_data = json.load(f)
                            existing_ids = {e.get('event_id') for e in old_master_data.get('events', [])}
                            accumulated_deduped = old_master_data.get('deduped_events', [])
                    except:
                        pass
                
                # Filter for Delta
                delta_events = [e for e in all_events if e.get('last_updated', '') >= pipeline_start_time]
                
                # Separate new vs updated events
                new_event_ids = []
                updated_event_ids = []
                
                for e in delta_events:
                    eid = e.get('event_id')
                    if eid in existing_ids:
                        updated_event_ids.append(eid)
                    else:
                        new_event_ids.append(eid)

                # Save Delta (includes current run's deduped_events)
                with open(delta_events_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        "events": delta_events,
                        "deduped_events": current_run_deduped
                    }, f, indent=2)
                print(f"[INFO] Saved Delta Events: {delta_events_file.name} ({len(delta_events)} events, {len(current_run_deduped)} deduped)")
                
                # Merge deduped_events: Add new deduplications to accumulated list
                # Avoid duplicates based on event_id
                existing_deduped_ids = {d.get('event_id') for d in accumulated_deduped}
                for deduped in current_run_deduped:
                    if deduped.get('event_id') not in existing_deduped_ids:
                        accumulated_deduped.append(deduped)
                        existing_deduped_ids.add(deduped.get('event_id'))
                
                # Update snapshot with accumulated deduped_events before saving as master
                snapshot_data['deduped_events'] = accumulated_deduped
                
                # Save updated master
                with open(master_events_file, 'w', encoding='utf-8') as f:
                    json.dump(snapshot_data, f, indent=2)
                print(f"[INFO] Updated Master Outlook Events: {master_events_file.name} (Total deduped: {len(accumulated_deduped)})")
                
                # Keep temp_snapshot_file (and any deduped snapshot) for Observation review.
                pass
                
                # Update Log
                with open(final_threads_file, 'r', encoding='utf-8') as f:
                    threads_data = json.load(f)
                new_threads_count = len(threads_data)
                
                update_log_stats(incremental_dir / "outlook" / "fetch_log.json", index, {
                    "new_threads": new_threads_count,
                    "new_events_count": len(new_event_ids),
                    "updated_events_count": len(updated_event_ids),
                    "new_event_ids": new_event_ids,
                    "updated_event_ids": updated_event_ids
                })
                
            except Exception as e:
                print(f"[ERROR] Failed to process events snapshot: {e}")

    # 7. AI Analysis (Teams)
    final_teams_dir = incremental_dir / "teams" / f"teams_conversations_{index}"
    if final_teams_dir.exists():
        teams_output_dir = incremental_dir / "teams" / f"teams_analysis_{index}"
        
        # Prepare existing summary argument
        existing_summary_arg = []
        master_summary_path = base_dir / MASTER_TEAMS_SUMMARY
        
        if master_summary_path.exists():
            existing_summary_arg = ["--existing-summary", str(master_summary_path)]
            print(f"[INFO] Using Master Teams Summary: {master_summary_path.name}")
        else:
            # Fallback: Try to find previous run's summary if master doesn't exist
            if index > 1:
                user_id = user_email.split('@')[0]
                prev_summary_file = incremental_dir / "teams" / f"teams_analysis_{index-1}" / f"teams_analysis_summary_{user_id}.json"
                if prev_summary_file.exists():
                    existing_summary_arg = ["--existing-summary", str(prev_summary_file)]
                    print(f"[INFO] Master summary not found. Using previous run summary: {prev_summary_file.name}")

        cmd_teams = [
            sys.executable,
            "-m",
            "teams.analyze_teams_conversations",
            str(final_teams_dir),
            "--user", user_email,
            "--guide", str(base_dir / TEAMS_GUIDE),
            "--output", str(teams_output_dir),
            "--profile", str(base_dir / USER_PROFILE_FILE),
            "--recent-focus", str(incremental_dir / "output" / "recent_focus.json"),
        ] + existing_summary_arg
        
        user_id = user_email.split('@')[0]
        snapshot_summary_file = teams_output_dir / f"teams_analysis_summary_{user_id}.json"

        if run_command(
            cmd_teams,
            cwd=base_dir,
            description="Analyzing Teams Conversations",
            input_files=[
                str(final_teams_dir),
                str(base_dir / TEAMS_GUIDE),
                str(base_dir / USER_PROFILE_FILE),
                *( [existing_summary_arg[1]] if len(existing_summary_arg) >= 2 else [] ),
            ],
            output_files=[str(snapshot_summary_file)],
        ):
            # Dedup Teams Todos -> Updates summary in-place
            cmd_teams_dedup = [
                sys.executable,
                "-m",
                "teams.dedup_todos",
                "--input", str(snapshot_summary_file),
                "--output", str(snapshot_summary_file),
                "--conversations-dir", str(final_teams_dir),
            ]
            run_command(
                cmd_teams_dedup,
                cwd=base_dir,
                description="Deduplicating Teams Todos",
                input_files=[str(snapshot_summary_file)],
                output_files=[str(snapshot_summary_file)],
            )

            # Process Snapshot: Create Delta and Update Master
            user_id = user_email.split('@')[0]
            snapshot_summary_file = teams_output_dir / f"teams_analysis_summary_{user_id}.json"
            
            if snapshot_summary_file.exists():
                try:
                    with open(snapshot_summary_file, 'r', encoding='utf-8') as f:
                        summary_data = json.load(f)
                    
                    # Handle Dict vs List (analyze_teams_conversations outputs dict, but we need list)
                    if isinstance(summary_data, dict):
                        all_conversations = summary_data.get('results', [])
                    else:
                        all_conversations = summary_data

                    # Load existing master IDs to distinguish new vs updated
                    existing_ids = set()
                    if master_summary_path.exists():
                        try:
                            with open(master_summary_path, 'r', encoding='utf-8') as f:
                                master_data = json.load(f)
                                if isinstance(master_data, dict):
                                    master_list = master_data.get('results', [])
                                else:
                                    master_list = master_data
                                existing_ids = {c.get('conversation_id') for c in master_list}
                        except:
                            pass
                    
                    # Filter for Delta
                    delta_summary = [c for c in all_conversations if c.get('last_updated', '') >= pipeline_start_time]
                    
                    new_conv_ids = []
                    updated_conv_ids = []
                    
                    for c in delta_summary:
                        cid = c.get('conversation_id')
                        if cid in existing_ids:
                            updated_conv_ids.append(cid)
                        else:
                            new_conv_ids.append(cid)
                    
                    # Update Master
                    # Ensure we save as list for compatibility with analyze_teams_conversations.py loading logic
                    with open(master_summary_path, 'w', encoding='utf-8') as f:
                        json.dump(all_conversations, f, indent=2)
                    print(f"[INFO] Updated Master Teams Summary: {master_summary_path}")
                    
                    # Overwrite the output file with Delta
                    with open(snapshot_summary_file, 'w', encoding='utf-8') as f:
                        json.dump(delta_summary, f, indent=2)
                    print(f"[INFO] Saved Delta Teams Summary: {snapshot_summary_file.name} ({len(delta_summary)} items)")
                    
                    # Update Log
                    new_teams_count = len(list(final_teams_dir.glob("*.json")))
                    update_log_stats(incremental_dir / "teams" / "fetch_log.json", index, {
                        "raw_new_conversations": new_teams_count,
                        "new_conversations_count": len(new_conv_ids),
                        "updated_conversations_count": len(updated_conv_ids),
                        "new_conversation_ids": new_conv_ids,
                        "updated_conversation_ids": updated_conv_ids
                    })
                    
                except Exception as e:
                    print(f"[WARN] Failed to process Teams summary: {e}")

    # 7b. (Pruning runs before AI analysis; no second pass here)

    # 8. Generate Briefing Data (for React App)
    prepare_data_script = base_dir / "pipeline" / "prepare_briefing_data.py"
    
    if prepare_data_script.exists():
        output_dir = incremental_dir / "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Define paths
        delta_events_file = incremental_dir / "outlook" / f"outlook_events_{user_alias}_{index}.json"
        final_threads_file = incremental_dir / "outlook" / f"threads_{index}.json"
        
        user_id = user_email.split('@')[0]
        teams_output_dir = incremental_dir / "teams" / f"teams_analysis_{index}"
        snapshot_summary_file = teams_output_dir / f"teams_analysis_summary_{user_id}.json"
        
        # Intermediate Data File
        briefing_data_file = PATHS.incremental_output_dir() / f"briefing_data_{index}.json"
        
        # Check existence
        has_outlook = delta_events_file.exists()
        has_teams = snapshot_summary_file.exists() and latest_teams and latest_teams.exists()
        
        if has_outlook or has_teams:
            # Create dummy files for missing inputs if necessary
            dummy_teams_raw = incremental_dir / "dummy_teams_raw.json"
            dummy_teams_analysis = incremental_dir / "dummy_teams_analysis.json"
            dummy_outlook_events = incremental_dir / "dummy_outlook_events.json"
            
            if not has_teams:
                with open(dummy_teams_raw, 'w', encoding='utf-8') as f: json.dump({"messages": []}, f)
                with open(dummy_teams_analysis, 'w', encoding='utf-8') as f: json.dump([], f)
            
            if not has_outlook:
                with open(dummy_outlook_events, 'w', encoding='utf-8') as f: json.dump({"events": []}, f)

            # Step 8a: Prepare Data
            cmd_prep = [
                sys.executable,
                str(prepare_data_script),
                "--output-json", str(briefing_data_file),
                "--outlook-events", str(delta_events_file if has_outlook else dummy_outlook_events),
                "--teams-analysis", str(snapshot_summary_file if has_teams else dummy_teams_analysis),
                "--teams-raw", str(latest_teams if has_teams else dummy_teams_raw),
                "--user-id", user_id
            ]
            
            if has_outlook and final_threads_file.exists():
                cmd_prep.extend(["--outlook-threads", str(final_threads_file)])
                
            prep_inputs = [
                str(delta_events_file if has_outlook else dummy_outlook_events),
                str(snapshot_summary_file if has_teams else dummy_teams_analysis),
                str(latest_teams if has_teams else dummy_teams_raw),
            ]
            if has_outlook and final_threads_file.exists():
                prep_inputs.append(str(final_threads_file))

            run_command(
                cmd_prep,
                cwd=base_dir,
                description="Preparing Briefing Data",
                input_files=prep_inputs,
                output_files=[str(briefing_data_file)],
            )
            
            # Cleanup dummies
            if not has_teams:
                if dummy_teams_raw.exists(): os.remove(dummy_teams_raw)
                if dummy_teams_analysis.exists(): os.remove(dummy_teams_analysis)
            if not has_outlook:
                if dummy_outlook_events.exists(): os.remove(dummy_outlook_events)

    # 9. Generate Master Briefing Data
    print("\n[STEP] Generating Master Briefing Data")
    master_data_file = PATHS.briefing_data_file()
    
    master_events_file = PATHS.incremental_data_dir() / PATHS.outlook_dirname / f"master_outlook_events_{user_alias}.json"
    master_teams_summary = base_dir / MASTER_TEAMS_SUMMARY
    master_threads_file = base_dir / MASTER_THREADS_FILE
    fetch_log_file = PATHS.incremental_data_dir() / PATHS.outlook_dirname / "fetch_log.json"
    teams_fetch_log_file = PATHS.incremental_data_dir() / PATHS.teams_dirname / "fetch_log.json"
    
    # We need a master raw teams file. The pipeline doesn't maintain a single master raw file 
    # in the same way, but we can try to find 'all_teams_messages.json' if it exists or use the latest.
    # The user request implies 'incremental_data/teams/all_teams_messages.json' exists.
    master_teams_raw = PATHS.incremental_data_dir() / PATHS.teams_dirname / "all_teams_messages.json"
    
    # Check if master files exist (run if at least one source is available)
    if master_events_file.exists() or master_teams_summary.exists():
        # Fill in dummy files for whichever source is missing
        dummy_master_events = incremental_dir / "dummy_master_events.json"
        dummy_master_teams = incremental_dir / "dummy_master_teams.json"
        if not master_events_file.exists():
            dummy_master_events.write_text(json.dumps({"events": []}), encoding="utf-8")
            master_events_file = dummy_master_events
        if not master_teams_summary.exists():
            dummy_master_teams.write_text(json.dumps([]), encoding="utf-8")
            master_teams_summary = dummy_master_teams
    if master_events_file.exists() and master_teams_summary.exists():
        user_id = user_email.split('@')[0]
        
        # Step 9a: Prepare Master Data
        cmd_master_prep = [
            sys.executable,
            str(prepare_data_script),
            "--output-json", str(master_data_file),
            "--outlook-events", str(master_events_file),
            "--teams-analysis", str(master_teams_summary),
            "--teams-raw", str(master_teams_raw if master_teams_raw.exists() else latest_teams), # Fallback to latest if master raw missing
            "--outlook-threads", str(master_threads_file),
            "--user-id", user_id
        ]

        if fetch_log_file.exists():
            cmd_master_prep.extend(["--fetch-log", str(fetch_log_file)])

        if teams_fetch_log_file.exists():
            cmd_master_prep.extend(["--teams-fetch-log", str(teams_fetch_log_file)])

        run_command(
            cmd_master_prep,
            cwd=base_dir,
            description="Preparing Master Briefing Data",
            input_files=[
                str(master_events_file),
                str(master_teams_summary),
                str(master_teams_raw if master_teams_raw.exists() else latest_teams),
                str(master_threads_file),
                *( [str(fetch_log_file)] if fetch_log_file.exists() else [] ),
                *( [str(teams_fetch_log_file)] if teams_fetch_log_file.exists() else [] ),
            ],
            output_files=[str(master_data_file)],
        )
    else:
        print("[WARN] Skipping Master Briefing Data: no master data files found yet.")

    # Clean up any dummy master files created above
    for _dummy in [
        incremental_dir / "dummy_master_events.json",
        incremental_dir / "dummy_master_teams.json",
    ]:
        try:
            if _dummy.exists():
                _dummy.unlink()
        except Exception:
            pass

    print("\n" + "="*60)
    print("INCREMENTAL PIPELINE COMPLETE")
    print("Dashboard available at: http://localhost:3000")
    print("="*60)

def main():
    global _PIPELINE_IN_WORKING_STATE
    global _BACKUP_ENABLED
    parser = argparse.ArgumentParser(description="Run the incremental data pipeline.")
    parser.add_argument("--interval", type=int, help="Run repeatedly with this interval in minutes.")
    parser.add_argument("--disable-dedup", action="store_true", help="Disable event deduplication")
    parser.add_argument("--no-servers", action="store_true", help="Do not start Flask/React servers")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the dashboard in a browser")
    parser.add_argument("--skip-backup", action="store_true", help="Skip incremental_data_backup management (server-managed)")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip incremental_fetch.py and process existing emails_*/teams_* inputs")
    args = parser.parse_args()

    # Set up tee logging (console + file) before anything else.
    _log_fh = _setup_tee_logging()

    if args.skip_backup:
        _BACKUP_ENABLED = False

    base_dir = Path(__file__).parent.absolute()

    # Ensure pipeline_config.json exists (fresh clone / no server started yet)
    cfg = {}
    try:
        loaded = ensure_effective_config(base_dir)
        if isinstance(loaded, dict):
            cfg = loaded
    except Exception:
        pass

    # Apply active dataset folder selection before any backup/start/run operations.
    _configure_active_data_folder(cfg, base_dir=base_dir)
    _configure_pipeline_subprocess_env(cfg)

    interval = args.interval
    enable_dedup = not args.disable_dedup
    
    # Start servers (optional; server_react.py already hosts the UI)
    if not args.no_servers:
        start_servers(base_dir)
    
    # Check config if interval not provided
    if interval is None:
        try:
            if isinstance(cfg, dict):
                val = cfg.get("fetch_interval_minutes")
                if val:
                    interval = int(val)
                    print(f"[INFO] Using interval from config: {interval} minutes")
        except Exception as e:
            print(f"[WARN] Failed to read fetch_interval_minutes from config: {e}")

    if interval:
        interval_seconds = interval * 60
        print(f"[INFO] Running in loop mode with interval: {interval} minutes ({interval_seconds} seconds)")
        first_run = True
        # Always fetch on the first loop iteration
        skip_first_fetch = False
        while True:
            try:
                # ---- Schedule enforcement ----
                # Re-read config each cycle so UI changes take effect immediately
                sched = _load_schedule_config()
                if not _is_within_schedule(sched):
                    wait_secs = _seconds_until_next_schedule_window(sched)
                    next_window_iso = datetime.fromtimestamp(time.time() + wait_secs).isoformat()
                    days_map = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}
                    day_names = ', '.join(days_map.get(d, '?') for d in sched.get('schedule_days', []))
                    start_t = f"{sched.get('schedule_start_hour', 8)}:{sched.get('schedule_start_minute', 0):02d}"
                    end_t = f"{sched.get('schedule_end_hour', 17)}:{sched.get('schedule_end_minute', 0):02d}"
                    msg = f"Outside schedule ({day_names} {start_t}-{end_t}). Waiting until next window."
                    print(f"[SCHEDULE] {msg}")
                    update_pipeline_status("sleeping", msg, next_run=next_window_iso)
                    time.sleep(min(wait_secs, 300))  # Re-check every 5 min max
                    continue

                print(f"\n[START] Pipeline run at {datetime.now()}")
                # Before entering working state, snapshot incremental_data (optional).
                if _BACKUP_ENABLED:
                    src = _incremental_data_path()
                    backup = _incremental_backup_path()
                    print(f"[INFO] Creating backup: {src} -> {backup}")
                    base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
                    pipeline_state.create_incremental_backup(
                        base_dir,
                        incremental_dirname=incremental_dirname,
                        backup_dirname=backup_dirname,
                    )
                    _PIPELINE_IN_WORKING_STATE = True
                update_pipeline_status("working", "Pipeline is fetching new data...")
                skip_fetch_this_run = bool(args.skip_fetch or skip_first_fetch)
                run_pipeline(open_browser=False, enable_dedup=enable_dedup, skip_fetch=skip_fetch_this_run)
                # Working run finished successfully; remove backup.
                if _BACKUP_ENABLED:
                    try:
                        base_dir, _, backup_dirname = _incremental_backup_spec()
                        pipeline_state.delete_incremental_backup(
                            base_dir,
                            backup_dirname=backup_dirname,
                        )
                    except Exception as e:
                        print(f"[WARN] Failed to delete incremental_data backup: {e}")
                    _PIPELINE_IN_WORKING_STATE = False
                
                # Only skip fetch on the very first run after reset
                if skip_first_fetch:
                    skip_first_fetch = False
                
                if first_run and not args.no_browser:
                    print("[INFO] Opening dashboard in browser...")
                    webbrowser.open("http://localhost:3000/")
                    first_run = False
                
                # Calculate next aligned run time
                sched = _load_schedule_config()
                next_run_dt = _calculate_next_aligned_run(sched, interval)
                next_run_iso = next_run_dt.isoformat()
                sleep_seconds = max(1, int((next_run_dt - datetime.now()).total_seconds()))
                
                next_run_time_str = next_run_dt.strftime("%H:%M:%S")
                print(f"\n[WAIT] Next run at {next_run_time_str} (sleeping {sleep_seconds}s)...")
                update_pipeline_status("sleeping", f"Next run at {next_run_time_str}", next_run=next_run_iso)
                
                time.sleep(sleep_seconds)
            except KeyboardInterrupt:
                print("\n[STOP] Pipeline stopped by user.")
                # If the user interrupts during working, restore incremental_data.
                if _BACKUP_ENABLED and _PIPELINE_IN_WORKING_STATE:
                    src = _incremental_data_path()
                    backup = _incremental_backup_path()
                    if backup.exists():
                        print(f"[WARN] Restoring incremental data from backup: {backup} -> {src}")
                        base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
                        ok = pipeline_state.restore_incremental_from_backup(
                            base_dir,
                            incremental_dirname=incremental_dirname,
                            backup_dirname=backup_dirname,
                        )
                        if not ok:
                            print("[ERROR] Failed to restore incremental_data from backup")
                delete_pipeline_status_file()
                cleanup_processes()
                break
            except Exception as e:
                print(f"[ERROR] Pipeline run failed: {e}")

                # If we were in working state and the run failed unexpectedly, exit working state.
                # Keep the backup to aid manual recovery if desired; the next run will overwrite it.
                _PIPELINE_IN_WORKING_STATE = False
                
                # Calculate next aligned run time
                sched = _load_schedule_config()
                next_run_dt = _calculate_next_aligned_run(sched, interval)
                next_run_iso = next_run_dt.isoformat()
                sleep_seconds = max(1, int((next_run_dt - datetime.now()).total_seconds()))
                
                next_run_time_str = next_run_dt.strftime("%H:%M:%S")
                print(f"\n[WAIT] Retrying at {next_run_time_str} (sleeping {sleep_seconds}s)...")
                update_pipeline_status("sleeping", f"Error: {str(e)}. Retrying at {next_run_time_str}", next_run=next_run_iso)
                
                time.sleep(sleep_seconds)
    else:
        if _BACKUP_ENABLED:
            src = _incremental_data_path()
            backup = _incremental_backup_path()
            print(f"[INFO] Creating backup: {src} -> {backup}")
            base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
            pipeline_state.create_incremental_backup(
                base_dir,
                incremental_dirname=incremental_dirname,
                backup_dirname=backup_dirname,
            )
            _PIPELINE_IN_WORKING_STATE = True
        update_pipeline_status("working", "Pipeline is running (one-off)...")
        skip_fetch = bool(args.skip_fetch)
        try:
            run_pipeline(open_browser=False, enable_dedup=enable_dedup, skip_fetch=skip_fetch)
            if _BACKUP_ENABLED:
                try:
                    base_dir, _, backup_dirname = _incremental_backup_spec()
                    pipeline_state.delete_incremental_backup(
                        base_dir,
                        backup_dirname=backup_dirname,
                    )
                except Exception as e:
                    print(f"[WARN] Failed to delete incremental_data backup: {e}")
                _PIPELINE_IN_WORKING_STATE = False
            update_pipeline_status("offline", "Pipeline finished (one-off)")
            # One-off completed successfully; keep status file.
        finally:
            # If interrupted during working, restore and delete pipeline_status.json.
            if _BACKUP_ENABLED and _PIPELINE_IN_WORKING_STATE:
                src = _incremental_data_path()
                backup = _incremental_backup_path()
                if backup.exists():
                    print(f"[WARN] Restoring incremental data from backup: {backup} -> {src}")
                    base_dir, incremental_dirname, backup_dirname = _incremental_backup_spec()
                    ok = pipeline_state.restore_incremental_from_backup(
                        base_dir,
                        incremental_dirname=incremental_dirname,
                        backup_dirname=backup_dirname,
                    )
                    if not ok:
                        print("[ERROR] Failed to restore incremental_data from backup")
                delete_pipeline_status_file()
                _PIPELINE_IN_WORKING_STATE = False

if __name__ == "__main__":
    main()
