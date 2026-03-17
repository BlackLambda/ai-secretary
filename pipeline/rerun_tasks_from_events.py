"""Re-extract tasks (actions/todos) from existing Outlook events.

This is used by the desktop/web UI reset menu option:
  - Keep events and re-extract tasks

It does NOT refetch data from Outlook. It re-runs the AI action extraction +
validation on the existing master events snapshot, then regenerates
incremental_data/output/briefing_data.json so the UI reflects the updates.
This script is intended to be launched as a background process by server_react.py
and will write best-effort progress to the pipeline status file when possible.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.pipeline_config_manager import ensure_effective_config


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _status_path() -> Path:
    raw = os.environ.get('AI_SECRETARY_PIPELINE_STATUS_PATH') or os.environ.get('PIPELINE_STATUS_PATH')
    if isinstance(raw, str) and raw.strip():
        return Path(raw.strip()).resolve()
    return (_repo_root() / 'pipeline_status.json').resolve()


def _write_status(*, state: str, message: str, next_run: str = "", steps: list[dict] | None = None, current_step_id: str = "") -> None:
    try:
        p = _status_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            'state': state,
            'message': message,
            'next_run': next_run,
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'run_id': f"rerun_tasks_{int(time.time())}",
            'current_step_id': current_step_id,
            'steps': steps or [],
        }
        p.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    except Exception:
        pass


def _load_json(path: Path):
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _load_user_profile(*, inc_dir: Path) -> dict:
    """Load dataset-scoped user_profile.json.

    Strict: do NOT fall back to repo-root user_profile.json.
    """
    p = inc_dir / 'user_profile.json'
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Dataset user_profile.json not found: {p}")
    data = _load_json(p)
    return data if isinstance(data, dict) else {}


def _get_user_alias(*, repo_root: Path, inc_dir: Path | None) -> str:
    # Match existing repo conventions: USER_ALIAS is usually a list.
    try:
        if not isinstance(inc_dir, Path):
            raise FileNotFoundError('Missing active dataset folder')
        data = _load_user_profile(inc_dir=inc_dir)
        raw = None
        if isinstance(data, dict):
            raw = (
                data.get('USER_ALIAS')
                or data.get('USER_ALIAS'.lower())
                or data.get('user_alias')
                or data.get('alias')
            )
        if isinstance(raw, list) and raw:
            s = str(raw[0]).strip()
            if s:
                return s
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    except FileNotFoundError:
        raise
    except Exception:
        pass
    return 'user'


def _get_user_id(*, repo_root: Path, inc_dir: Path | None, alias: str) -> str:
    # Used by prepare_briefing_data.py for some history paths.
    try:
        if not isinstance(inc_dir, Path):
            raise FileNotFoundError('Missing active dataset folder')
        data = _load_user_profile(inc_dir=inc_dir)
        raw = None
        if isinstance(data, dict):
            raw = (
                data.get('USER_EMAIL')
                or data.get('USER_EMAIL'.lower())
                or data.get('user_email')
                or data.get('email')
            )
        if isinstance(raw, list) and raw:
            raw = raw[0]
        if isinstance(raw, str) and '@' in raw:
            return raw.split('@', 1)[0]
    except FileNotFoundError:
        raise
    except Exception:
        pass
    return alias


def _get_user_email(*, repo_root: Path, inc_dir: Path | None, alias: str) -> str:
    try:
        if not isinstance(inc_dir, Path):
            raise FileNotFoundError('Missing active dataset folder')
        data = _load_user_profile(inc_dir=inc_dir)
        raw = None
        if isinstance(data, dict):
            raw = (
                data.get('USER_EMAIL')
                or data.get('USER_EMAIL'.lower())
                or data.get('user_email')
                or data.get('email')
            )
        if isinstance(raw, list) and raw:
            raw = raw[0]
        if isinstance(raw, str) and raw.strip() and '@' in raw:
            return raw.strip()
    except FileNotFoundError:
        raise
    except Exception:
        pass
    # Best-effort fallback to a neutral example address.
    return f"{alias}@example.com"


def _user_profile_path_for_scripts(*, repo_root: Path, inc_dir: Path | None) -> Path:
    """Return dataset-scoped user_profile.json to pass to downstream scripts.

    Strict: if missing, raise an error (no repo-root fallback).
    """
    if not isinstance(inc_dir, Path):
        raise FileNotFoundError('Missing active dataset folder')
    p = inc_dir / 'user_profile.json'
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Dataset user_profile.json not found: {p}")
    return p


def _pick_existing(path: Path, glob_pattern: str) -> Path | None:
    if path.exists() and path.is_file():
        return path
    try:
        candidates = [p for p in path.parent.glob(glob_pattern) if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    except Exception:
        return None


def _run(cmd: list[str], *, cwd: Path, label: str) -> None:
    print(f"\n[STEP] {label}\n{' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=str(cwd))


def _resolve_data_folder_path(base_dir: Path) -> Path:
    """Resolve the configured data folder path.

    Mirrors server_react.py behavior so reruns use the active dataset.
    """
    try:
        cfg = ensure_effective_config(base_dir)
    except Exception:
        cfg = {}

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

    p = Path(str(chosen))
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def main() -> int:
    parser = argparse.ArgumentParser(description='Re-extract tasks (actions/todos) from existing snapshots.')
    parser.add_argument(
        '--sources',
        required=False,
        default='both',
        choices=['outlook', 'teams', 'both'],
        help='Which sources to rerun for option-3 (default: both).',
    )
    args = parser.parse_args()

    do_outlook = args.sources in ('outlook', 'both')
    do_teams = args.sources in ('teams', 'both')

    base_dir = _repo_root()
    os.chdir(str(base_dir))

    inc_dir = _resolve_data_folder_path(base_dir)

    profile_path = inc_dir / 'user_profile.json'
    if not profile_path.exists():
        _write_status(state='offline', message=f"Missing dataset user_profile.json: {profile_path}")
        raise FileNotFoundError(f"Missing dataset user_profile.json: {profile_path}")

    alias = _get_user_alias(repo_root=base_dir, inc_dir=inc_dir)
    user_id = _get_user_id(repo_root=base_dir, inc_dir=inc_dir, alias=alias)
    user_email = _get_user_email(repo_root=base_dir, inc_dir=inc_dir, alias=alias)
    outlook_dir = inc_dir / 'outlook'
    teams_dir = inc_dir / 'teams'

    master_events = outlook_dir / f"master_outlook_events_{alias}.json"
    master_threads = outlook_dir / 'master_threads.json'

    if not master_events.exists():
        _write_status(state='offline', message=f"Missing master events: {master_events}")
        raise FileNotFoundError(f"Missing master events file: {master_events}")

    threads_file = _pick_existing(master_threads, 'threads_*.json')
    if threads_file is None:
        _write_status(state='offline', message=f"Missing Outlook threads file: {master_threads}")
        raise FileNotFoundError(f"Missing Outlook threads file (master or threads_*.json) under {outlook_dir}")

    steps: list[dict] = []
    if do_outlook:
        steps.extend(
            [
                {'id': 'extract_actions', 'name': 'Extracting tasks (actions)'},
                {'id': 'validate_actions', 'name': 'Validating tasks (actions)'},
                {'id': 'dedup_todos', 'name': 'Deduplicating todos'},
            ]
        )
    if do_teams:
        steps.extend(
            [
                {'id': 'teams_process', 'name': 'Processing Teams messages'},
                {'id': 'teams_analyze', 'name': 'Extracting Teams tasks (AI)'},
                {'id': 'teams_dedup', 'name': 'Deduplicating Teams todos'},
            ]
        )
    steps.append({'id': 'prepare_briefing', 'name': 'Regenerating briefing_data.json'})

    tmp_snapshot: Path | None = None
    if do_outlook:
        tmp_snapshot = outlook_dir / f"master_outlook_events_{alias}.rerun_tmp.json"
        shutil.copy2(master_events, tmp_snapshot)

    try:
        profile_path = _user_profile_path_for_scripts(repo_root=base_dir, inc_dir=inc_dir)
        recent_focus_path = inc_dir / 'output' / 'recent_focus.json'
        if do_outlook:
            _write_status(state='working', message='Re-extracting tasks from existing events…', steps=steps, current_step_id='extract_actions')
            _run(
                [
                    sys.executable,
                    '-m',
                    'outlook_v2.ai_extract_actions',
                    '--input',
                    str(tmp_snapshot),
                    '--threads',
                    str(threads_file),
                    '--user-profile',
                    str(profile_path),
                    '--recent-focus',
                    str(recent_focus_path),
                    '--guide',
                    str(Path('outlook_v2') / 'action_extraction_guide.md'),
                ],
                cwd=base_dir,
                label='Extracting Outlook Actions',
            )
            _write_status(state='working', message='Validating extracted tasks…', steps=steps, current_step_id='validate_actions')
            _run(
                [
                    sys.executable,
                    '-m',
                    'outlook_v2.ai_validate_actions',
                    '--input',
                    str(tmp_snapshot),
                    '--output',
                    str(tmp_snapshot),
                    '--threads',
                    str(threads_file),
                    '--user-profile',
                    str(profile_path),
                    '--recent-focus',
                    str(recent_focus_path),
                    '--guide',
                    str(Path('outlook_v2') / 'action_validation_guide.md'),
                ],
                cwd=base_dir,
                label='Validating Outlook Actions',
            )

            _write_status(state='working', message='Deduplicating todos…', steps=steps, current_step_id='dedup_todos')
            _run(
                [
                    sys.executable,
                    '-m',
                    'outlook_v2.ai_dedup_todos',
                    '--input',
                    str(tmp_snapshot),
                    '--output',
                    str(tmp_snapshot),
                ],
                cwd=base_dir,
                label='Deduplicating Outlook Todos',
            )

            # Swap into place.
            shutil.copy2(tmp_snapshot, master_events)

        # Re-run Teams extraction from existing raw Teams messages when available.
        # This mirrors the Teams pipeline steps:
        #   all_teams_messages.json -> conversation files -> AI analysis summary -> dedup
        teams_analysis = teams_dir / 'master_teams_analysis_summary.json'
        # Prefer the canonical all_teams_messages.json; fallback to the latest teams_*.json.
        teams_raw = _pick_existing(teams_dir / 'all_teams_messages.json', 'teams_*.json')

        if do_teams and teams_raw is not None:
            teams_conv_dir = teams_dir / '_rerun_teams_conversations'
            teams_out_dir = teams_dir / '_rerun_teams_analysis'

            _write_status(state='working', message='Processing Teams messages…', steps=steps, current_step_id='teams_process')
            _run(
                [
                    sys.executable,
                    '-m',
                    'teams.process_teams_messages',
                    str(teams_raw),
                    str(teams_conv_dir),
                ],
                cwd=base_dir,
                label='Processing Teams Messages',
            )

            _write_status(state='working', message='Analyzing Teams conversations…', steps=steps, current_step_id='teams_analyze')
            _run(
                [
                    sys.executable,
                    '-m',
                    'teams.analyze_teams_conversations',
                    str(teams_conv_dir),
                    '--user',
                    str(user_email),
                    '--guide',
                    str(Path('teams') / 'Teams_Chat.md'),
                    '--output',
                    str(teams_out_dir),
                    '--profile',
                    str(profile_path),
                    '--recent-focus',
                    str(inc_dir / 'output' / 'recent_focus.json'),
                ],
                cwd=base_dir,
                label='Analyzing Teams Conversations',
            )

            _write_status(state='working', message='Deduplicating Teams todos…', steps=steps, current_step_id='teams_dedup')
            snapshot_summary_file = teams_out_dir / f'teams_analysis_summary_{user_id}.json'
            if snapshot_summary_file.exists():
                _run(
                    [
                        sys.executable,
                        '-m',
                        'teams.dedup_todos',
                        '--input',
                        str(snapshot_summary_file),
                        '--output',
                        str(snapshot_summary_file),
                        '--conversations-dir',
                        str(teams_conv_dir),
                    ],
                    cwd=base_dir,
                    label='Deduplicating Teams Todos',
                )

                # Write master Teams summary as a list for compatibility with the main pipeline.
                try:
                    data = _load_json(snapshot_summary_file)
                    convs = data.get('results', []) if isinstance(data, dict) else data
                    if not isinstance(convs, list):
                        convs = []
                    teams_analysis.write_text(json.dumps(convs, indent=2), encoding='utf-8')
                except Exception:
                    pass
        elif do_teams:
            print('[WARN] No Teams raw snapshot found (all_teams_messages.json or teams_*.json); skipping Teams rerun.')

        _write_status(state='working', message='Regenerating briefing_data.json…', steps=steps, current_step_id='prepare_briefing')

        dummy_root = inc_dir
        dummy_teams_raw = dummy_root / 'dummy_teams_raw.json'
        dummy_teams_analysis = dummy_root / 'dummy_teams_analysis.json'

        has_teams = teams_analysis.exists() and teams_raw is not None and teams_raw.exists()
        if not has_teams:
            dummy_teams_raw.write_text(json.dumps({'messages': []}), encoding='utf-8')
            dummy_teams_analysis.write_text(json.dumps([]), encoding='utf-8')

        out_dir = inc_dir / 'output'
        out_dir.mkdir(parents=True, exist_ok=True)
        briefing_out = out_dir / 'briefing_data.json'

        cmd_briefing = [
            sys.executable,
            str(base_dir / 'pipeline' / 'prepare_briefing_data.py'),
            '--output-json',
            str(briefing_out),
            '--outlook-events',
            str(master_events),
            '--teams-analysis',
            str(teams_analysis if has_teams else dummy_teams_analysis),
            '--teams-raw',
            str((teams_raw if has_teams else dummy_teams_raw)),
            '--outlook-threads',
            str(threads_file),
            '--user-id',
            str(user_id),
        ]

        fetch_log = outlook_dir / 'fetch_log.json'
        if fetch_log.exists():
            cmd_briefing.extend(['--fetch-log', str(fetch_log)])
        teams_fetch_log = teams_dir / 'fetch_log.json'
        if teams_fetch_log.exists():
            cmd_briefing.extend(['--teams-fetch-log', str(teams_fetch_log)])

        _run(cmd_briefing, cwd=base_dir, label='Preparing Master Briefing Data')

        try:
            if not has_teams:
                if dummy_teams_raw.exists():
                    dummy_teams_raw.unlink()
                if dummy_teams_analysis.exists():
                    dummy_teams_analysis.unlink()
        except Exception:
            pass

        _write_status(state='offline', message='Re-extract tasks finished.', steps=steps, current_step_id='')
        print('\n[OK] Re-extract tasks finished.')
        return 0

    finally:
        try:
            if tmp_snapshot is not None and tmp_snapshot.exists():
                tmp_snapshot.unlink()
        except Exception:
            pass


if __name__ == '__main__':
    raise SystemExit(main())
