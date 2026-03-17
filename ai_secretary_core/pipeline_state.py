from __future__ import annotations

import os
import shutil
import stat
import time
import uuid
from pathlib import Path


def _on_rm_error(func, path, exc_info) -> None:
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def _path_lexists(path: Path) -> bool:
    try:
        return os.path.lexists(path)
    except Exception:
        return path.exists()


def _remove_path_best_effort(path: Path, retries: int = 3) -> bool:
    for attempt in range(max(1, retries)):
        try:
            if not _path_lexists(path):
                return True
            if path.is_symlink() or path.is_file():
                path.unlink()
            else:
                shutil.rmtree(path, onerror=_on_rm_error)
        except Exception:
            pass

        if not _path_lexists(path):
            return True
        if attempt + 1 < retries:
            time.sleep(0.2)
    return not _path_lexists(path)


def _cleanup_backup_temp_paths(backup: Path) -> None:
    legacy_tmp = backup.with_name(f"{backup.name}.tmp")
    _remove_path_best_effort(legacy_tmp)

    pattern = f"{backup.name}.tmp-*"
    try:
        for candidate in backup.parent.glob(pattern):
            _remove_path_best_effort(candidate)
    except Exception:
        pass


def _unique_backup_tmp_path(backup: Path) -> Path:
    return backup.with_name(f"{backup.name}.tmp-{uuid.uuid4().hex}")


def incremental_data_path(
    base_dir: Path,
    incremental_dirname: str = "incremental_data",
) -> Path:
    return Path(base_dir) / incremental_dirname


def incremental_backup_path(
    base_dir: Path,
    backup_dirname: str = "incremental_data_backup",
) -> Path:
    return Path(base_dir) / backup_dirname


def pipeline_status_path(
    base_dir: Path,
    status_filename: str = "pipeline_status.json",
) -> Path:
    return Path(base_dir) / status_filename


def delete_pipeline_status_file(
    base_dir: Path,
    status_filename: str = "pipeline_status.json",
) -> None:
    try:
        p = pipeline_status_path(base_dir, status_filename=status_filename)
        if p.exists():
            p.unlink()
    except Exception:
        pass


def delete_stale_backup_if_coexists(
    base_dir: Path,
    incremental_dirname: str = "incremental_data",
    backup_dirname: str = "incremental_data_backup",
) -> None:
    """If both incremental_data and incremental_data_backup exist, delete the backup.

    This usually means a prior run crashed and left a stale backup behind.
    """
    src = incremental_data_path(base_dir, incremental_dirname=incremental_dirname)
    backup = incremental_backup_path(base_dir, backup_dirname=backup_dirname)
    try:
        _cleanup_backup_temp_paths(backup)
        if src.exists() and backup.exists():
            _remove_path_best_effort(backup)
    except Exception:
        pass


def create_incremental_backup(
    base_dir: Path,
    incremental_dirname: str = "incremental_data",
    backup_dirname: str = "incremental_data_backup",
) -> None:
    """Create backup_dirname as a snapshot of incremental_dirname."""
    base_dir = Path(base_dir)
    src = incremental_data_path(base_dir, incremental_dirname=incremental_dirname)
    backup = incremental_backup_path(base_dir, backup_dirname=backup_dirname)

    if not src.exists():
        src.mkdir(parents=True, exist_ok=True)

    _cleanup_backup_temp_paths(backup)
    _remove_path_best_effort(backup)

    tmp = _unique_backup_tmp_path(backup)
    shutil.copytree(src, tmp)

    # Move tmp into place (best-effort atomic on same volume).
    try:
        for _ in range(2):
            _remove_path_best_effort(backup)
            try:
                shutil.move(str(tmp), str(backup))
                return
            except OSError:
                if not _path_lexists(backup):
                    raise
                time.sleep(0.2)
        shutil.move(str(tmp), str(backup))
    finally:
        if _path_lexists(tmp):
            _remove_path_best_effort(tmp)


def restore_incremental_from_backup(
    base_dir: Path,
    incremental_dirname: str = "incremental_data",
    backup_dirname: str = "incremental_data_backup",
) -> bool:
    """Restore incremental_dirname from backup_dirname and remove the backup."""
    base_dir = Path(base_dir)
    src = incremental_data_path(base_dir, incremental_dirname=incremental_dirname)
    backup = incremental_backup_path(base_dir, backup_dirname=backup_dirname)
    if not backup.exists():
        return False

    try:
        _remove_path_best_effort(src)
        shutil.move(str(backup), str(src))
        return True
    except Exception:
        return False


def delete_incremental_backup(
    base_dir: Path,
    backup_dirname: str = "incremental_data_backup",
) -> None:
    backup = incremental_backup_path(base_dir, backup_dirname=backup_dirname)
    try:
        _remove_path_best_effort(backup)
    except Exception:
        pass


def delete_incremental_data_dir(
    base_dir: Path,
    incremental_dirname: str = "incremental_data",
) -> bool:
    src = incremental_data_path(base_dir, incremental_dirname=incremental_dirname)
    try:
        if src.exists():
            _remove_path_best_effort(src)
            return True
    except Exception:
        return False
    return False
