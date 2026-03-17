from __future__ import annotations

import shutil
from pathlib import Path


def _remove_path_best_effort(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
            return
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


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
    tmp = backup.with_name(f"{backup.name}.tmp")
    try:
        _remove_path_best_effort(tmp)
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
    tmp = backup.with_name(f"{backup.name}.tmp")

    if not src.exists():
        src.mkdir(parents=True, exist_ok=True)

    _remove_path_best_effort(tmp)
    _remove_path_best_effort(backup)

    shutil.copytree(src, tmp)

    # Move tmp into place (best-effort atomic on same volume).
    try:
        _remove_path_best_effort(backup)
    except Exception:
        pass
    shutil.move(str(tmp), str(backup))


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
