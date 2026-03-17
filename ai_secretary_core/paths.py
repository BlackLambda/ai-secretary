from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RepoPaths:
    """Centralized filesystem layout for this repo.

    Goal: avoid scattering hardcoded relative paths across scripts.

    `base_dir` should be the repo root (folder containing `run_incremental_pipeline.py`).
    """

    base_dir: Path

    # Directory / file names
    incremental_data_dirname: str = "incremental_data"
    incremental_backup_dirname: str = "incremental_data_backup"
    user_state_dirname: str = "user_state"

    pipeline_status_filename: str = "pipeline_status.json"

    user_profile_filename: str = "user_profile.json"
    topics_filename: str = "topics.json"

    # Config
    pipeline_config_default_filename: str = "pipeline_config.default.json"
    pipeline_config_user_filename: str = "pipeline_config.user.json"
    pipeline_config_effective_filename: str = "pipeline_config.json"

    # Files under user_state/
    user_topics_filename: str = "user_topics.json"
    user_ops_store_filename: str = "user_ops_store.json"
    card_feedback_filename: str = "card_feedback.json"

    # Files under incremental_data/
    user_operation_filename: str = "user_operation.json"
    task_vectors_filename: str = "task_vectors.json"
    focus_model_filename: str = "focus_model.json"

    # Output files under incremental_data/output/
    briefing_data_filename: str = "briefing_data.json"
    prune_log_filename: str = "prune_log.json"

    # Other incremental_data/* dirs
    bug_reports_dirname: str = "bug_reports"
    sent_emails_dirname: str = "sent_emails"

    # Outlook/Teams dirs
    outlook_dirname: str = "outlook"
    teams_dirname: str = "teams"

    # Output locations
    output_dirname: str = "output"

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_dir", Path(self.base_dir).resolve())

    def resolve(self, relative: str | Path) -> Path:
        return (self.base_dir / Path(relative)).resolve()

    # --- Core dirs ---

    def incremental_data_dir(self) -> Path:
        return self.base_dir / self.incremental_data_dirname

    def incremental_backup_dir(self) -> Path:
        return self.base_dir / self.incremental_backup_dirname

    def user_state_dir(self) -> Path:
        return self.base_dir / self.user_state_dirname

    def pipeline_status_file(self) -> Path:
        return self.base_dir / self.pipeline_status_filename

    # --- Config ---

    def pipeline_config_default(self) -> Path:
        return self.base_dir / "config" / self.pipeline_config_default_filename

    def pipeline_config_user(self) -> Path:
        return self.base_dir / "config" / self.pipeline_config_user_filename

    def pipeline_config_effective(self) -> Path:
        return self.base_dir / "config" / self.pipeline_config_effective_filename

    # --- Common top-level files ---

    def user_profile_file(self) -> Path:
        return self.base_dir / self.user_profile_filename

    def topics_file(self) -> Path:
        return self.base_dir / "config" / self.topics_filename

    # --- user_state/* ---

    def user_topics_file(self) -> Path:
        return self.user_state_dir() / self.user_topics_filename

    def user_ops_store_file(self) -> Path:
        return self.user_state_dir() / self.user_ops_store_filename

    def card_feedback_file(self) -> Path:
        return self.user_state_dir() / self.card_feedback_filename

    # --- incremental_data/* ---

    def user_operation_file(self) -> Path:
        return self.incremental_data_dir() / self.user_operation_filename

    def task_vectors_file(self) -> Path:
        return self.incremental_data_dir() / self.task_vectors_filename

    def focus_model_file(self) -> Path:
        return self.incremental_data_dir() / self.focus_model_filename

    def incremental_output_dir(self) -> Path:
        return self.incremental_data_dir() / self.output_dirname

    def briefing_data_file(self) -> Path:
        return self.incremental_output_dir() / self.briefing_data_filename

    def prune_log_file(self) -> Path:
        return self.incremental_output_dir() / self.prune_log_filename

    def bug_reports_dir(self) -> Path:
        return self.incremental_data_dir() / self.bug_reports_dirname

    def sent_emails_dir(self) -> Path:
        return self.incremental_data_dir() / self.sent_emails_dirname

    # --- Outlook / Teams canonical locations ---

    def outlook_root(self) -> Path:
        return self.base_dir / "outlook_v2"

    def teams_root(self) -> Path:
        return self.base_dir / "teams"

    def outlook_scoring_system_default(self) -> Path:
        return self.base_dir / "config" / "scoring_system.json"

    def teams_scoring_system_default(self) -> Path:
        return self.base_dir / "config" / "scoring_system.json"
