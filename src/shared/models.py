from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class BootstrapConfig:
    app_base_path: Path
    bootstrap_config_path: Path
    data_root_path: Path
    database_path: Path
    app_log_path: Path
    backup_path: Path
    export_path: Path
    test_data_root_path: Path
    default_window_state: str
    default_theme: str
    data_path_override: str | None = None
    migration_status: str = "No migration needed"
    last_backup_path: Path | None = None
    schema_version: int = 1


@dataclass(slots=True)
class AuthenticatedUser:
    user_id: int
    username: str
    display_name: str
    role_name: str
