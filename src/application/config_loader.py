from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from src.shared.constants import APP_VERSION, BOOTSTRAP_CONFIG_PATH, DEFAULT_SCHEMA_VERSION
from src.shared.models import BootstrapConfig


class ConfigError(RuntimeError):
    pass


def get_app_base_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def resolve_app_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (get_app_base_path() / path).resolve()


def get_default_data_root() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return (Path(local_app_data) / "OpsMonitor").resolve()
    return (Path.home() / ".opsmonitor").resolve()


def _load_bootstrap_raw() -> tuple[Path, dict]:
    path = resolve_app_path(BOOTSTRAP_CONFIG_PATH)
    if not path.exists():
        raise ConfigError(f"Bootstrap config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return path, raw


def save_bootstrap_data_path_override(new_path: str | None) -> None:
    path, raw = _load_bootstrap_raw()
    if new_path:
        raw["data_path_override"] = new_path
    else:
        raw.pop("data_path_override", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)


def load_bootstrap_config() -> BootstrapConfig:
    path, raw = _load_bootstrap_raw()
    data_override = str(raw.get("data_path_override") or "").strip() or None
    data_root = Path(data_override).expanduser() if data_override else get_default_data_root()
    if not data_root.is_absolute():
        data_root = (get_app_base_path() / data_root).resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    migration_status, last_backup = _migrate_legacy_data_if_needed(get_app_base_path(), data_root, raw)
    _ensure_data_directories(data_root)
    return BootstrapConfig(
        app_base_path=get_app_base_path(),
        bootstrap_config_path=path,
        data_root_path=data_root,
        database_path=(data_root / "database" / "OpsMonitor.db").resolve(),
        app_log_path=(data_root / "logs" / "app.log").resolve(),
        backup_path=(data_root / "backups").resolve(),
        export_path=(data_root / "exports").resolve(),
        test_data_root_path=(data_root / "TestData").resolve(),
        default_window_state=str(raw.get("default_window_state", "normal")),
        default_theme=str(raw.get("default_theme", "Charcoal Blue")),
        data_path_override=data_override,
        migration_status=migration_status,
        last_backup_path=last_backup,
        schema_version=DEFAULT_SCHEMA_VERSION,
    )


def _ensure_data_directories(data_root: Path) -> None:
    for rel in [
        "database",
        "config",
        "logs",
        "backups",
        "backups/pre_upgrade",
        "exports",
        "exports/incident_summaries",
        "exports/config_exports",
        "runtime",
        "TestData/Site1",
        "TestData/Site2",
    ]:
        (data_root / rel).mkdir(parents=True, exist_ok=True)


def _migrate_legacy_data_if_needed(app_base_path: Path, data_root: Path, raw: dict) -> tuple[str, Path | None]:
    db_target = data_root / "database" / "OpsMonitor.db"
    if db_target.exists():
        return ("Using existing persistent data folder", None)

    legacy_db_value = raw.get("database_path", "OpsMonitor.db")
    legacy_db_path = Path(legacy_db_value)
    if not legacy_db_path.is_absolute():
        legacy_db_path = (app_base_path / legacy_db_path).resolve()
    if not legacy_db_path.exists():
        return ("Initialized new persistent data folder", None)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_upgrade_dir = data_root / "backups" / "pre_upgrade"
    pre_upgrade_dir.mkdir(parents=True, exist_ok=True)
    last_backup = pre_upgrade_dir / f"OpsMonitor_preupgrade_{timestamp}.db"
    shutil.copy2(legacy_db_path, last_backup)
    db_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(legacy_db_path, db_target)

    copied: list[str] = []
    for src_name, target_rel in [
        ("Logs", "logs"),
        ("Backups", "backups/legacy_build_backups"),
        ("TestData", "TestData"),
        ("Exports", "exports/legacy_build_exports"),
    ]:
        src = app_base_path / src_name
        dst = data_root / target_rel
        if src.exists():
            shutil.copytree(src, dst, dirs_exist_ok=True)
            copied.append(src_name)

    config_src = app_base_path / "config"
    if config_src.exists():
        shutil.copytree(config_src, data_root / "config" / "legacy_build_config", dirs_exist_ok=True)
        copied.append("config")

    return (f"Migrated legacy build data to persistent folder ({', '.join(copied) if copied else 'database only'})", last_backup)


def write_runtime_info_to_db(db_path: Path, config: BootstrapConfig) -> None:
    if not db_path.exists():
        return
    conn = sqlite3.connect(db_path)
    try:
        now = datetime.utcnow().isoformat()
        entries = {
            "data_root_path_hint": str(config.data_root_path),
            "backup_path_hint": str(config.backup_path),
            "export_path_hint": str(config.export_path),
            "test_data_root_path": str(config.test_data_root_path),
            "notification_log_path": str(config.data_root_path / "logs" / "notifications.log"),
            "last_startup_migration_status": config.migration_status,
            "last_startup_backup_path": str(config.last_backup_path) if config.last_backup_path else "",
            "schema_version": str(config.schema_version),
            "app_version": APP_VERSION,
        }
        for key, value in entries.items():
            conn.execute(
                """
                INSERT INTO application_settings(setting_key, setting_value, updated_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET
                    setting_value=excluded.setting_value,
                    updated_utc=excluded.updated_utc
                """,
                (key, value, now),
            )
        conn.commit()
    finally:
        conn.close()
