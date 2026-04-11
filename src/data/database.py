from __future__ import annotations

import sqlite3
from pathlib import Path

from src.shared.constants import APP_VERSION, DEFAULT_SCHEMA_VERSION


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS roles (
    role_id INTEGER PRIMARY KEY,
    role_name TEXT NOT NULL UNIQUE,
    description TEXT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role_id INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_utc TEXT NOT NULL,
    last_login_utc TEXT NULL,
    FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE TABLE IF NOT EXISTS sites (
    site_id INTEGER PRIMARY KEY,
    site_name TEXT NOT NULL UNIQUE,
    description TEXT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL,
    display_order INTEGER NOT NULL DEFAULT 0,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    description TEXT NULL,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checks (
    check_id INTEGER PRIMARY KEY,
    internal_name TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    description TEXT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    display_order INTEGER NOT NULL DEFAULT 0,
    applies_to_site1 INTEGER NOT NULL DEFAULT 1,
    applies_to_site2 INTEGER NOT NULL DEFAULT 1,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL,
    created_by_user_id INTEGER NULL,
    updated_by_user_id INTEGER NULL,
    troubleshooting_tier1_url TEXT NULL,
    troubleshooting_tier2_url TEXT NULL,
    graph_type TEXT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(group_id),
    FOREIGN KEY (created_by_user_id) REFERENCES users(user_id),
    FOREIGN KEY (updated_by_user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS check_source_config (
    check_source_config_id INTEGER PRIMARY KEY,
    check_id INTEGER NOT NULL UNIQUE,
    source_type TEXT NOT NULL,
    relative_path_site1 TEXT NULL,
    relative_path_site2 TEXT NULL,
    file_pattern TEXT NULL,
    parser_type TEXT NOT NULL,
    match_strategy TEXT NULL,
    case_sensitive INTEGER NOT NULL DEFAULT 0,
    target_pattern TEXT NULL,
    secondary_pattern TEXT NULL,
    notes TEXT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS check_rule_config (
    check_rule_config_id INTEGER PRIMARY KEY,
    check_id INTEGER NOT NULL UNIQUE,
    rule_type TEXT NOT NULL,
    operator TEXT NULL,
    threshold_min REAL NULL,
    threshold_max REAL NULL,
    expected_interval_seconds INTEGER NULL,
    stale_timeout_seconds INTEGER NOT NULL,
    grace_period_seconds INTEGER NOT NULL DEFAULT 0,
    pass_text TEXT NULL,
    fail_text TEXT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS current_check_status (
    check_id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL,
    mode_name TEXT NOT NULL,
    operational_state TEXT NOT NULL,
    condition_state TEXT NULL,
    freshness_state TEXT NULL,
    alert_state TEXT NOT NULL,
    is_acknowledged INTEGER NOT NULL DEFAULT 0,
    owner_user_id INTEGER NULL,
    severity TEXT NOT NULL DEFAULT 'Medium',
    active_alert_instance_id INTEGER NULL,
    last_result_utc TEXT NULL,
    last_source_modified_utc TEXT NULL,
    last_source_age_seconds INTEGER NULL,
    last_parse_status TEXT NULL,
    last_value_text TEXT NULL,
    last_value_numeric REAL NULL,
    last_detail_message TEXT NULL,
    updated_utc TEXT NOT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id),
    FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE TABLE IF NOT EXISTS check_results (
    result_id INTEGER PRIMARY KEY,
    check_id INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    mode_name TEXT NOT NULL,
    evaluated_utc TEXT NOT NULL,
    source_file_path TEXT NULL,
    source_modified_utc TEXT NULL,
    source_age_seconds INTEGER NULL,
    parse_status TEXT NOT NULL,
    value_type TEXT NULL,
    value_text TEXT NULL,
    value_numeric REAL NULL,
    rule_type TEXT NOT NULL,
    rule_outcome TEXT NOT NULL,
    operational_state TEXT NOT NULL,
    condition_state TEXT NULL,
    freshness_state TEXT NULL,
    detail_message TEXT NOT NULL,
    technical_detail TEXT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id),
    FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE TABLE IF NOT EXISTS alert_instances (
    alert_instance_id INTEGER PRIMARY KEY,
    check_id INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    mode_name TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_acknowledged INTEGER NOT NULL DEFAULT 0,
    owner_user_id INTEGER NULL,
    severity TEXT NOT NULL DEFAULT 'Medium',
    acknowledged_by_user_id INTEGER NULL,
    acknowledged_utc TEXT NULL,
    acknowledgment_note TEXT NULL,
    is_escalated INTEGER NOT NULL DEFAULT 0,
    escalated_by_user_id INTEGER NULL,
    escalated_utc TEXT NULL,
    escalation_note TEXT NULL,
    start_message TEXT NOT NULL,
    clear_message TEXT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id),
    FOREIGN KEY (site_id) REFERENCES sites(site_id),
    FOREIGN KEY (acknowledged_by_user_id) REFERENCES users(user_id),
    FOREIGN KEY (escalated_by_user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    event_utc TEXT NOT NULL,
    event_type TEXT NOT NULL,
    check_id INTEGER NULL,
    alert_instance_id INTEGER NULL,
    site_id INTEGER NULL,
    mode_name TEXT NULL,
    user_id INTEGER NULL,
    message TEXT NOT NULL,
    detail TEXT NULL,
    FOREIGN KEY (check_id) REFERENCES checks(check_id),
    FOREIGN KEY (alert_instance_id) REFERENCES alert_instances(alert_instance_id),
    FOREIGN KEY (site_id) REFERENCES sites(site_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY,
    audit_utc TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NULL,
    entity_name TEXT NULL,
    old_value_json TEXT NULL,
    new_value_json TEXT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS themes (
    theme_id INTEGER PRIMARY KEY,
    theme_name TEXT NOT NULL UNIQUE,
    is_dark_mode INTEGER NOT NULL,
    background_color TEXT NOT NULL,
    panel_color TEXT NOT NULL,
    header_color TEXT NOT NULL,
    text_color TEXT NOT NULL,
    accent_color TEXT NOT NULL,
    border_color TEXT NOT NULL,
    is_system_preset INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS notification_rules (
    notification_rule_id INTEGER PRIMARY KEY,
    rule_name TEXT NOT NULL UNIQUE,
    scope_type TEXT NOT NULL DEFAULT 'Global',
    scope_value_id INTEGER NULL,
    trigger_event TEXT NOT NULL DEFAULT 'AlertStarted',
    channel_type TEXT NOT NULL DEFAULT 'FileLog',
    destination TEXT NULL,
    renotify_minutes INTEGER NOT NULL DEFAULT 0,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL,
    created_by_user_id INTEGER NULL,
    updated_by_user_id INTEGER NULL,
    FOREIGN KEY (created_by_user_id) REFERENCES users(user_id),
    FOREIGN KEY (updated_by_user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS notification_delivery_log (
    notification_delivery_id INTEGER PRIMARY KEY,
    notification_rule_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    check_id INTEGER NULL,
    alert_instance_id INTEGER NULL,
    delivered_utc TEXT NOT NULL,
    delivery_status TEXT NOT NULL,
    destination TEXT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (notification_rule_id) REFERENCES notification_rules(notification_rule_id),
    FOREIGN KEY (check_id) REFERENCES checks(check_id),
    FOREIGN KEY (alert_instance_id) REFERENCES alert_instances(alert_instance_id)
);

CREATE TABLE IF NOT EXISTS maintenance_windows (
    maintenance_window_id INTEGER PRIMARY KEY,
    window_name TEXT NOT NULL,
    scope_type TEXT NOT NULL DEFAULT 'Global',
    scope_value_id INTEGER NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    reason TEXT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL,
    created_by_user_id INTEGER NULL,
    updated_by_user_id INTEGER NULL,
    FOREIGN KEY (created_by_user_id) REFERENCES users(user_id),
    FOREIGN KEY (updated_by_user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS app_metadata (
    metadata_key TEXT PRIMARY KEY,
    metadata_value TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS application_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_utc TEXT NOT NULL,
    updated_by_user_id INTEGER NULL,
    FOREIGN KEY (updated_by_user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS user_preferences (
    user_id INTEGER PRIMARY KEY,
    theme_id INTEGER NULL,
    dashboard_density TEXT NULL,
    default_screen TEXT NULL,
    updated_utc TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (theme_id) REFERENCES themes(theme_id)
);

CREATE INDEX IF NOT EXISTS idx_checks_group_enabled ON checks(group_id, is_enabled);
CREATE INDEX IF NOT EXISTS idx_check_results_check_time ON check_results(check_id, evaluated_utc DESC);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_utc DESC);
CREATE INDEX IF NOT EXISTS idx_alert_instances_active ON alert_instances(check_id, is_active);
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_active ON maintenance_windows(is_enabled, start_utc, end_utc);
CREATE INDEX IF NOT EXISTS idx_notification_rules_enabled ON notification_rules(is_enabled, trigger_event);
"""


def create_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.DatabaseError:
        # Some shared/network locations may reject WAL mode. Continue with the default journal mode.
        pass
    try:
        conn.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.DatabaseError:
        pass
    return conn


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _ensure_column(conn, "alert_instances", "is_escalated", "is_escalated INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "alert_instances", "escalated_by_user_id", "escalated_by_user_id INTEGER NULL")
    _ensure_column(conn, "alert_instances", "escalated_utc", "escalated_utc TEXT NULL")
    _ensure_column(conn, "alert_instances", "escalation_note", "escalation_note TEXT NULL")
    _ensure_column(conn, "checks", "troubleshooting_tier1_url", "troubleshooting_tier1_url TEXT NULL")
    _ensure_column(conn, "checks", "troubleshooting_tier2_url", "troubleshooting_tier2_url TEXT NULL")
    _ensure_column(conn, "checks", "graph_type", "graph_type TEXT NULL")
    _ensure_column(conn, "current_check_status", "condition_state", "condition_state TEXT NULL")
    _ensure_column(conn, "current_check_status", "freshness_state", "freshness_state TEXT NULL")
    _ensure_column(conn, "check_results", "condition_state", "condition_state TEXT NULL")
    _ensure_column(conn, "check_results", "freshness_state", "freshness_state TEXT NULL")
    _ensure_column(conn, "alert_instances", "owner_user_id", "owner_user_id INTEGER NULL")
    _ensure_column(conn, "alert_instances", "severity", "severity TEXT NOT NULL DEFAULT 'Medium'")
    _ensure_column(conn, "current_check_status", "owner_user_id", "owner_user_id INTEGER NULL")
    _ensure_column(conn, "current_check_status", "severity", "severity TEXT NOT NULL DEFAULT 'Medium'")
    _ensure_column(conn, "checks", "is_deleted", "is_deleted INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "checks", "deleted_utc", "deleted_utc TEXT NULL")
    _ensure_column(conn, "checks", "deleted_by_user_id", "deleted_by_user_id INTEGER NULL")
    _ensure_column(conn, "checks", "deleted_payload", "deleted_payload TEXT NULL")
    _ensure_column(conn, "checks", "is_retired", "is_retired INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "checks", "retired_utc", "retired_utc TEXT NULL")
    _ensure_column(conn, "checks", "retired_by_user_id", "retired_by_user_id INTEGER NULL")
    _ensure_column(conn, "groups", "is_deleted", "is_deleted INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "groups", "deleted_utc", "deleted_utc TEXT NULL")
    _ensure_column(conn, "groups", "deleted_by_user_id", "deleted_by_user_id INTEGER NULL")
    _ensure_column(conn, "groups", "deleted_payload", "deleted_payload TEXT NULL")
    _ensure_column(conn, "groups", "is_retired", "is_retired INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "groups", "retired_utc", "retired_utc TEXT NULL")
    _ensure_column(conn, "groups", "retired_by_user_id", "retired_by_user_id INTEGER NULL")
    _set_metadata(conn, "schema_version", str(DEFAULT_SCHEMA_VERSION))
    _set_metadata(conn, "last_used_app_version", APP_VERSION)
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO app_metadata(metadata_key, metadata_value, updated_utc)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(metadata_key) DO UPDATE SET
            metadata_value=excluded.metadata_value,
            updated_utc=CURRENT_TIMESTAMP
        """,
        (key, value),
    )
