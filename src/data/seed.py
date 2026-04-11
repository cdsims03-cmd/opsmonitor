from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timedelta, timezone



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()



def hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}${digest.hex()}"



def verify_password(password: str, stored_hash: str) -> bool:
    salt_hex, digest_hex = stored_hash.split("$", 1)
    salt = bytes.fromhex(salt_hex)
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return candidate.hex() == digest_hex



def seed_database(conn: sqlite3.Connection, default_theme: str) -> None:
    now = _utc_now()

    roles = [
        ("Viewer", "Read-only access"),
        ("Operator", "Can acknowledge alerts and switch site"),
        ("ConfigAdmin", "Can manage checks and groups"),
        ("SystemAdmin", "Full administrative access"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO roles(role_name, description) VALUES(?, ?)", roles
    )

    sites = [
        (1, "Site 1", "Site 1 monitored environment"),
        (2, "Site 2", "Site 2 monitored environment"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO sites(site_id, site_name, description) VALUES(?, ?, ?)", sites
    )

    themes = [
        ("Dark Slate", 1, "#10151D", "#171E28", "#1E293B", "#E5E7EB", "#4F7DF3", "#2B3442", 1),
        ("Midnight Blue", 1, "#0D1625", "#142033", "#1C3252", "#E6EEF8", "#4F8BD8", "#29364A", 1),
        ("Graphite Gray", 1, "#121416", "#1B1F23", "#2B3138", "#EDF1F5", "#5B7FA3", "#323840", 1),
        ("Deep Teal", 1, "#0D1718", "#142224", "#1D3C40", "#E6F5F3", "#4F8F89", "#274045", 1),
        ("Muted Purple", 1, "#14131A", "#1D1A26", "#322B45", "#F0EBF7", "#7A6FA5", "#393247", 1),
        ("Soft Navy", 1, "#101722", "#182130", "#21334B", "#E8EDF5", "#6686B5", "#2C394C", 1),
        ("Charcoal Blue", 1, "#11161B", "#1A2129", "#243343", "#E5ECF4", "#5A86A8", "#2F3A46", 1),
        ("Cool Gray", 1, "#131518", "#1C1F24", "#303641", "#ECEFF4", "#6F86A1", "#353B45", 1),
        ("Steel Blue", 1, "#10161D", "#18212B", "#27394B", "#EAF0F7", "#6184A8", "#2E3A47", 1),
        ("Dark Olive", 1, "#151711", "#1F231A", "#333A24", "#EFF2E9", "#7A8D5B", "#3B4330", 1),
        ("Deep Orange", 1, "#18120E", "#241A14", "#3A2417", "#F7EEE8", "#C96B2C", "#4B3428", 1),
        ("Wabtec Dark", 1, "#1E1E1E", "#2A2A2A", "#B00020", "#F2F2F2", "#D32F2F", "#444444", 1),
        ("Wabtec Light", 0, "#F5F5F5", "#FFFFFF", "#B00020", "#222222", "#D32F2F", "#CCCCCC", 1),
        ("Neutral Dark", 1, "#202124", "#2D2F31", "#3C4043", "#E8EAED", "#8AB4F8", "#5F6368", 1),
        ("High Contrast", 1, "#000000", "#101010", "#B00020", "#FFFFFF", "#FFFFFF", "#FFFFFF", 1),
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO themes(
            theme_name, is_dark_mode, background_color, panel_color, header_color,
            text_color, accent_color, border_color, is_system_preset
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        themes,
    )

    role_row = conn.execute(
        "SELECT role_id FROM roles WHERE role_name = ?", ("SystemAdmin",)
    ).fetchone()
    if role_row is None:
        raise RuntimeError("SystemAdmin role missing during seed")

    existing_admin = conn.execute(
        "SELECT 1 FROM users WHERE username = ?", ("admin",)
    ).fetchone()
    if existing_admin is None:
        conn.execute(
            """
            INSERT INTO users(username, display_name, password_hash, role_id, is_active, created_utc)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            ("admin", "Administrator", hash_password("admin123"), int(role_row["role_id"]), now),
        )

    defaults = {
        "active_mode": "Test",
        "active_site_id": "1",
        "test_data_root_path": "TestData",
        "live_data_root_site1": "",
        "live_data_root_site2": "",
        "dashboard_density": "Standard",
        "default_theme": "Charcoal Blue",
        "notification_log_path": "Logs/notifications.log",
    }
    for key, value in defaults.items():
        conn.execute(
            """
            INSERT OR IGNORE INTO application_settings(setting_key, setting_value, updated_utc)
            VALUES (?, ?, ?)
            """,
            (key, value, now),
        )

    admin_id = int(conn.execute("SELECT user_id FROM users WHERE username = 'admin'").fetchone()["user_id"])
    _seed_dashboard_demo_data(conn)
    _seed_phase2_defaults(conn, admin_id)
    conn.commit()



def _seed_dashboard_demo_data(conn: sqlite3.Connection) -> None:
    existing = conn.execute("SELECT COUNT(1) AS c FROM groups").fetchone()
    if existing and int(existing["c"]) > 0:
        return

    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()

    groups = [
        ("system_checks", "System Checks", 1, 1, "Core platform checks", now, now),
        ("regional_services", "Regional Services", 2, 1, "Regional service health", now, now),
        ("performance", "Performance", 3, 1, "Performance indicators", now, now),
    ]
    conn.executemany(
        """
        INSERT INTO groups(group_name, display_label, display_order, is_enabled, description, created_utc, updated_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        groups,
    )

    group_ids = {
        row["group_name"]: int(row["group_id"])
        for row in conn.execute("SELECT group_id, group_name FROM groups")
    }
    admin_id = int(conn.execute("SELECT user_id FROM users WHERE username = 'admin'").fetchone()["user_id"])

    checks = [
        ("check_q", "Check Queue", group_ids["system_checks"], "Queue backlog monitor", 1, 1, 1, 1, now, now, admin_id, admin_id),
        ("hung_locks", "Hung Locks", group_ids["system_checks"], "Hung lock indicator", 1, 2, 1, 1, now, now, admin_id, admin_id),
        ("mp_region_1", "MP Region 1", group_ids["regional_services"], "Regional status monitor", 1, 1, 1, 1, now, now, admin_id, admin_id),
        ("mp_region_3", "MP Region 3", group_ids["regional_services"], "Regional status monitor", 1, 2, 1, 1, now, now, admin_id, admin_id),
        ("versant_cpu", "Versant CPU", group_ids["performance"], "CPU trend monitor", 1, 1, 1, 1, now, now, admin_id, admin_id),
    ]
    conn.executemany(
        """
        INSERT INTO checks(
            internal_name, display_label, group_id, description, is_enabled, display_order,
            applies_to_site1, applies_to_site2, created_utc, updated_utc, created_by_user_id, updated_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        checks,
    )

    check_rows = {row["internal_name"]: int(row["check_id"]) for row in conn.execute("SELECT check_id, internal_name FROM checks")}

    source_configs = [
        (check_rows["check_q"], "File", "checkQ.log", "checkQ.log", None, "IntegerFromPattern", "FirstMatch", 0, "Count:", None, None),
        (check_rows["hung_locks"], "File", "hunglocks.log", "hunglocks.log", None, "RawText", "FirstMatch", 0, "ERROR", None, None),
        (check_rows["mp_region_1"], "File", "mpReg1.log", "mpReg1.log", None, "RawText", "FirstMatch", 0, "OK", None, None),
        (check_rows["mp_region_3"], "File", "mpReg3.log", "mpReg3.log", None, "RawText", "FirstMatch", 0, "OK", None, None),
        (check_rows["versant_cpu"], "File", "versantcpu.log", "versantcpu.log", None, "DecimalFromPattern", "FirstMatch", 0, "CPU:", None, None),
    ]
    conn.executemany(
        """
        INSERT INTO check_source_config(
            check_id, source_type, relative_path_site1, relative_path_site2, file_pattern,
            parser_type, match_strategy, case_sensitive, target_pattern, secondary_pattern, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        source_configs,
    )

    rule_configs = [
        (check_rows["check_q"], "NumericCompare", ">", None, 10, 30, 90, 0, None, "Count too high"),
        (check_rows["hung_locks"], "TextNotExists", None, None, None, 60, 180, 0, "No hung locks", "Hung lock text found"),
        (check_rows["mp_region_1"], "TextExists", None, None, None, 30, 90, 0, "Region healthy", "Region missing OK marker"),
        (check_rows["mp_region_3"], "TextExists", None, None, None, 30, 90, 0, "Region healthy", "Region missing OK marker"),
        (check_rows["versant_cpu"], "NumericRange", None, 0, 80, 60, 300, 0, "CPU normal", "CPU outside normal range"),
    ]
    conn.executemany(
        """
        INSERT INTO check_rule_config(
            check_id, rule_type, operator, threshold_min, threshold_max,
            expected_interval_seconds, stale_timeout_seconds, grace_period_seconds, pass_text, fail_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rule_configs,
    )

    active_alert_id = conn.execute(
        """
        INSERT INTO alert_instances(
            check_id, site_id, mode_name, alert_type, start_utc, is_active, is_acknowledged,
            acknowledged_by_user_id, acknowledged_utc, acknowledgment_note, start_message
        ) VALUES (?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?)
        RETURNING alert_instance_id
        """,
        (
            check_rows["check_q"],
            1,
            "Test",
            "Threshold",
            (now_dt - timedelta(minutes=12)).isoformat(),
            admin_id,
            (now_dt - timedelta(minutes=10)).isoformat(),
            "Investigating test threshold",
            "Check Queue exceeded threshold",
        ),
    ).fetchone()["alert_instance_id"]

    current_status = [
        (check_rows["check_q"], 1, "Test", "Unhealthy", "ActiveAcknowledged", 1, active_alert_id,
         (now_dt - timedelta(seconds=15)).isoformat(), (now_dt - timedelta(seconds=20)).isoformat(), 20,
         "Success", None, 12, "Queue High 12 > 10", now),
        (check_rows["hung_locks"], 1, "Test", "Healthy", "None", 0, None,
         (now_dt - timedelta(seconds=40)).isoformat(), (now_dt - timedelta(seconds=40)).isoformat(), 40,
         "Success", "No errors found", None, "No hung locks detected", now),
        (check_rows["mp_region_1"], 1, "Test", "Healthy", "None", 0, None,
         (now_dt - timedelta(seconds=25)).isoformat(), (now_dt - timedelta(seconds=25)).isoformat(), 25,
         "Success", "OK", None, "Region healthy", now),
        (check_rows["mp_region_3"], 1, "Test", "Stale", "ActiveUnacknowledged", 0, None,
         (now_dt - timedelta(minutes=22)).isoformat(), (now_dt - timedelta(minutes=22)).isoformat(), 1320,
         "NoInput", None, None, "Stale 22m > 90s", now),
        (check_rows["versant_cpu"], 1, "Test", "Healthy", "None", 0, None,
         (now_dt - timedelta(seconds=55)).isoformat(), (now_dt - timedelta(seconds=60)).isoformat(), 60,
         "Success", None, 42.6, "CPU normal 42.6%", now),
    ]
    conn.executemany(
        """
        INSERT INTO current_check_status(
            check_id, site_id, mode_name, operational_state, alert_state, is_acknowledged,
            active_alert_instance_id, last_result_utc, last_source_modified_utc, last_source_age_seconds,
            last_parse_status, last_value_text, last_value_numeric, last_detail_message, updated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        current_status,
    )

    result_rows = [
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(days=20)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(days=20, seconds=5)).isoformat(), 5, "Success", "number", None, 6, "NumericCompare", "Passed", "Healthy", "Queue OK 6 <= 10", None),
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(days=7)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(days=7, seconds=5)).isoformat(), 5, "Success", "number", None, 8, "NumericCompare", "Passed", "Healthy", "Queue OK 8 <= 10", None),
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(days=1)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(days=1, seconds=5)).isoformat(), 5, "Success", "number", None, 9, "NumericCompare", "Passed", "Healthy", "Queue OK 9 <= 10", None),
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(minutes=11)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(minutes=11, seconds=5)).isoformat(), 5, "Success", "number", None, 9, "NumericCompare", "Passed", "Healthy", "Queue OK 9 <= 10", None),
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(minutes=6)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(minutes=6, seconds=5)).isoformat(), 5, "Success", "number", None, 11, "NumericCompare", "Failed", "Unhealthy", "Queue High 11 > 10", None),
        (check_rows["check_q"], 1, "Test", (now_dt - timedelta(seconds=15)).isoformat(), "TestData/Site1/checkQ.log", (now_dt - timedelta(seconds=20)).isoformat(), 20, "Success", "number", None, 12, "NumericCompare", "Failed", "Unhealthy", "Queue High 12 > 10", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(days=25)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(days=25)).isoformat(), 0, "Success", "number", None, 31.8, "NumericRange", "Passed", "Healthy", "CPU normal 31.8%", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(days=14)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(days=14)).isoformat(), 0, "Success", "number", None, 39.4, "NumericRange", "Passed", "Healthy", "CPU normal 39.4%", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(days=3)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(days=3)).isoformat(), 0, "Success", "number", None, 37.5, "NumericRange", "Passed", "Healthy", "CPU normal 37.5%", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(minutes=10)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(minutes=10)).isoformat(), 0, "Success", "number", None, 37.5, "NumericRange", "Passed", "Healthy", "CPU normal 37.5%", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(minutes=5)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(minutes=5)).isoformat(), 0, "Success", "number", None, 45.2, "NumericRange", "Passed", "Healthy", "CPU normal 45.2%", None),
        (check_rows["versant_cpu"], 1, "Test", (now_dt - timedelta(seconds=55)).isoformat(), "TestData/Site1/versantcpu.log", (now_dt - timedelta(seconds=60)).isoformat(), 60, "Success", "number", None, 42.6, "NumericRange", "Passed", "Healthy", "CPU normal 42.6%", None),
    ]
    conn.executemany(
        """
        INSERT INTO check_results(
            check_id, site_id, mode_name, evaluated_utc, source_file_path, source_modified_utc,
            source_age_seconds, parse_status, value_type, value_text, value_numeric,
            rule_type, rule_outcome, operational_state, detail_message, technical_detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        result_rows,
    )

    events = [
        ((now_dt - timedelta(minutes=12)).isoformat(), "AlertStarted", check_rows["check_q"], active_alert_id, 1, "Test", admin_id, "Check Queue exceeded threshold", "Queue rose above threshold"),
        ((now_dt - timedelta(minutes=10)).isoformat(), "AlertAcknowledged", check_rows["check_q"], active_alert_id, 1, "Test", admin_id, "Check Queue acknowledged by Administrator", "Investigating test threshold"),
        ((now_dt - timedelta(minutes=2)).isoformat(), "StaleStarted", check_rows["mp_region_3"], None, 1, "Test", None, "MP Region 3 entered stale state", "No fresh file received"),
        ((now_dt - timedelta(minutes=1)).isoformat(), "Info", check_rows["versant_cpu"], None, 1, "Test", None, "Versant CPU healthy at 42.6%", None),
    ]
    conn.executemany(
        """
        INSERT INTO events(event_utc, event_type, check_id, alert_instance_id, site_id, mode_name, user_id, message, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        events,
    )

    audit_entries = [
        ((now_dt - timedelta(minutes=15)).isoformat(), admin_id, "SeedDemoData", "System", None, "Demo setup", None, None, "Inserted Step 2 demo monitoring data"),
    ]
    conn.executemany(
        """
        INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_id, entity_name, old_value_json, new_value_json, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        audit_entries,
    )


def _seed_phase2_defaults(conn: sqlite3.Connection, admin_id: int) -> None:
    now = _utc_now()
    existing = conn.execute("SELECT COUNT(1) AS c FROM notification_rules").fetchone()
    if existing and int(existing["c"] or 0) == 0:
        conn.execute(
            """
            INSERT INTO notification_rules(
                rule_name, scope_type, scope_value_id, trigger_event, channel_type, destination,
                renotify_minutes, is_enabled, created_utc, updated_utc, created_by_user_id, updated_by_user_id
            ) VALUES (?, 'Global', NULL, 'AlertStarted', 'FileLog', ?, 0, 1, ?, ?, ?, ?)
            """,
            ("Default Alert Started", "Logs/notifications.log", now, now, admin_id, admin_id),
        )
        conn.execute(
            """
            INSERT INTO notification_rules(
                rule_name, scope_type, scope_value_id, trigger_event, channel_type, destination,
                renotify_minutes, is_enabled, created_utc, updated_utc, created_by_user_id, updated_by_user_id
            ) VALUES (?, 'Global', NULL, 'AlertCleared', 'FileLog', ?, 0, 1, ?, ?, ?, ?)
            """,
            ("Default Alert Cleared", "Logs/notifications.log", now, now, admin_id, admin_id),
        )
