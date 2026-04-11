from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class MonitoringCheckConfig:
    check_id: int
    internal_name: str
    display_label: str
    group_id: int
    description: str | None
    source_type: str
    relative_path_site1: str | None
    relative_path_site2: str | None
    parser_type: str
    match_strategy: str | None
    case_sensitive: bool
    target_pattern: str | None
    secondary_pattern: str | None
    rule_type: str
    operator: str | None
    threshold_min: float | None
    threshold_max: float | None
    expected_interval_seconds: int | None
    stale_timeout_seconds: int
    grace_period_seconds: int
    pass_text: str | None
    fail_text: str | None


class MonitoringRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_active_mode(self) -> str:
        return self._get_setting("active_mode", "Test")

    def get_active_site_id(self) -> int:
        return int(self._get_setting("active_site_id", "1"))

    def get_test_data_root(self) -> str:
        return self._get_setting("test_data_root_path", "TestData")

    def get_live_data_root(self, site_id: int) -> str:
        key = "live_data_root_site1" if site_id == 1 else "live_data_root_site2"
        return self._get_setting(key, "")

    def get_enabled_checks_for_site(self, site_id: int) -> list[MonitoringCheckConfig]:
        site_column = "c.applies_to_site1" if site_id == 1 else "c.applies_to_site2"
        rows = self._conn.execute(
            f"""
            SELECT
                c.check_id,
                c.internal_name,
                c.display_label,
                c.group_id,
                c.description,
                sc.source_type,
                sc.relative_path_site1,
                sc.relative_path_site2,
                sc.parser_type,
                sc.match_strategy,
                sc.case_sensitive,
                sc.target_pattern,
                sc.secondary_pattern,
                rc.rule_type,
                rc.operator,
                rc.threshold_min,
                rc.threshold_max,
                rc.expected_interval_seconds,
                rc.stale_timeout_seconds,
                rc.grace_period_seconds,
                rc.pass_text,
                rc.fail_text
            FROM checks c
            JOIN check_source_config sc ON sc.check_id = c.check_id
            JOIN check_rule_config rc ON rc.check_id = c.check_id
            WHERE c.is_enabled = 1 AND {site_column} = 1
            ORDER BY c.display_order, c.display_label
            """
        ).fetchall()
        return [
            MonitoringCheckConfig(
                check_id=int(row["check_id"]),
                internal_name=str(row["internal_name"]),
                display_label=str(row["display_label"]),
                group_id=int(row["group_id"]),
                description=str(row["description"]) if row["description"] else None,
                source_type=str(row["source_type"]),
                relative_path_site1=str(row["relative_path_site1"]) if row["relative_path_site1"] else None,
                relative_path_site2=str(row["relative_path_site2"]) if row["relative_path_site2"] else None,
                parser_type=str(row["parser_type"]),
                match_strategy=str(row["match_strategy"]) if row["match_strategy"] else None,
                case_sensitive=bool(row["case_sensitive"]),
                target_pattern=str(row["target_pattern"]) if row["target_pattern"] else None,
                secondary_pattern=str(row["secondary_pattern"]) if row["secondary_pattern"] else None,
                rule_type=str(row["rule_type"]),
                operator=str(row["operator"]) if row["operator"] else None,
                threshold_min=float(row["threshold_min"]) if row["threshold_min"] is not None else None,
                threshold_max=float(row["threshold_max"]) if row["threshold_max"] is not None else None,
                expected_interval_seconds=int(row["expected_interval_seconds"]) if row["expected_interval_seconds"] is not None else None,
                stale_timeout_seconds=int(row["stale_timeout_seconds"]),
                grace_period_seconds=int(row["grace_period_seconds"]),
                pass_text=str(row["pass_text"]) if row["pass_text"] else None,
                fail_text=str(row["fail_text"]) if row["fail_text"] else None,
            )
            for row in rows
        ]

    def get_open_alert(self, check_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM alert_instances WHERE check_id = ? AND is_active = 1 ORDER BY start_utc DESC LIMIT 1",
            (check_id,),
        ).fetchone()

    def acknowledge_alert(self, check_id: int, user_id: int, note: str | None = None) -> bool:
        open_alert = self.get_open_alert(check_id)
        if open_alert is None or int(open_alert["is_acknowledged"] or 0) == 1:
            return False

        now_utc = datetime.now(timezone.utc).isoformat()
        site_id = int(open_alert["site_id"])
        mode_name = str(open_alert["mode_name"])

        self._conn.execute(
            """
            UPDATE alert_instances
            SET is_acknowledged = 1,
                acknowledged_by_user_id = ?,
                acknowledged_utc = ?
            WHERE alert_instance_id = ?
            """,
            (user_id, now_utc, int(open_alert["alert_instance_id"])),
        )
        self._conn.execute(
            """
            UPDATE current_check_status
            SET is_acknowledged = 1,
                alert_state = 'ActiveAcknowledged',
                updated_utc = ?
            WHERE check_id = ?
            """,
            (now_utc, check_id),
        )
        user_row = self._conn.execute("SELECT display_name FROM users WHERE user_id = ?", (user_id,)).fetchone()
        user_name = str(user_row["display_name"]) if user_row else f"User {user_id}"
        self.insert_event(
            {
                "event_utc": now_utc,
                "event_type": "AlertAcknowledged",
                "check_id": check_id,
                "alert_instance_id": int(open_alert["alert_instance_id"]),
                "site_id": site_id,
                "mode_name": mode_name,
                "user_id": user_id,
                "message": f"Alert acknowledged by {user_name}",
                "detail": note,
            }
        )
        self.insert_audit_log(
            {
                "audit_utc": now_utc,
                "user_id": user_id,
                "action_type": "AlertAcknowledged",
                "entity_type": "AlertInstance",
                "entity_id": str(open_alert["alert_instance_id"]),
                "entity_name": str(check_id),
                "old_value_json": '{"is_acknowledged": false}',
                "new_value_json": '{"is_acknowledged": true}',
                "message": f"User {user_name} acknowledged alert for check {check_id}",
            }
        )
        self._conn.commit()
        return True

    def escalate_alert(self, check_id: int, user_id: int) -> bool:
        open_alert = self.get_open_alert(check_id)
        if open_alert is None:
            return False

        now_utc = datetime.now(timezone.utc).isoformat()
        site_id = int(open_alert["site_id"])
        mode_name = str(open_alert["mode_name"])
        alert_instance_id = int(open_alert["alert_instance_id"])

        user_row = self._conn.execute("SELECT display_name FROM users WHERE user_id = ?", (user_id,)).fetchone()
        user_name = str(user_row["display_name"]) if user_row else f"User {user_id}"

        if int(open_alert["is_acknowledged"] or 0) == 0:
            self._conn.execute(
                """
                UPDATE alert_instances
                SET is_acknowledged = 1,
                    acknowledged_by_user_id = ?,
                    acknowledged_utc = ?,
                    acknowledgment_note = ?
                WHERE alert_instance_id = ?
                """,
                (user_id, now_utc, note, alert_instance_id),
            )
            self.insert_event(
                {
                    "event_utc": now_utc,
                    "event_type": "AlertAcknowledged",
                    "check_id": check_id,
                    "alert_instance_id": alert_instance_id,
                    "site_id": site_id,
                    "mode_name": mode_name,
                    "user_id": user_id,
                    "message": f"Alert auto-acknowledged during escalation by {user_name}",
                    "detail": None,
                }
            )

        self._conn.execute(
            """
            UPDATE alert_instances
            SET is_escalated = 1,
                escalated_by_user_id = ?,
                escalated_utc = ?
            WHERE alert_instance_id = ?
            """,
            (user_id, now_utc, alert_instance_id),
        )
        self._conn.execute(
            """
            UPDATE current_check_status
            SET is_acknowledged = 1,
                alert_state = 'ActiveAcknowledged',
                updated_utc = ?
            WHERE check_id = ?
            """,
            (now_utc, check_id),
        )
        self.insert_event(
            {
                "event_utc": now_utc,
                "event_type": "AlertEscalated",
                "check_id": check_id,
                "alert_instance_id": alert_instance_id,
                "site_id": site_id,
                "mode_name": mode_name,
                "user_id": user_id,
                "message": f"Alert escalated to support engineer by {user_name}",
                "detail": None,
            }
        )
        self.insert_audit_log(
            {
                "audit_utc": now_utc,
                "user_id": user_id,
                "action_type": "AlertEscalated",
                "entity_type": "AlertInstance",
                "entity_id": str(alert_instance_id),
                "entity_name": str(check_id),
                "old_value_json": '{"is_escalated": false}',
                "new_value_json": '{"is_escalated": true}',
                "message": f"User {user_name} escalated alert for check {check_id}",
            }
        )
        self._conn.commit()
        return True

    def upsert_current_status(self, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO current_check_status(
                check_id, site_id, mode_name, operational_state, condition_state, freshness_state, alert_state, is_acknowledged, owner_user_id, severity,
                active_alert_instance_id, last_result_utc, last_source_modified_utc, last_source_age_seconds,
                last_parse_status, last_value_text, last_value_numeric, last_detail_message, updated_utc
            ) VALUES (
                :check_id, :site_id, :mode_name, :operational_state, :condition_state, :freshness_state, :alert_state, :is_acknowledged, :owner_user_id, :severity,
                :active_alert_instance_id, :last_result_utc, :last_source_modified_utc, :last_source_age_seconds,
                :last_parse_status, :last_value_text, :last_value_numeric, :last_detail_message, :updated_utc
            )
            ON CONFLICT(check_id) DO UPDATE SET
                site_id=excluded.site_id,
                mode_name=excluded.mode_name,
                operational_state=excluded.operational_state,
                condition_state=excluded.condition_state,
                freshness_state=excluded.freshness_state,
                alert_state=excluded.alert_state,
                is_acknowledged=excluded.is_acknowledged,
                owner_user_id=excluded.owner_user_id,
                severity=excluded.severity,
                active_alert_instance_id=excluded.active_alert_instance_id,
                last_result_utc=excluded.last_result_utc,
                last_source_modified_utc=excluded.last_source_modified_utc,
                last_source_age_seconds=excluded.last_source_age_seconds,
                last_parse_status=excluded.last_parse_status,
                last_value_text=excluded.last_value_text,
                last_value_numeric=excluded.last_value_numeric,
                last_detail_message=excluded.last_detail_message,
                updated_utc=excluded.updated_utc
            """,
            payload,
        )

    def insert_check_result(self, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO check_results(
                check_id, site_id, mode_name, evaluated_utc, source_file_path, source_modified_utc,
                source_age_seconds, parse_status, value_type, value_text, value_numeric,
                rule_type, rule_outcome, operational_state, condition_state, freshness_state, detail_message, technical_detail
            ) VALUES (
                :check_id, :site_id, :mode_name, :evaluated_utc, :source_file_path, :source_modified_utc,
                :source_age_seconds, :parse_status, :value_type, :value_text, :value_numeric,
                :rule_type, :rule_outcome, :operational_state, :condition_state, :freshness_state, :detail_message, :technical_detail
            )
            """,
            payload,
        )

    def create_alert_instance(self, payload: dict[str, Any]) -> int:
        row = self._conn.execute(
            """
            INSERT INTO alert_instances(
                check_id, site_id, mode_name, alert_type, start_utc, is_active, is_acknowledged,
                is_escalated, start_message, severity
            ) VALUES (
                :check_id, :site_id, :mode_name, :alert_type, :start_utc, 1, 0, 0, :start_message, :severity
            ) RETURNING alert_instance_id
            """,
            payload,
        ).fetchone()
        return int(row[0])

    def close_alert_instance(self, alert_instance_id: int, clear_utc: str, clear_message: str) -> None:
        self._conn.execute(
            """
            UPDATE alert_instances
            SET is_active = 0, end_utc = ?, clear_message = ?
            WHERE alert_instance_id = ?
            """,
            (clear_utc, clear_message, alert_instance_id),
        )

    def update_alert_instance_type(self, alert_instance_id: int, alert_type: str, start_message: str) -> None:
        self._conn.execute(
            """
            UPDATE alert_instances
            SET alert_type = ?,
                start_message = ?,
                is_acknowledged = 0,
                acknowledged_by_user_id = NULL,
                acknowledged_utc = NULL,
                acknowledgment_note = NULL,
                is_escalated = 0,
                escalated_by_user_id = NULL,
                escalated_utc = NULL,
                escalation_note = NULL
            WHERE alert_instance_id = ?
            """,
            (alert_type, start_message, alert_instance_id),
        )

    def insert_event(self, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO events(event_utc, event_type, check_id, alert_instance_id, site_id, mode_name, user_id, message, detail)
            VALUES (:event_utc, :event_type, :check_id, :alert_instance_id, :site_id, :mode_name, :user_id, :message, :detail)
            """,
            payload,
        )

    def insert_audit_log(self, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_id, entity_name, old_value_json, new_value_json, message)
            VALUES (:audit_utc, :user_id, :action_type, :entity_type, :entity_id, :entity_name, :old_value_json, :new_value_json, :message)
            """,
            payload,
        )

    def set_alert_owner(self, check_id: int, owner_user_id: int | None, actor_user_id: int) -> bool:
        row = self.get_current_status_row(check_id)
        if row is None or row["active_alert_instance_id"] is None:
            return False
        now_utc = datetime.now(timezone.utc).isoformat()
        alert_instance_id = int(row["active_alert_instance_id"])
        self._conn.execute("UPDATE alert_instances SET owner_user_id = ? WHERE alert_instance_id = ?", (owner_user_id, alert_instance_id))
        self._conn.execute("UPDATE current_check_status SET owner_user_id = ?, updated_utc = ? WHERE check_id = ?", (owner_user_id, now_utc, check_id))
        actor = self._conn.execute("SELECT display_name FROM users WHERE user_id = ?", (actor_user_id,)).fetchone()
        owner = self._conn.execute("SELECT display_name FROM users WHERE user_id = ?", (owner_user_id,)).fetchone() if owner_user_id else None
        actor_name = str(actor["display_name"]) if actor else f"User {actor_user_id}"
        owner_name = str(owner["display_name"]) if owner else "Unassigned"
        self.insert_event({
            "event_utc": now_utc, "event_type": "AlertOwnerChanged", "check_id": check_id, "alert_instance_id": alert_instance_id,
            "site_id": row["site_id"], "mode_name": row["mode_name"], "user_id": actor_user_id,
            "message": f"Alert owner set to {owner_name}", "detail": None,
        })
        self.insert_audit_log({
            "audit_utc": now_utc, "user_id": actor_user_id, "action_type": "AlertOwnerChanged", "entity_type": "AlertInstance",
            "entity_id": str(alert_instance_id), "entity_name": str(check_id), "old_value_json": None, "new_value_json": f'{{"owner_user_id": {owner_user_id if owner_user_id is not None else "null"}}}',
            "message": f"User {actor_name} set alert owner to {owner_name} for check {check_id}",
        })
        self._conn.commit()
        return True

    def set_alert_severity(self, check_id: int, severity: str, actor_user_id: int) -> bool:
        row = self.get_current_status_row(check_id)
        now_utc = datetime.now(timezone.utc).isoformat()
        self._conn.execute("UPDATE current_check_status SET severity = ?, updated_utc = ? WHERE check_id = ?", (severity, now_utc, check_id))
        alert_instance_id = row["active_alert_instance_id"] if row else None
        if alert_instance_id is not None:
            self._conn.execute("UPDATE alert_instances SET severity = ? WHERE alert_instance_id = ?", (severity, int(alert_instance_id)))
        actor = self._conn.execute("SELECT display_name FROM users WHERE user_id = ?", (actor_user_id,)).fetchone()
        actor_name = str(actor["display_name"]) if actor else f"User {actor_user_id}"
        self.insert_event({
            "event_utc": now_utc, "event_type": "AlertSeverityChanged", "check_id": check_id, "alert_instance_id": int(alert_instance_id) if alert_instance_id is not None else None,
            "site_id": row["site_id"] if row else None, "mode_name": row["mode_name"] if row else None, "user_id": actor_user_id,
            "message": f"Alert severity set to {severity}", "detail": None,
        })
        self.insert_audit_log({
            "audit_utc": now_utc, "user_id": actor_user_id, "action_type": "AlertSeverityChanged", "entity_type": "Check",
            "entity_id": str(check_id), "entity_name": str(check_id), "old_value_json": None, "new_value_json": f'{{"severity": "{severity}"}}',
            "message": f"User {actor_name} set alert severity to {severity} for check {check_id}",
        })
        self._conn.commit()
        return True

    def get_current_status_row(self, check_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM current_check_status WHERE check_id = ?",
            (check_id,),
        ).fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def _get_setting(self, key: str, default: str) -> str:
        row = self._conn.execute(
            "SELECT setting_value FROM application_settings WHERE setting_key = ?",
            (key,),
        ).fetchone()
        return str(row["setting_value"]) if row else default


    def get_active_maintenance_for_check(self, check_id: int, group_id: int | None = None) -> sqlite3.Row | None:
        now_utc = datetime.now(timezone.utc).isoformat()
        return self._conn.execute(
            """
            SELECT *
            FROM maintenance_windows
            WHERE is_enabled = 1
              AND start_utc <= ?
              AND end_utc >= ?
              AND (
                    scope_type = 'Global'
                    OR (scope_type = 'Check' AND scope_value_id = ?)
                    OR (scope_type = 'Group' AND scope_value_id = ?)
              )
            ORDER BY CASE scope_type WHEN 'Check' THEN 1 WHEN 'Group' THEN 2 ELSE 3 END, start_utc DESC
            LIMIT 1
            """,
            (now_utc, now_utc, check_id, group_id),
        ).fetchone()

    def list_notification_rules(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT * FROM notification_rules ORDER BY is_enabled DESC, rule_name"))

    def create_notification_rule(self, user_id: int, data: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO notification_rules(
                rule_name, scope_type, scope_value_id, trigger_event, channel_type, destination,
                renotify_minutes, is_enabled, created_utc, updated_utc, created_by_user_id, updated_by_user_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data['rule_name'], data.get('scope_type', 'Global'), data.get('scope_value_id'), data.get('trigger_event', 'AlertStarted'),
                data.get('channel_type', 'FileLog'), data.get('destination'), int(data.get('renotify_minutes') or 0),
                1 if data.get('is_enabled', True) else 0, now, now, user_id, user_id,
            ),
        )
        self._conn.commit()

    def update_notification_rule(self, rule_id: int, user_id: int, data: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            UPDATE notification_rules
            SET rule_name = ?, scope_type = ?, scope_value_id = ?, trigger_event = ?, channel_type = ?,
                destination = ?, renotify_minutes = ?, is_enabled = ?, updated_utc = ?, updated_by_user_id = ?
            WHERE notification_rule_id = ?
            """,
            (
                data['rule_name'], data.get('scope_type', 'Global'), data.get('scope_value_id'), data.get('trigger_event', 'AlertStarted'),
                data.get('channel_type', 'FileLog'), data.get('destination'), int(data.get('renotify_minutes') or 0),
                1 if data.get('is_enabled', True) else 0, now, user_id, rule_id,
            ),
        )
        self._conn.commit()

    def delete_notification_rule(self, rule_id: int) -> None:
        self._conn.execute("DELETE FROM notification_rules WHERE notification_rule_id = ?", (rule_id,))
        self._conn.commit()

    def list_maintenance_windows(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT * FROM maintenance_windows ORDER BY is_enabled DESC, start_utc DESC, window_name"))

    def create_maintenance_window(self, user_id: int, data: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO maintenance_windows(
                window_name, scope_type, scope_value_id, start_utc, end_utc, reason,
                is_enabled, created_utc, updated_utc, created_by_user_id, updated_by_user_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data['window_name'], data.get('scope_type', 'Global'), data.get('scope_value_id'), data['start_utc'], data['end_utc'],
                data.get('reason'), 1 if data.get('is_enabled', True) else 0, now, now, user_id, user_id,
            ),
        )
        self._conn.commit()

    def update_maintenance_window(self, window_id: int, user_id: int, data: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            UPDATE maintenance_windows
            SET window_name = ?, scope_type = ?, scope_value_id = ?, start_utc = ?, end_utc = ?, reason = ?,
                is_enabled = ?, updated_utc = ?, updated_by_user_id = ?
            WHERE maintenance_window_id = ?
            """,
            (
                data['window_name'], data.get('scope_type', 'Global'), data.get('scope_value_id'), data['start_utc'], data['end_utc'],
                data.get('reason'), 1 if data.get('is_enabled', True) else 0, now, user_id, window_id,
            ),
        )
        self._conn.commit()

    def delete_maintenance_window(self, window_id: int) -> None:
        self._conn.execute("DELETE FROM maintenance_windows WHERE maintenance_window_id = ?", (window_id,))
        self._conn.commit()

    def get_matching_notification_rules(self, trigger_event: str, check_id: int, group_id: int | None = None) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT *
                FROM notification_rules
                WHERE is_enabled = 1
                  AND trigger_event = ?
                  AND (
                        scope_type = 'Global'
                        OR (scope_type = 'Check' AND scope_value_id = ?)
                        OR (scope_type = 'Group' AND scope_value_id = ?)
                  )
                ORDER BY CASE scope_type WHEN 'Check' THEN 1 WHEN 'Group' THEN 2 ELSE 3 END, rule_name
                """,
                (trigger_event, check_id, group_id),
            )
        )

    def notification_recently_sent(self, rule_id: int, event_type: str, check_id: int, window_minutes: int) -> bool:
        if window_minutes <= 0:
            return False
        row = self._conn.execute(
            """
            SELECT delivered_utc
            FROM notification_delivery_log
            WHERE notification_rule_id = ? AND event_type = ? AND check_id = ?
            ORDER BY delivered_utc DESC
            LIMIT 1
            """,
            (rule_id, event_type, check_id),
        ).fetchone()
        if row is None or not row['delivered_utc']:
            return False
        try:
            last = datetime.fromisoformat(str(row['delivered_utc']))
        except ValueError:
            return False
        return (datetime.now(timezone.utc) - last).total_seconds() < window_minutes * 60

    def log_notification_delivery(self, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO notification_delivery_log(
                notification_rule_id, event_type, check_id, alert_instance_id, delivered_utc,
                delivery_status, destination, message
            ) VALUES (:notification_rule_id, :event_type, :check_id, :alert_instance_id, :delivered_utc,
                      :delivery_status, :destination, :message)
            """,
            payload,
        )
