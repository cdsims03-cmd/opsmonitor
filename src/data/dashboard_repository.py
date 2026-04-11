from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(slots=True)
class DashboardSummary:
    unacknowledged: int
    stale: int
    healthy: int
    disabled: int
    mode_name: str
    site_name: str


class DashboardRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_summary(self) -> DashboardSummary:
        mode_name = self._get_setting("active_mode", "Test")
        site_id = int(self._get_setting("active_site_id", "1"))
        site_row = self._conn.execute("SELECT site_name FROM sites WHERE site_id = ?", (site_id,)).fetchone()
        site_name = str(site_row["site_name"]) if site_row else "Site 1"

        unack = self._scalar(
            "SELECT COUNT(1) FROM current_check_status ccs JOIN checks c ON c.check_id = ccs.check_id WHERE COALESCE(c.is_deleted,0)=0 AND COALESCE(c.is_retired,0)=0 AND operational_state != 'Healthy' AND is_acknowledged = 0 AND alert_state != 'SuppressedMaintenance'"
        )
        stale = self._scalar(
            "SELECT COUNT(1) FROM current_check_status ccs JOIN checks c ON c.check_id = ccs.check_id WHERE COALESCE(c.is_deleted,0)=0 AND COALESCE(c.is_retired,0)=0 AND COALESCE(freshness_state, CASE WHEN operational_state = 'Stale' THEN 'Stale' ELSE 'Fresh' END) = 'Stale'"
        )
        healthy = self._scalar(
            "SELECT COUNT(1) FROM current_check_status ccs JOIN checks c ON c.check_id = ccs.check_id WHERE COALESCE(c.is_deleted,0)=0 AND COALESCE(c.is_retired,0)=0 AND operational_state = 'Healthy' AND COALESCE(freshness_state, 'Fresh') = 'Fresh'"
        )
        disabled = self._scalar("SELECT COUNT(1) FROM checks WHERE is_enabled = 0 AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0")

        return DashboardSummary(unack, stale, healthy, disabled, mode_name, site_name)

    def get_groups_with_checks(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT
                    g.group_id,
                    g.display_label AS group_label,
                    c.check_id,
                    c.display_label AS check_label,
                    c.description,
                    cs.operational_state,
                    cs.condition_state,
                    cs.freshness_state,
                    cs.alert_state,
                    cs.is_acknowledged,
                    cs.last_detail_message,
                    cs.last_result_utc,
                    cs.last_value_text,
                    cs.last_value_numeric,
                    COALESCE(a.is_escalated, 0) AS is_escalated,
                    COALESCE(ou.display_name, '') AS owner_name,
                    COALESCE(cs.severity, a.severity, 'Medium') AS severity,
                    cs.alert_state
                FROM groups g
                JOIN checks c ON c.group_id = g.group_id AND c.is_enabled = 1 AND COALESCE(c.is_deleted,0)=0 AND COALESCE(c.is_retired,0)=0
                LEFT JOIN current_check_status cs ON cs.check_id = c.check_id
                LEFT JOIN alert_instances a ON a.alert_instance_id = cs.active_alert_instance_id
                LEFT JOIN users ou ON ou.user_id = COALESCE(cs.owner_user_id, a.owner_user_id)
                WHERE g.is_enabled = 1 AND COALESCE(g.is_deleted,0)=0 AND COALESCE(g.is_retired,0)=0
                ORDER BY g.display_order, c.display_order, c.display_label
                """
            )
        )

    def get_recent_events(self, limit: int = 8) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT
                    e.event_utc,
                    e.event_type,
                    e.message,
                    e.detail,
                    c.display_label AS check_label,
                    s.site_name,
                    u.display_name AS user_name
                FROM events e
                LEFT JOIN checks c ON c.check_id = e.check_id
                LEFT JOIN sites s ON s.site_id = e.site_id
                LEFT JOIN users u ON u.user_id = e.user_id
                ORDER BY e.event_utc DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def get_check_details(self, check_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT
                c.check_id,
                c.internal_name,
                c.display_label,
                c.description,
                c.troubleshooting_tier1_url,
                c.troubleshooting_tier2_url,
                c.graph_type,
                g.display_label AS group_label,
                sc.parser_type,
                sc.target_pattern,
                rc.rule_type,
                rc.operator,
                rc.threshold_min,
                rc.threshold_max,
                rc.expected_interval_seconds,
                rc.stale_timeout_seconds,
                cs.operational_state,
                cs.condition_state,
                cs.freshness_state,
                cs.alert_state,
                cs.is_acknowledged,
                cs.last_detail_message,
                cs.last_result_utc,
                cs.last_source_age_seconds,
                cs.last_parse_status,
                cs.last_value_text,
                cs.last_value_numeric,
                a.start_utc AS alert_start_utc,
                a.acknowledged_utc,
                a.is_escalated,
                COALESCE(cs.owner_user_id, a.owner_user_id, 0) AS owner_user_id,
                COALESCE(cs.severity, a.severity, 'Medium') AS severity,
                COALESCE(ou.display_name, '') AS owner_name,
                a.escalated_utc,
                eu.display_name AS escalated_by_name,
                u.display_name AS acknowledged_by_name,
                cs.alert_state
            FROM checks c
            JOIN groups g ON g.group_id = c.group_id
            JOIN check_source_config sc ON sc.check_id = c.check_id
            JOIN check_rule_config rc ON rc.check_id = c.check_id
            LEFT JOIN current_check_status cs ON cs.check_id = c.check_id
            LEFT JOIN alert_instances a ON a.alert_instance_id = cs.active_alert_instance_id
            LEFT JOIN users u ON u.user_id = a.acknowledged_by_user_id
            LEFT JOIN users eu ON eu.user_id = a.escalated_by_user_id
            LEFT JOIN users ou ON ou.user_id = COALESCE(cs.owner_user_id, a.owner_user_id)
            WHERE c.check_id = ? AND COALESCE(c.is_deleted,0)=0 AND COALESCE(c.is_retired,0)=0
            """,
            (check_id,),
        ).fetchone()

    def get_check_recent_history(self, check_id: int, limit: int = 8) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT evaluated_utc, operational_state, condition_state, freshness_state, value_text, value_numeric, detail_message
                FROM check_results
                WHERE check_id = ?
                ORDER BY evaluated_utc DESC
                LIMIT ?
                """,
                (check_id, limit),
            )
        )

    def get_check_recent_events(self, check_id: int, limit: int = 6) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT e.event_utc, e.event_type, e.message, e.detail, u.display_name AS user_name
                FROM events e
                LEFT JOIN users u ON u.user_id = e.user_id
                WHERE e.check_id = ?
                ORDER BY e.event_utc DESC
                LIMIT ?
                """,
                (check_id, limit),
            )
        )

    def _get_setting(self, key: str, default: str) -> str:
        row = self._conn.execute(
            "SELECT setting_value FROM application_settings WHERE setting_key = ?",
            (key,),
        ).fetchone()
        return str(row["setting_value"]) if row else default

    def _scalar(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row else 0


    def get_check_history_for_range(self, check_id: int, range_key: str) -> list[sqlite3.Row]:
        now_sql = "datetime('now')"
        clause = ""
        params = [check_id]
        if range_key == "1h":
            clause = " AND datetime(replace(substr(evaluated_utc,1,19),'T',' ')) >= datetime('now', '-1 hour')"
        elif range_key == "6h":
            clause = " AND datetime(replace(substr(evaluated_utc,1,19),'T',' ')) >= datetime('now', '-6 hours')"
        elif range_key == "24h":
            clause = " AND datetime(replace(substr(evaluated_utc,1,19),'T',' ')) >= datetime('now', '-24 hours')"
        elif range_key == "7d":
            clause = " AND datetime(replace(substr(evaluated_utc,1,19),'T',' ')) >= datetime('now', '-7 days')"
        elif range_key == "30d":
            clause = " AND datetime(replace(substr(evaluated_utc,1,19),'T',' ')) >= datetime('now', '-30 days')"
        return list(
            self._conn.execute(
                f"""
                SELECT evaluated_utc, operational_state, condition_state, freshness_state, value_text, value_numeric, detail_message
                FROM check_results
                WHERE check_id = ? {clause}
                ORDER BY evaluated_utc ASC
                LIMIT 500
                """,
                params,
            )
        )
