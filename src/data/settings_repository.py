from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class SettingsRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def list_themes(self) -> list[sqlite3.Row]:
        return list(self._conn.execute("SELECT theme_id, theme_name, is_dark_mode FROM themes ORDER BY theme_name"))

    def get_audit_entries(self, limit: int = 200) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT a.audit_utc, a.action_type, a.entity_type, a.entity_name, a.message, u.display_name AS user_name
                FROM audit_log a
                LEFT JOIN users u ON u.user_id = a.user_id
                ORDER BY a.audit_utc DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def get_setting(self, key: str, default: str) -> str:
        row = self._conn.execute(
            "SELECT setting_value FROM application_settings WHERE setting_key = ?",
            (key,),
        ).fetchone()
        return str(row["setting_value"]) if row else default

    def set_setting(self, key: str, value: str, user_id: int | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO application_settings(setting_key, setting_value, updated_utc, updated_by_user_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET
                setting_value=excluded.setting_value,
                updated_utc=excluded.updated_utc,
                updated_by_user_id=excluded.updated_by_user_id
            """,
            (key, value, now, user_id),
        )
        self._conn.commit()

    def get_retention_days(self) -> int:
        raw = self.get_setting("retention_days", "90")
        if raw.lower() == "keep":
            return 0
        try:
            return max(0, int(raw))
        except ValueError:
            return 90

    def run_retention_cleanup(self, user_id: int | None = None, force: bool = False) -> dict[str, int | str]:
        retention_days = self.get_retention_days()
        now = datetime.now(timezone.utc).isoformat()
        last_run = self.get_setting("retention_last_run_utc", "")
        if not force and last_run:
            try:
                last_dt = datetime.fromisoformat(last_run)
                if (datetime.now(timezone.utc) - last_dt).total_seconds() < 86400:
                    return {"retention_days": retention_days, "rows_purged": 0, "ran": "0", "last_run_utc": last_run}
            except ValueError:
                pass
        if retention_days <= 0:
            self.set_setting("retention_last_run_utc", now, user_id)
            self.set_setting("retention_last_rows_purged", "0", user_id)
            return {"retention_days": retention_days, "rows_purged": 0, "ran": "1", "last_run_utc": now}

        cutoff_expr = f"datetime('now', '-{retention_days} days')"
        total = 0
        statements = [
            ("notification_delivery_log", f"DELETE FROM notification_delivery_log WHERE datetime(replace(substr(delivered_utc,1,19),'T',' ')) < {cutoff_expr}"),
            ("audit_log", f"DELETE FROM audit_log WHERE datetime(replace(substr(audit_utc,1,19),'T',' ')) < {cutoff_expr}"),
            ("events", f"DELETE FROM events WHERE datetime(replace(substr(event_utc,1,19),'T',' ')) < {cutoff_expr}"),
            ("check_results", f"DELETE FROM check_results WHERE datetime(replace(substr(evaluated_utc,1,19),'T',' ')) < {cutoff_expr}"),
            ("closed_alerts", f"DELETE FROM alert_instances WHERE is_active = 0 AND end_utc IS NOT NULL AND datetime(replace(substr(end_utc,1,19),'T',' ')) < {cutoff_expr}"),
        ]
        for _, sql in statements:
            cur = self._conn.execute(sql)
            total += int(cur.rowcount if cur.rowcount != -1 else 0)
        self._conn.commit()
        self.set_setting("retention_last_run_utc", now, user_id)
        self.set_setting("retention_last_rows_purged", str(total), user_id)
        return {"retention_days": retention_days, "rows_purged": total, "ran": "1", "last_run_utc": now}
