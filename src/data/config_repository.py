from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class GroupOption:
    group_id: int
    display_label: str


class ConfigRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def list_groups(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT group_id, group_name, display_label, display_order, is_enabled, description, updated_utc
                FROM groups
                WHERE COALESCE(is_deleted, 0) = 0 AND COALESCE(is_retired, 0) = 0
                ORDER BY display_order, display_label
                """
            )
        )

    def list_group_options(self) -> list[GroupOption]:
        return [GroupOption(int(r["group_id"]), str(r["display_label"])) for r in self.list_groups() if int(r["is_enabled"]) == 1]

    def list_checks(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT
                    c.check_id,
                    c.internal_name,
                    c.display_label,
                    g.display_label AS group_label,
                    c.group_id,
                    c.is_enabled,
                    c.display_order,
                    sc.relative_path_site1,
                    sc.relative_path_site2,
                    sc.parser_type,
                    sc.target_pattern,
                    rc.rule_type,
                    rc.operator,
                    rc.threshold_min,
                    rc.threshold_max,
                    rc.expected_interval_seconds,
                    rc.stale_timeout_seconds,
                    c.troubleshooting_tier1_url,
                    c.troubleshooting_tier2_url,
                    c.graph_type
                FROM checks c
                JOIN groups g ON g.group_id = c.group_id
                JOIN check_source_config sc ON sc.check_id = c.check_id
                JOIN check_rule_config rc ON rc.check_id = c.check_id
                WHERE COALESCE(c.is_deleted, 0) = 0 AND COALESCE(c.is_retired, 0) = 0
                  AND COALESCE(g.is_deleted, 0) = 0 AND COALESCE(g.is_retired, 0) = 0
                ORDER BY g.display_order, c.display_order, c.display_label
                """
            )
        )

    def get_check(self, check_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT
                c.check_id,
                c.internal_name,
                c.display_label,
                c.group_id,
                c.description,
                c.is_enabled,
                c.display_order,
                c.applies_to_site1,
                c.applies_to_site2,
                c.troubleshooting_tier1_url,
                c.troubleshooting_tier2_url,
                c.graph_type,
                sc.relative_path_site1,
                sc.relative_path_site2,
                sc.parser_type,
                sc.target_pattern,
                sc.match_strategy,
                sc.case_sensitive,
                rc.rule_type,
                rc.operator,
                rc.threshold_min,
                rc.threshold_max,
                rc.expected_interval_seconds,
                rc.stale_timeout_seconds,
                rc.pass_text,
                rc.fail_text
            FROM checks c
            JOIN check_source_config sc ON sc.check_id = c.check_id
            JOIN check_rule_config rc ON rc.check_id = c.check_id
            WHERE c.check_id = ?
              AND COALESCE(c.is_deleted, 0) = 0
              AND COALESCE(c.is_retired, 0) = 0
            """,
            (check_id,),
        ).fetchone()

    def create_group(self, user_id: int, data: dict[str, object]) -> None:
        now = self._utc_now()
        self._conn.execute(
            """
            INSERT INTO groups(group_name, display_label, display_order, is_enabled, description, created_utc, updated_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["group_name"],
                data["display_label"],
                int(data["display_order"]),
                1 if data.get("is_enabled", True) else 0,
                data.get("description"),
                now,
                now,
            ),
        )
        self._audit(user_id, "GroupCreated", "Group", str(data["group_name"]), str(data["display_label"]), None, json.dumps(data))
        self._conn.commit()

    def update_group(self, group_id: int, user_id: int, data: dict[str, object]) -> None:
        existing = self._conn.execute("SELECT * FROM groups WHERE group_id = ?", (group_id,)).fetchone()
        if existing is None:
            raise ValueError("Group not found")
        now = self._utc_now()
        self._conn.execute(
            """
            UPDATE groups
            SET group_name = ?, display_label = ?, display_order = ?, is_enabled = ?, description = ?, updated_utc = ?
            WHERE group_id = ?
            """,
            (
                data["group_name"],
                data["display_label"],
                int(data["display_order"]),
                1 if data.get("is_enabled", True) else 0,
                data.get("description"),
                now,
                group_id,
            ),
        )
        self._audit(user_id, "GroupUpdated", "Group", str(group_id), str(data["display_label"]), json.dumps(dict(existing)), json.dumps(data))
        self._conn.commit()

    def create_check(self, user_id: int, data: dict[str, object]) -> None:
        self._validate_check(data)
        now = self._utc_now()
        cur = self._conn.execute(
            """
            INSERT INTO checks(
                internal_name, display_label, group_id, description, is_enabled, display_order,
                applies_to_site1, applies_to_site2, created_utc, updated_utc, created_by_user_id, updated_by_user_id,
                troubleshooting_tier1_url, troubleshooting_tier2_url, graph_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["internal_name"],
                data["display_label"],
                int(data["group_id"]),
                data.get("description"),
                1 if data.get("is_enabled", True) else 0,
                int(data.get("display_order", 0)),
                1 if data.get("applies_to_site1", True) else 0,
                1 if data.get("applies_to_site2", True) else 0,
                now,
                now,
                user_id,
                user_id,
                data.get("troubleshooting_tier1_url"),
                data.get("troubleshooting_tier2_url"),
                data.get("graph_type", "Line"),
            ),
        )
        check_id = int(cur.lastrowid)
        self._conn.execute(
            """
            INSERT INTO check_source_config(
                check_id, source_type, relative_path_site1, relative_path_site2, file_pattern,
                parser_type, match_strategy, case_sensitive, target_pattern, secondary_pattern, notes
            ) VALUES (?, 'File', ?, ?, NULL, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                check_id,
                data.get("relative_path_site1"),
                data.get("relative_path_site2"),
                data["parser_type"],
                data.get("match_strategy", "FirstMatch"),
                1 if data.get("case_sensitive", False) else 0,
                data.get("target_pattern"),
            ),
        )
        self._conn.execute(
            """
            INSERT INTO check_rule_config(
                check_id, rule_type, operator, threshold_min, threshold_max,
                expected_interval_seconds, stale_timeout_seconds, grace_period_seconds, pass_text, fail_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                check_id,
                data["rule_type"],
                data.get("operator"),
                self._maybe_float(data.get("threshold_min")),
                self._maybe_float(data.get("threshold_max")),
                int(data.get("expected_interval_seconds") or 60),
                int(data.get("stale_timeout_seconds") or 300),
                data.get("pass_text"),
                data.get("fail_text"),
            ),
        )
        self._audit(user_id, "CheckCreated", "Check", str(check_id), str(data["display_label"]), None, json.dumps(data))
        self._conn.commit()

    def update_check(self, check_id: int, user_id: int, data: dict[str, object]) -> None:
        existing = self.get_check(int(check_id))
        if existing is None:
            raise ValueError(f"Check not found for id {check_id}")
        data = dict(data)
        data["internal_name"] = str(existing["internal_name"])
        self._validate_check_update(data)
        now = self._utc_now()
        self._conn.execute(
            """
            UPDATE checks
            SET display_label = ?, group_id = ?, description = ?, is_enabled = ?,
                display_order = ?, applies_to_site1 = ?, applies_to_site2 = ?, updated_utc = ?, updated_by_user_id = ?,
                troubleshooting_tier1_url = ?, troubleshooting_tier2_url = ?, graph_type = ?
            WHERE check_id = ?
            """,
            (
                data["display_label"], int(data["group_id"]), data.get("description"),
                1 if data.get("is_enabled", True) else 0, int(data.get("display_order", 0)),
                1 if data.get("applies_to_site1", True) else 0, 1 if data.get("applies_to_site2", True) else 0,
                now, user_id,
                data.get("troubleshooting_tier1_url"), data.get("troubleshooting_tier2_url"), data.get("graph_type", "Line"),
                check_id,
            ),
        )
        self._conn.execute(
            """
            UPDATE check_source_config
            SET relative_path_site1 = ?, relative_path_site2 = ?, parser_type = ?, match_strategy = ?,
                case_sensitive = ?, target_pattern = ?
            WHERE check_id = ?
            """,
            (
                data.get("relative_path_site1"), data.get("relative_path_site2"), data["parser_type"],
                data.get("match_strategy", "FirstMatch"), 1 if data.get("case_sensitive", False) else 0,
                data.get("target_pattern"), check_id,
            ),
        )
        self._conn.execute(
            """
            UPDATE check_rule_config
            SET rule_type = ?, operator = ?, threshold_min = ?, threshold_max = ?,
                expected_interval_seconds = ?, stale_timeout_seconds = ?, pass_text = ?, fail_text = ?
            WHERE check_id = ?
            """,
            (
                data["rule_type"], data.get("operator"), self._maybe_float(data.get("threshold_min")),
                self._maybe_float(data.get("threshold_max")), int(data.get("expected_interval_seconds") or 60),
                int(data.get("stale_timeout_seconds") or 300), data.get("pass_text"), data.get("fail_text"), check_id,
            ),
        )
        self._audit(user_id, "CheckUpdated", "Check", str(check_id), str(data["display_label"]), json.dumps(dict(existing)), json.dumps(data))
        self._conn.commit()

    def duplicate_check(self, check_id: int, user_id: int) -> int:
        row = self.get_check(check_id)
        if row is None:
            raise ValueError("Check not found")
        base_internal = f"{row['internal_name']}_copy"
        internal_name = base_internal
        counter = 2
        while self._conn.execute("SELECT 1 FROM checks WHERE internal_name = ?", (internal_name,)).fetchone():
            internal_name = f"{base_internal}{counter}"
            counter += 1
        data = {
            "internal_name": internal_name,
            "display_label": f"{row['display_label']} Copy",
            "group_id": int(row['group_id']),
            "description": row['description'],
            "is_enabled": False,
            "display_order": int(row['display_order']),
            "applies_to_site1": bool(row['applies_to_site1']),
            "applies_to_site2": bool(row['applies_to_site2']),
            "relative_path_site1": row['relative_path_site1'],
            "relative_path_site2": row['relative_path_site2'],
            "parser_type": row['parser_type'],
            "target_pattern": row['target_pattern'],
            "match_strategy": row['match_strategy'],
            "case_sensitive": bool(row['case_sensitive']),
            "rule_type": row['rule_type'],
            "operator": row['operator'],
            "threshold_min": row['threshold_min'],
            "threshold_max": row['threshold_max'],
            "expected_interval_seconds": int(row['expected_interval_seconds'] or 60),
            "stale_timeout_seconds": int(row['stale_timeout_seconds'] or 300),
            "pass_text": row['pass_text'],
            "fail_text": row['fail_text'],
        }
        self.create_check(user_id, data)
        new_id = self._conn.execute("SELECT check_id FROM checks WHERE internal_name = ?", (internal_name,)).fetchone()["check_id"]
        self._audit(user_id, "CheckDuplicated", "Check", str(new_id), str(data['display_label']), json.dumps(dict(row)), json.dumps(data))
        self._conn.commit()
        return int(new_id)

    def toggle_check_enabled(self, check_id: int, user_id: int) -> None:
        row = self.get_check(check_id)
        if row is None:
            raise ValueError("Check not found")
        new_state = 0 if int(row["is_enabled"]) == 1 else 1
        now = self._utc_now()
        self._conn.execute(
            "UPDATE checks SET is_enabled = ?, updated_utc = ?, updated_by_user_id = ? WHERE check_id = ?",
            (new_state, now, user_id, check_id),
        )
        self._audit(user_id, "CheckToggled", "Check", str(check_id), str(row["display_label"]), json.dumps({"is_enabled": int(row["is_enabled"])}), json.dumps({"is_enabled": new_state}))
        self._conn.commit()

    def delete_check(self, check_id: int, user_id: int) -> None:
        row = self.get_check(check_id)
        if row is None:
            raise ValueError("Check not found")
        now = self._utc_now()
        payload = json.dumps(dict(row))
        self._conn.execute(
            """
            UPDATE checks
            SET is_deleted = 1, is_enabled = 0, deleted_utc = ?, deleted_by_user_id = ?, deleted_payload = ?, updated_utc = ?, updated_by_user_id = ?
            WHERE check_id = ?
            """,
            (now, user_id, payload, now, user_id, check_id),
        )
        self._audit(user_id, "CheckDeleted", "Check", str(check_id), str(row["display_label"]), payload, None)
        self._conn.commit()

    def list_deleted_checks(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT c.check_id, c.display_label, c.internal_name, c.deleted_utc, g.display_label AS group_label
                FROM checks c
                LEFT JOIN groups g ON g.group_id = c.group_id
                WHERE COALESCE(c.is_deleted, 0) = 1 AND COALESCE(c.is_retired, 0) = 0
                ORDER BY c.deleted_utc DESC, c.display_label
                """
            )
        )

    def restore_check(self, check_id: int, user_id: int) -> None:
        row = self._conn.execute("SELECT check_id, display_label, deleted_payload FROM checks WHERE check_id = ?", (check_id,)).fetchone()
        if row is None:
            raise ValueError("Deleted check not found")
        is_enabled = 1
        group_id = None
        if row["deleted_payload"]:
            try:
                payload = json.loads(str(row["deleted_payload"]))
                is_enabled = 1 if payload.get("is_enabled", 1) else 0
                group_id = payload.get("group_id")
            except Exception:
                pass
        if group_id is not None:
            group_row = self._conn.execute("SELECT group_id FROM groups WHERE group_id = ? AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0", (int(group_id),)).fetchone()
            if group_row is None:
                fallback = self._conn.execute("SELECT group_id FROM groups WHERE COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0 ORDER BY display_order, group_id LIMIT 1").fetchone()
                if fallback is None:
                    raise ValueError("No active group is available for restore")
                group_id = int(fallback["group_id"])
        now = self._utc_now()
        params = [is_enabled, now, user_id]
        sql = "UPDATE checks SET is_deleted = 0, deleted_utc = NULL, deleted_by_user_id = NULL, deleted_payload = NULL, is_enabled = ?, updated_utc = ?, updated_by_user_id = ?"
        if group_id is not None:
            sql += ", group_id = ?"
            params.append(int(group_id))
        sql += " WHERE check_id = ?"
        params.append(check_id)
        self._conn.execute(sql, tuple(params))
        self._audit(user_id, "CheckRestored", "Check", str(check_id), str(row["display_label"]), None, json.dumps({"restored": True}))
        self._conn.commit()

    def retire_check(self, check_id: int, user_id: int) -> None:
        row = self._conn.execute("SELECT check_id, display_label FROM checks WHERE check_id = ?", (check_id,)).fetchone()
        if row is None:
            raise ValueError("Deleted check not found")
        now = self._utc_now()
        self._conn.execute(
            "UPDATE checks SET is_retired = 1, retired_utc = ?, retired_by_user_id = ? WHERE check_id = ?",
            (now, user_id, check_id),
        )
        self._audit(user_id, "CheckRetired", "Check", str(check_id), str(row["display_label"]), None, json.dumps({"retired": True}))
        self._conn.commit()

    def list_deleted_groups(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT group_id, display_label, group_name, deleted_utc
                FROM groups
                WHERE COALESCE(is_deleted, 0) = 1 AND COALESCE(is_retired, 0) = 0
                ORDER BY deleted_utc DESC, display_label
                """
            )
        )

    def delete_group(self, group_id: int, user_id: int, strategy: str, destination_group_id: int | None = None) -> None:
        row = self._conn.execute("SELECT * FROM groups WHERE group_id = ? AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0", (group_id,)).fetchone()
        if row is None:
            raise ValueError("Group not found")
        now = self._utc_now()
        active_checks = [r for r in self._conn.execute("SELECT check_id FROM checks WHERE group_id = ? AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0", (group_id,)).fetchall()]
        if strategy == "move":
            if destination_group_id is None or int(destination_group_id) == int(group_id):
                raise ValueError("Choose a different destination group")
            self._conn.execute("UPDATE checks SET group_id = ?, updated_utc = ?, updated_by_user_id = ? WHERE group_id = ? AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0", (int(destination_group_id), now, user_id, group_id))
        elif strategy == "delete_all":
            for check in active_checks:
                self.delete_check(int(check["check_id"]), user_id)
        else:
            raise ValueError("Unsupported group delete strategy")
        self._conn.execute("UPDATE groups SET is_deleted = 1, is_enabled = 0, deleted_utc = ?, deleted_by_user_id = ?, deleted_payload = ?, updated_utc = ? WHERE group_id = ?", (now, user_id, json.dumps(dict(row)), now, group_id))
        self._audit(user_id, "GroupDeleted", "Group", str(group_id), str(row["display_label"]), json.dumps(dict(row)), json.dumps({"strategy": strategy, "destination_group_id": destination_group_id}))
        self._conn.commit()

    def restore_group(self, group_id: int, user_id: int) -> None:
        row = self._conn.execute("SELECT group_id, group_name, display_label FROM groups WHERE group_id = ?", (group_id,)).fetchone()
        if row is None:
            raise ValueError("Deleted group not found")
        name = str(row["group_name"])
        if self._conn.execute("SELECT 1 FROM groups WHERE group_name = ? AND group_id != ? AND COALESCE(is_deleted,0)=0 AND COALESCE(is_retired,0)=0", (name, group_id)).fetchone():
            suffix = 2
            new_name = f"{name}_restored"
            while self._conn.execute("SELECT 1 FROM groups WHERE group_name = ? AND group_id != ?", (new_name, group_id)).fetchone():
                suffix += 1
                new_name = f"{name}_restored{suffix}"
            self._conn.execute("UPDATE groups SET group_name = ?, display_label = ? WHERE group_id = ?", (new_name, f"{row['display_label']} Restored", group_id))
        now = self._utc_now()
        self._conn.execute("UPDATE groups SET is_deleted = 0, deleted_utc = NULL, deleted_by_user_id = NULL, deleted_payload = NULL, is_enabled = 1, updated_utc = ? WHERE group_id = ?", (now, group_id))
        self._audit(user_id, "GroupRestored", "Group", str(group_id), str(row["display_label"]), None, json.dumps({"restored": True}))
        self._conn.commit()

    def retire_group(self, group_id: int, user_id: int) -> None:
        row = self._conn.execute("SELECT group_id, display_label FROM groups WHERE group_id = ?", (group_id,)).fetchone()
        if row is None:
            raise ValueError("Deleted group not found")
        now = self._utc_now()
        self._conn.execute("UPDATE groups SET is_retired = 1, retired_utc = ?, retired_by_user_id = ? WHERE group_id = ?", (now, user_id, group_id))
        self._audit(user_id, "GroupRetired", "Group", str(group_id), str(row["display_label"]), None, json.dumps({"retired": True}))
        self._conn.commit()

    def _validate_check_update(self, data: dict[str, object]) -> None:
        if not str(data.get("display_label", "")).strip():
            raise ValueError("Display label is required")
        if not data.get("group_id"):
            raise ValueError("Group is required")
        parser_type = str(data.get("parser_type", "")).strip()
        rule_type = str(data.get("rule_type", "")).strip()
        if parser_type not in {"RawText", "AnsiFormattedText", "IntegerFromPattern", "DecimalFromPattern", "FreshnessOnly", "FileMustBeBlank"}:
            raise ValueError("Unsupported parser type")
        if rule_type not in {"TextExists", "TextNotExists", "NumericCompare", "NumericRange", "FreshnessOnly", "FileMustBeBlank"}:
            raise ValueError("Unsupported rule type")
        if not str(data.get("relative_path_site1", "")).strip() and not str(data.get("relative_path_site2", "")).strip():
            raise ValueError("At least one site file path is required")
        expected = int(data.get("expected_interval_seconds") or 60)
        stale = int(data.get("stale_timeout_seconds") or 300)
        if stale <= expected:
            raise ValueError("Stale timeout must be greater than expected interval")
        if rule_type in {"TextExists", "TextNotExists"} and not str(data.get("target_pattern", "")).strip():
            raise ValueError("Target pattern is required for text rules")
        if parser_type in {"IntegerFromPattern", "DecimalFromPattern"} and not str(data.get("target_pattern", "")).strip():
            raise ValueError("Target pattern is required for numeric parsers")
        if rule_type == "NumericCompare":
            if data.get("operator") not in {">", ">=", "<", "<=", "=="}:
                raise ValueError("Numeric compare requires a valid operator")
            if data.get("threshold_max") in (None, ""):
                raise ValueError("Numeric compare requires a threshold value")
        if rule_type == "NumericRange":
            if data.get("threshold_min") in (None, "") or data.get("threshold_max") in (None, ""):
                raise ValueError("Numeric range requires both min and max")

    def _validate_check(self, data: dict[str, object], check_id: int | None = None) -> None:
        if not str(data.get("internal_name", "")).strip():
            raise ValueError("Internal name is required")
        if not str(data.get("display_label", "")).strip():
            raise ValueError("Display label is required")
        if not data.get("group_id"):
            raise ValueError("Group is required")
        parser_type = str(data.get("parser_type", "")).strip()
        rule_type = str(data.get("rule_type", "")).strip()
        if parser_type not in {"RawText", "AnsiFormattedText", "IntegerFromPattern", "DecimalFromPattern", "FreshnessOnly", "FileMustBeBlank"}:
            raise ValueError("Unsupported parser type")
        if rule_type not in {"TextExists", "TextNotExists", "NumericCompare", "NumericRange", "FreshnessOnly", "FileMustBeBlank"}:
            raise ValueError("Unsupported rule type")
        if not str(data.get("relative_path_site1", "")).strip() and not str(data.get("relative_path_site2", "")).strip():
            raise ValueError("At least one site file path is required")
        expected = int(data.get("expected_interval_seconds") or 60)
        stale = int(data.get("stale_timeout_seconds") or 300)
        if stale <= expected:
            raise ValueError("Stale timeout must be greater than expected interval")
        if rule_type in {"TextExists", "TextNotExists"} and not str(data.get("target_pattern", "")).strip():
            raise ValueError("Target pattern is required for text rules")
        if parser_type in {"IntegerFromPattern", "DecimalFromPattern"} and not str(data.get("target_pattern", "")).strip():
            raise ValueError("Target pattern is required for numeric parsers")
        if rule_type == "NumericCompare":
            if data.get("operator") not in {">", ">=", "<", "<="}:
                raise ValueError("Numeric compare requires a valid operator")
            if data.get("threshold_max") in (None, ""):
                raise ValueError("Numeric compare requires a threshold value")
        if rule_type == "NumericRange":
            if data.get("threshold_min") in (None, "") or data.get("threshold_max") in (None, ""):
                raise ValueError("Numeric range requires both min and max")
        if check_id is None:
            existing = self._conn.execute(
                "SELECT check_id FROM checks WHERE internal_name = ?",
                (str(data["internal_name"]),),
            ).fetchone()
            if existing:
                raise ValueError("Internal name already exists")
        else:
            # Internal name is locked during edit, so updates should not be blocked by duplicate-name validation.
            pass

    def _audit(self, user_id: int, action: str, entity_type: str, entity_id: str | None, entity_name: str | None, old_value: str | None, new_value: str | None) -> None:
        self._conn.execute(
            """
            INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_id, entity_name, old_value_json, new_value_json, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (self._utc_now(), user_id, action, entity_type, entity_id, entity_name, old_value, new_value, f"{action} {entity_type} {entity_name or entity_id or ''}".strip()),
        )

    @staticmethod
    def _maybe_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()
