from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import logging

from src.data.monitoring_repository import MonitoringCheckConfig, MonitoringRepository
from src.engine.parser_engine import ParseResult, ParserEngine

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EvaluationResult:
    operational_state: str
    condition_state: str
    freshness_state: str
    rule_outcome: str
    detail_message: str
    value_text: str | None
    value_numeric: float | None


class MonitoringEngine:
    def __init__(self, repository: MonitoringRepository, app_base_path: Path) -> None:
        self._repo = repository
        self._parser = ParserEngine()
        self._app_base_path = app_base_path

    def _resolve_path_setting(self, setting_value: str) -> Path:
        configured = Path(setting_value)
        if not configured.is_absolute():
            configured = (self._app_base_path / configured).resolve()
        return configured

    def _resolve_site_data_root(self, mode_name: str, site_id: int) -> Path:
        if mode_name.lower() == "live":
            configured = self._repo.get_live_data_root(site_id)
            if configured:
                return self._resolve_path_setting(configured)
        return self._resolve_path_setting(self._repo.get_test_data_root()) / ("Site1" if site_id == 1 else "Site2")

    def _resolve_runtime_log_path(self) -> Path:
        configured = Path(self._repo._get_setting("notification_log_path", "Logs/notifications.log"))
        if not configured.is_absolute():
            configured = (self._app_base_path / configured).resolve()
        configured.parent.mkdir(parents=True, exist_ok=True)
        return configured

    def run_cycle(self) -> None:
        mode_name = self._repo.get_active_mode()
        site_id = self._repo.get_active_site_id()
        checks = self._repo.get_enabled_checks_for_site(site_id)
        site_folder = self._resolve_site_data_root(mode_name, site_id)
        site_folder.mkdir(parents=True, exist_ok=True)

        for check in checks:
            try:
                self._evaluate_check(check, site_id, mode_name, site_folder)
            except Exception:  # noqa: BLE001
                logger.exception("Evaluation failed for check %s", check.internal_name)
        self._repo.commit()

    def _evaluate_check(self, check: MonitoringCheckConfig, site_id: int, mode_name: str, site_folder: Path) -> None:
        file_name = check.relative_path_site1 if site_id == 1 else check.relative_path_site2
        file_path = site_folder / (file_name or "")
        parsed = self._parser.parse_file(file_path, check)
        prior = self._repo.get_current_status_row(check.check_id)
        evaluation = self._apply_rule(check, parsed, prior)
        now_utc = datetime.now(timezone.utc).isoformat()

        open_alert = self._repo.get_open_alert(check.check_id)
        active_alert_id = int(open_alert["alert_instance_id"]) if open_alert else None
        is_ack = int(open_alert["is_acknowledged"] or 0) if open_alert else 0
        maintenance = self._repo.get_active_maintenance_for_check(check.check_id, check.group_id)
        is_issue_state = evaluation.condition_state == "Unhealthy" or evaluation.freshness_state == "Stale"
        is_suppressed = maintenance is not None and is_issue_state
        desired_alert_type = self._derive_alert_type(evaluation)
        prior_condition = self._prior_condition_state(prior)
        prior_freshness = self._prior_freshness_state(prior)

        # Freshness transition events are independent from alert transitions.
        if prior is not None and prior_freshness != evaluation.freshness_state:
            if evaluation.freshness_state == "Stale":
                self._repo.insert_event(
                    {
                        "event_utc": now_utc,
                        "event_type": "DataBecameStale",
                        "check_id": check.check_id,
                        "alert_instance_id": active_alert_id,
                        "site_id": site_id,
                        "mode_name": mode_name,
                        "user_id": None,
                        "message": f"{check.display_label} data became stale",
                        "detail": evaluation.detail_message,
                    }
                )
                self._dispatch_notifications("DataBecameStale", check, active_alert_id, evaluation.detail_message)
            else:
                self._repo.insert_event(
                    {
                        "event_utc": now_utc,
                        "event_type": "DataBecameFresh",
                        "check_id": check.check_id,
                        "alert_instance_id": active_alert_id,
                        "site_id": site_id,
                        "mode_name": mode_name,
                        "user_id": None,
                        "message": f"{check.display_label} data became fresh",
                        "detail": evaluation.detail_message,
                    }
                )
                self._dispatch_notifications("DataBecameFresh", check, active_alert_id, evaluation.detail_message)

        # Alert lifecycle / state transitions.
        if desired_alert_type is not None and open_alert is None and not is_suppressed:
            active_alert_id = self._repo.create_alert_instance(
                {
                    "check_id": check.check_id,
                    "site_id": site_id,
                    "mode_name": mode_name,
                    "alert_type": desired_alert_type,
                    "start_utc": now_utc,
                    "start_message": evaluation.detail_message,
                }
            )
            is_ack = 0
            self._repo.insert_event(
                {
                    "event_utc": now_utc,
                    "event_type": "AlertStarted",
                    "check_id": check.check_id,
                    "alert_instance_id": active_alert_id,
                    "site_id": site_id,
                    "mode_name": mode_name,
                    "user_id": None,
                    "message": f"{check.display_label} entered {self._display_state_text(evaluation).lower()} state",
                    "detail": evaluation.detail_message,
                }
            )
            self._dispatch_notifications("AlertStarted", check, active_alert_id, evaluation.detail_message)
        elif desired_alert_type is None and open_alert is not None:
            self._repo.close_alert_instance(int(open_alert["alert_instance_id"]), now_utc, evaluation.detail_message)
            self._repo.insert_event(
                {
                    "event_utc": now_utc,
                    "event_type": "AlertCleared",
                    "check_id": check.check_id,
                    "alert_instance_id": int(open_alert["alert_instance_id"]),
                    "site_id": site_id,
                    "mode_name": mode_name,
                    "user_id": None,
                    "message": f"{check.display_label} returned to healthy state",
                    "detail": evaluation.detail_message,
                }
            )
            self._dispatch_notifications("AlertCleared", check, int(open_alert["alert_instance_id"]), evaluation.detail_message)
            active_alert_id = None
            is_ack = 0
        elif desired_alert_type is not None and open_alert is not None and str(open_alert["alert_type"] or "") != desired_alert_type and not is_suppressed:
            old_type = str(open_alert["alert_type"] or "")
            self._repo.update_alert_instance_type(int(open_alert["alert_instance_id"]), desired_alert_type, evaluation.detail_message)
            self._repo.insert_event(
                {
                    "event_utc": now_utc,
                    "event_type": "AlertStateChanged",
                    "check_id": check.check_id,
                    "alert_instance_id": int(open_alert["alert_instance_id"]),
                    "site_id": site_id,
                    "mode_name": mode_name,
                    "user_id": None,
                    "message": f"{check.display_label} alert state changed from {old_type} to {desired_alert_type}",
                    "detail": evaluation.detail_message,
                }
            )
            self._dispatch_notifications("AlertStateChanged", check, int(open_alert["alert_instance_id"]), evaluation.detail_message)
            is_ack = 0

        # Condition transition event for healthy/unhealthy changes.
        if prior is not None and prior_condition != evaluation.condition_state:
            if not (prior_condition == "Healthy" and evaluation.condition_state == "Healthy"):
                self._repo.insert_event(
                    {
                        "event_utc": now_utc,
                        "event_type": "AlertStateChanged",
                        "check_id": check.check_id,
                        "alert_instance_id": active_alert_id,
                        "site_id": site_id,
                        "mode_name": mode_name,
                        "user_id": None,
                        "message": f"{check.display_label} condition changed from {prior_condition} to {evaluation.condition_state}",
                        "detail": evaluation.detail_message,
                    }
                )

        alert_state = "None"
        if is_issue_state:
            if is_suppressed:
                alert_state = "SuppressedMaintenance"
                active_alert_id = None
                is_ack = 0
                self._log_suppressed_state_if_changed(prior, check, site_id, mode_name, maintenance, evaluation, now_utc)
            else:
                alert_state = "ActiveAcknowledged" if is_ack else "ActiveUnacknowledged"

        detail_message = evaluation.detail_message
        if is_suppressed and maintenance is not None:
            detail_message = f"{evaluation.detail_message} (suppressed by maintenance: {maintenance['window_name']})"

        self._repo.insert_check_result(
            {
                "check_id": check.check_id,
                "site_id": site_id,
                "mode_name": mode_name,
                "evaluated_utc": now_utc,
                "source_file_path": parsed.file_path,
                "source_modified_utc": parsed.source_modified_utc,
                "source_age_seconds": parsed.source_age_seconds,
                "parse_status": parsed.parse_status,
                "value_type": "number" if evaluation.value_numeric is not None else parsed.value_type,
                "value_text": evaluation.value_text,
                "value_numeric": evaluation.value_numeric,
                "rule_type": check.rule_type,
                "rule_outcome": evaluation.rule_outcome,
                "operational_state": evaluation.operational_state,
                "condition_state": evaluation.condition_state,
                "freshness_state": evaluation.freshness_state,
                "detail_message": detail_message,
                "technical_detail": parsed.technical_detail,
            }
        )

        current_owner_user_id = int(prior["owner_user_id"]) if prior is not None and prior["owner_user_id"] is not None else None
        current_severity = str(prior["severity"] or "Medium") if prior is not None and prior["severity"] is not None else "Medium"
        self._repo.upsert_current_status(
            {
                "check_id": check.check_id,
                "site_id": site_id,
                "mode_name": mode_name,
                "operational_state": evaluation.operational_state,
                "condition_state": evaluation.condition_state,
                "freshness_state": evaluation.freshness_state,
                "alert_state": alert_state,
                "is_acknowledged": is_ack,
                "owner_user_id": current_owner_user_id,
                "severity": current_severity,
                "active_alert_instance_id": active_alert_id,
                "last_result_utc": now_utc,
                "last_source_modified_utc": parsed.source_modified_utc,
                "last_source_age_seconds": parsed.source_age_seconds,
                "last_parse_status": parsed.parse_status,
                "last_value_text": evaluation.value_text,
                "last_value_numeric": evaluation.value_numeric,
                "last_detail_message": detail_message,
                "updated_utc": now_utc,
            }
        )

    def _log_suppressed_state_if_changed(self, prior, check, site_id: int, mode_name: str, maintenance, evaluation: EvaluationResult, now_utc: str) -> None:
        prior_alert_state = str(prior["alert_state"] or "") if prior is not None else ""
        if prior_alert_state == "SuppressedMaintenance":
            return
        self._repo.insert_event(
            {
                "event_utc": now_utc,
                "event_type": "AlertSuppressed",
                "check_id": check.check_id,
                "alert_instance_id": None,
                "site_id": site_id,
                "mode_name": mode_name,
                "user_id": None,
                "message": f"{check.display_label} matched maintenance window {maintenance['window_name']}",
                "detail": evaluation.detail_message,
            }
        )

    def _dispatch_notifications(self, event_type: str, check: MonitoringCheckConfig, alert_instance_id: int | None, detail_message: str) -> None:
        rules = self._repo.get_matching_notification_rules(event_type, check.check_id, check.group_id)
        if not rules:
            return
        delivered_utc = datetime.now(timezone.utc).isoformat()
        log_path = self._resolve_runtime_log_path()
        for rule in rules:
            renotify = int(rule["renotify_minutes"] or 0)
            rule_id = int(rule["notification_rule_id"])
            if self._repo.notification_recently_sent(rule_id, event_type, check.check_id, renotify):
                continue
            destination = str(rule["destination"] or log_path)
            message = f"[{delivered_utc}] {event_type} | {check.display_label} | {detail_message}"
            status = "Logged"
            try:
                if str(rule["channel_type"] or "FileLog") == "FileLog":
                    target = Path(destination)
                    if not target.is_absolute():
                        target = (self._app_base_path / target).resolve()
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("a", encoding="utf-8") as handle:
                        handle.write(message + "\n")
                else:
                    with log_path.open("a", encoding="utf-8") as handle:
                        handle.write(message + f" | Channel={rule['channel_type']} | Destination={destination}\n")
            except Exception as exc:  # noqa: BLE001
                status = f"Failed: {exc}"
                logger.exception("Notification delivery failed for rule %s", rule["rule_name"])
            self._repo.log_notification_delivery(
                {
                    "notification_rule_id": rule_id,
                    "event_type": event_type,
                    "check_id": check.check_id,
                    "alert_instance_id": alert_instance_id,
                    "delivered_utc": delivered_utc,
                    "delivery_status": status,
                    "destination": destination,
                    "message": message,
                }
            )

    def _apply_rule(self, check: MonitoringCheckConfig, parsed: ParseResult, prior) -> EvaluationResult:
        freshness_state = "Fresh"
        if parsed.parse_status == "NoInput":
            freshness_state = "Stale"
        elif parsed.source_age_seconds is not None and parsed.source_age_seconds > check.stale_timeout_seconds:
            freshness_state = "Stale"
        elif parsed.parse_status != "Success":
            freshness_state = "Stale"

        if freshness_state == "Stale":
            prior_condition = self._prior_condition_state(prior)
            if prior_condition == "Unhealthy":
                return EvaluationResult(
                    "Unhealthy",
                    "Unhealthy",
                    "Stale",
                    "Stale",
                    f"{self._stale_message(parsed, check)} | Last known result remains unhealthy",
                    parsed.value_text,
                    parsed.value_numeric,
                )
            return EvaluationResult(
                "Stale",
                "Healthy",
                "Stale",
                "Stale",
                self._stale_message(parsed, check),
                parsed.value_text,
                parsed.value_numeric,
            )

        if check.rule_type == "FreshnessOnly":
            return EvaluationResult("Healthy", "Healthy", "Fresh", "Passed", check.pass_text or "Source fresh", None, None)

        if check.rule_type == "FileMustBeBlank":
            content = (parsed.raw_content or "")
            has_content = bool(content.strip())
            condition = "Unhealthy" if has_content else "Healthy"
            return EvaluationResult(
                condition,
                condition,
                "Fresh",
                "Failed" if has_content else "Passed",
                (check.fail_text or "File contains data") if has_content else (check.pass_text or "File is blank"),
                content.strip()[:200] or None,
                None,
            )

        if check.rule_type == "TextExists":
            haystack = (parsed.value_text or parsed.raw_content or "")
            needle = check.target_pattern or ""
            found = self._contains(haystack, needle, check.case_sensitive)
            condition = "Healthy" if found else "Unhealthy"
            return EvaluationResult(
                condition,
                condition,
                "Fresh",
                "Passed" if found else "Failed",
                check.pass_text if found else (check.fail_text or f'Pattern "{needle}" missing'),
                haystack.strip()[:200] or None,
                None,
            )

        if check.rule_type == "TextNotExists":
            haystack = (parsed.value_text or parsed.raw_content or "")
            needle = check.target_pattern or ""
            found = self._contains(haystack, needle, check.case_sensitive)
            condition = "Unhealthy" if found else "Healthy"
            return EvaluationResult(
                condition,
                condition,
                "Fresh",
                "Failed" if found else "Passed",
                (check.fail_text or f'Pattern "{needle}" found') if found else (check.pass_text or f'Pattern "{needle}" not found'),
                haystack.strip()[:200] or None,
                None,
            )

        if check.rule_type == "NumericCompare":
            value = parsed.value_numeric
            threshold = check.threshold_max if check.threshold_max is not None else check.threshold_min
            if value is None or threshold is None:
                return EvaluationResult("Stale", "Healthy", "Stale", "ParseFailed", "Numeric value missing", None, None)
            op = check.operator or ">"
            failed = self._compare(float(value), op, float(threshold))
            condition = "Unhealthy" if failed else "Healthy"
            return EvaluationResult(
                condition,
                condition,
                "Fresh",
                "Failed" if failed else "Passed",
                check.fail_text or f"Value high {value} {op} {threshold}" if failed else check.pass_text or f"Value OK {value}",
                None,
                float(value),
            )

        if check.rule_type == "NumericRange":
            value = parsed.value_numeric
            if value is None or check.threshold_min is None or check.threshold_max is None:
                return EvaluationResult("Stale", "Healthy", "Stale", "ParseFailed", "Numeric range values missing", None, None)
            passed = check.threshold_min <= float(value) <= check.threshold_max
            condition = "Healthy" if passed else "Unhealthy"
            return EvaluationResult(
                condition,
                condition,
                "Fresh",
                "Passed" if passed else "Failed",
                check.pass_text or f"Value in range {value}" if passed else check.fail_text or f"Value out of range {value}",
                None,
                float(value),
            )

        return EvaluationResult("Stale", "Healthy", "Stale", "Unsupported", f"Unsupported rule type: {check.rule_type}", parsed.value_text, parsed.value_numeric)

    @staticmethod
    def _contains(haystack: str, needle: str, case_sensitive: bool) -> bool:
        if case_sensitive:
            return needle in haystack
        return needle.lower() in haystack.lower()

    @staticmethod
    def _compare(value: float, op: str, threshold: float) -> bool:
        if op == ">":
            return value > threshold
        if op == ">=":
            return value >= threshold
        if op == "<":
            return value < threshold
        if op == "<=":
            return value <= threshold
        if op == "==":
            return value == threshold
        return value > threshold

    @staticmethod
    def _stale_message(parsed: ParseResult, check: MonitoringCheckConfig) -> str:
        if parsed.source_age_seconds is None:
            return parsed.detail_message
        return f"Stale {parsed.source_age_seconds}s > {check.stale_timeout_seconds}s"

    @staticmethod
    def _prior_condition_state(prior) -> str:
        if prior is None:
            return "Healthy"
        return str(prior["condition_state"] or ("Unhealthy" if str(prior["operational_state"] or "") == "Unhealthy" else "Healthy"))

    @staticmethod
    def _prior_freshness_state(prior) -> str:
        if prior is None:
            return "Fresh"
        return str(prior["freshness_state"] or ("Stale" if str(prior["operational_state"] or "") == "Stale" else "Fresh"))

    @staticmethod
    def _derive_alert_type(evaluation: EvaluationResult) -> str | None:
        if evaluation.condition_state == "Unhealthy":
            return "Unhealthy"
        if evaluation.freshness_state == "Stale":
            return "Stale"
        return None

    @staticmethod
    def _display_state_text(evaluation: EvaluationResult) -> str:
        if evaluation.condition_state == "Unhealthy" and evaluation.freshness_state == "Stale":
            return "unhealthy (stale data)"
        if evaluation.condition_state == "Unhealthy":
            return "unhealthy"
        if evaluation.freshness_state == "Stale":
            return "stale"
        return "healthy"
