from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from PySide6.QtCore import Qt, QTimer, QObject, QEvent, QUrl
import logging
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QFileDialog,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDialogButtonBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QDialog,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from src.data.config_repository import ConfigRepository
from src.data.dashboard_repository import DashboardRepository
from src.data.monitoring_repository import MonitoringRepository
from src.data.settings_repository import SettingsRepository
from src.data.user_repository import UserRepository
from src.engine.monitoring_engine import MonitoringEngine
from src.engine.transfer_engine import TransferEngine, DEFAULT_MAPPINGS
from src.shared.constants import APP_NAME, APP_VERSION
from src.shared.models import AuthenticatedUser
from src.ui.config_dialogs import CheckDialog, GroupDialog
from src.ui.history_chart import HistoryChartWidget
from src.theming.theme_service import ThemeService
from src.application.config_loader import load_bootstrap_config, resolve_app_path, save_bootstrap_data_path_override
from src.data.database import initialize_database
from src.data.seed import seed_database
import json, shutil

logger = logging.getLogger(__name__)




class _CheckDetailsPopoutDialog(QDialog):
    def __init__(self, parent: "MainWindow") -> None:
        super().__init__(parent)
        self._window = parent
        self.setWindowTitle("Selected Check Details")
        self.resize(780, 640)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._title = QLabel("Selected Check")
        self._title.setObjectName("sectionTitle")
        self._status = QLabel("")
        self._status.setObjectName("selectedStatusPill")
        self._status.setWordWrap(True)
        self._meta = QLabel("")
        self._meta.setWordWrap(True)
        self._summary = QPlainTextEdit()
        self._summary.setReadOnly(True)
        self._summary.setMinimumHeight(120)
        self._alert = QPlainTextEdit()
        self._alert.setReadOnly(True)
        self._alert.setMinimumHeight(100)
        self._info = QPlainTextEdit()
        self._info.setReadOnly(True)
        self._info.setMinimumHeight(220)

        controls_layout = QFormLayout()
        controls_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self._owner = QComboBox()
        self._owner.currentIndexChanged.connect(self._apply_owner)
        self._severity = QComboBox()
        for severity in ["Low", "Medium", "High", "Critical"]:
            self._severity.addItem(severity, severity)
        self._severity.currentIndexChanged.connect(self._apply_severity)
        action_row = QHBoxLayout()
        self._ack = QPushButton("Acknowledge")
        self._ack.clicked.connect(self._window._ack_selected_alert)
        self._refresh = QPushButton("Refresh")
        self._refresh.clicked.connect(self._window._refresh_selected_check_popout)
        action_row.addWidget(self._ack)
        action_row.addWidget(self._refresh)
        action_row.addStretch()

        layout.addWidget(self._title)
        layout.addWidget(self._status)
        layout.addWidget(self._meta)
        layout.addWidget(QLabel("Summary"))
        layout.addWidget(self._summary)
        controls_layout.addRow("Owner", self._owner)
        controls_layout.addRow("Severity", self._severity)
        layout.addLayout(controls_layout)
        layout.addLayout(action_row)
        layout.addWidget(QLabel("Alert"))
        layout.addWidget(self._alert)
        layout.addWidget(QLabel("Details"))
        layout.addWidget(self._info, 1)

    def _apply_owner(self) -> None:
        if self._owner.signalsBlocked():
            return
        if hasattr(self._window, "_details_owner_combo"):
            idx = self._window._details_owner_combo.findData(self._owner.currentData())
            if idx >= 0:
                self._window._details_owner_combo.setCurrentIndex(idx)
            else:
                self._window._apply_alert_owner()

    def _apply_severity(self) -> None:
        if self._severity.signalsBlocked():
            return
        if hasattr(self._window, "_details_severity_combo"):
            idx = self._window._details_severity_combo.findData(self._severity.currentData())
            if idx >= 0:
                self._window._details_severity_combo.setCurrentIndex(idx)
            else:
                self._window._apply_alert_severity()

    def set_payload(self, payload: dict[str, object]) -> None:
        self._title.setText(str(payload.get("title") or "Selected Check"))
        self._status.setText(str(payload.get("status") or ""))
        self._meta.setText(str(payload.get("meta") or ""))
        self._summary.setPlainText(str(payload.get("summary") or ""))
        self._alert.setPlainText(str(payload.get("alert") or ""))
        self._info.setPlainText(str(payload.get("info") or ""))

        owners = list(payload.get("owners") or [])
        current_owner = payload.get("owner_user_id")
        self._owner.blockSignals(True)
        self._owner.clear()
        for owner_id, owner_name in owners:
            self._owner.addItem(str(owner_name), owner_id)
        idx = self._owner.findData(current_owner)
        if idx < 0:
            idx = self._owner.findData(0)
        if idx >= 0:
            self._owner.setCurrentIndex(idx)
        self._owner.blockSignals(False)

        severity = str(payload.get("severity") or "Medium")
        self._severity.blockSignals(True)
        idx = self._severity.findData(severity)
        if idx >= 0:
            self._severity.setCurrentIndex(idx)
        self._severity.blockSignals(False)

        editable = bool(payload.get("can_edit") or False)
        ack_visible = bool(payload.get("show_ack") or False)
        self._owner.setEnabled(bool(payload.get("owner_enabled") or False))
        self._severity.setEnabled(editable)
        self._ack.setVisible(ack_visible)
        self._ack.setEnabled(ack_visible)

class _IncidentDetailsPopoutDialog(QDialog):
    def __init__(self, parent: "MainWindow") -> None:
        super().__init__(parent)
        self._window = parent
        self.setWindowTitle("Incident Details")
        self.resize(960, 760)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._header = QLabel("Incident")
        self._header.setObjectName("sectionTitle")
        self._header.setWordWrap(True)
        self._summary = QPlainTextEdit()
        self._summary.setReadOnly(True)
        self._summary.setMinimumHeight(260)
        self._timeline = QPlainTextEdit()
        self._timeline.setReadOnly(True)
        self._timeline.setMinimumHeight(280)
        actions = QHBoxLayout()
        self._refresh = QPushButton("Refresh")
        self._refresh.clicked.connect(self._window._refresh_incident_popout)
        actions.addStretch()
        actions.addWidget(self._refresh)

        layout.addWidget(self._header)
        layout.addWidget(QLabel("Summary"))
        layout.addWidget(self._summary, 1)
        layout.addWidget(QLabel("Timeline"))
        layout.addWidget(self._timeline, 1)
        layout.addLayout(actions)

    def set_payload(self, payload: dict[str, object]) -> None:
        self._header.setText(str(payload.get("header") or "Incident"))
        summary = str(payload.get("summary") or "")
        timeline = str(payload.get("timeline") or "")
        self._set_text_preserve_scroll(self._summary, summary)
        self._set_text_preserve_scroll(self._timeline, timeline)

    @staticmethod
    def _set_text_preserve_scroll(widget: QPlainTextEdit, text: str) -> None:
        if widget.toPlainText() == text:
            return
        scrollbar = widget.verticalScrollBar()
        value = scrollbar.value()
        maximum = max(1, scrollbar.maximum())
        ratio = value / maximum if maximum else 0.0
        cursor = widget.textCursor()
        position = cursor.position()
        widget.setPlainText(text)
        new_max = scrollbar.maximum()
        if value >= maximum - 2:
            target = new_max
        else:
            target = int(round(ratio * new_max)) if new_max > 0 else 0
        def _restore() -> None:
            scrollbar.setValue(max(0, min(target, scrollbar.maximum())))
            cur = widget.textCursor()
            cur.setPosition(max(0, min(position, len(widget.toPlainText()))))
            widget.setTextCursor(cur)
        QTimer.singleShot(0, _restore)
        QTimer.singleShot(30, _restore)

class _TileHoverFilter(QObject):
    def __init__(self, window: "MainWindow", check_id: int) -> None:
        super().__init__(window)
        self._window = window
        self._check_id = check_id

    def eventFilter(self, watched, event):
        if event.type() == QEvent.Type.Enter:
            self._window._hover_check_id = self._check_id
            self._window._apply_tile_focus_state()
        elif event.type() == QEvent.Type.Leave and self._window._hover_check_id == self._check_id:
            self._window._hover_check_id = None
            self._window._apply_tile_focus_state()
        return False


class TicketPreviewDialog(QDialog):
    def __init__(self, title: str, body: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(760, 560)
        layout = QVBoxLayout(self)
        header = QLabel(title)
        header.setObjectName("sectionTitle")
        layout.addWidget(header)
        self._body = QPlainTextEdit()
        self._body.setReadOnly(True)
        self._body.setPlainText(body)
        layout.addWidget(self._body, 1)
        row = QHBoxLayout()
        row.addStretch()
        copy_button = QPushButton("Copy to Clipboard")
        copy_button.setObjectName("salesforceAction")
        copy_button.clicked.connect(self._copy)
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        row.addWidget(copy_button)
        row.addWidget(close_button)
        layout.addLayout(row)

    def _copy(self) -> None:
        app = QApplication.instance()
        if app is not None:
            app.clipboard().setText(self._body.toPlainText())

class GraphDialog(QDialog):
    def __init__(self, title: str, rows: list[dict], threshold_min: float | None, threshold_max: float | None, graph_type: str = "Line", theme: dict | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"{title} - Large Graph")
        self.resize(980, 620)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        header = QLabel(title)
        header.setObjectName("sectionTitle")
        layout.addWidget(header)
        toolbar = QHBoxLayout()
        toolbar.addStretch()
        self._reset_zoom_button = QPushButton("Reset Zoom")
        self._reset_zoom_button.clicked.connect(self._reset_zoom)
        toolbar.addWidget(self._reset_zoom_button)
        layout.addLayout(toolbar)
        self._chart = HistoryChartWidget()
        self._chart.setMinimumHeight(480)
        if theme:
            self._chart.apply_theme(theme)
        self._chart.set_graph_type(graph_type)
        self._chart.set_data(title, rows, threshold_min, threshold_max)
        layout.addWidget(self._chart, 1)

    def _reset_zoom(self) -> None:
        self._chart.reset_zoom()


class UserDialog(QDialog):
    def __init__(self, role_names: list[str], parent=None, user_data: dict | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("User")
        self.resize(420, 260)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self._username = QLineEdit(str((user_data or {}).get("username") or ""))
        self._display_name = QLineEdit(str((user_data or {}).get("display_name") or ""))
        self._role = QComboBox()
        for role_name in role_names:
            self._role.addItem(role_name, role_name)
        current_role = str((user_data or {}).get("role_name") or "Operator")
        idx = self._role.findData(current_role)
        if idx >= 0:
            self._role.setCurrentIndex(idx)
        self._password = QLineEdit()
        self._password.setEchoMode(QLineEdit.EchoMode.Password)
        self._password.setPlaceholderText("Required for new users")
        self._active = QCheckBox("Active")
        self._active.setChecked(bool((user_data or {}).get("is_active", True)))
        form.addRow("Username", self._username)
        form.addRow("Display Name", self._display_name)
        form.addRow("Role", self._role)
        form.addRow("Password", self._password)
        form.addRow("Status", self._active)
        layout.addLayout(form)
        note = QLabel("Leave password blank when editing a user. Use Reset Password for password-only updates.")
        note.setWordWrap(True)
        layout.addWidget(note)
        row = QHBoxLayout()
        row.addStretch()
        save = QPushButton("Save")
        save.clicked.connect(self.accept)
        cancel = QPushButton("Cancel")
        cancel.clicked.connect(self.reject)
        row.addWidget(save)
        row.addWidget(cancel)
        layout.addLayout(row)

    def get_data(self) -> dict[str, object]:
        return {
            "username": self._username.text().strip(),
            "display_name": self._display_name.text().strip(),
            "role_name": self._role.currentData(),
            "password": self._password.text(),
            "is_active": self._active.isChecked(),
        }


class PasswordResetDialog(QDialog):
    def __init__(self, target_name: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Reset Password")
        self.resize(420, 180)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"Set a new password for {target_name}."))
        form = QFormLayout()
        self._password1 = QLineEdit()
        self._password1.setEchoMode(QLineEdit.EchoMode.Password)
        self._password2 = QLineEdit()
        self._password2.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("New Password", self._password1)
        form.addRow("Confirm Password", self._password2)
        layout.addLayout(form)
        row = QHBoxLayout()
        row.addStretch()
        save = QPushButton("Reset")
        save.clicked.connect(self.accept)
        cancel = QPushButton("Cancel")
        cancel.clicked.connect(self.reject)
        row.addWidget(save)
        row.addWidget(cancel)
        layout.addLayout(row)

    def get_password(self) -> str:
        return self._password1.text()

    def passwords_match(self) -> bool:
        return self._password1.text() == self._password2.text() and bool(self._password1.text())


class AcknowledgeDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Acknowledge Alert")
        self.resize(420, 220)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Optional acknowledgment reason:"))
        self._note = QPlainTextEdit()
        self._note.setPlaceholderText("Investigating, assigned owner, known issue, etc.")
        layout.addWidget(self._note)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def note(self) -> str:
        return self._note.toPlainText().strip()


class NotificationRuleDialog(QDialog):
    def __init__(self, parent=None, initial: dict | None = None, groups: list[tuple[int, str]] | None = None, checks: list[tuple[int, str]] | None = None) -> None:
        super().__init__(parent)
        initial = initial or {}
        groups = groups or []
        checks = checks or []
        self.setWindowTitle("Notification Rule")
        self.resize(460, 320)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self._name = QLineEdit(str(initial.get("rule_name") or ""))
        self._scope = QComboBox()
        self._scope.addItems(["Global", "Group", "Check"])
        self._scope.setCurrentText(str(initial.get("scope_type") or "Global"))
        self._scope_value = QComboBox()
        self._groups = groups
        self._checks = checks
        self._trigger = QComboBox()
        self._trigger.addItems(["AlertStarted", "AlertStateChanged", "AlertCleared", "DataBecameStale", "DataBecameFresh", "StaleStarted", "StaleCleared"])
        self._trigger.setCurrentText(str(initial.get("trigger_event") or "AlertStarted"))
        self._channel = QComboBox()
        self._channel.addItems(["FileLog", "TeamsWebhookPlaceholder", "EmailPlaceholder"])
        self._channel.setCurrentText(str(initial.get("channel_type") or "FileLog"))
        self._destination = QLineEdit(str(initial.get("destination") or "Logs/notifications.log"))
        self._renotify = QSpinBox()
        self._renotify.setMaximum(10080)
        self._renotify.setValue(int(initial.get("renotify_minutes") or 0))
        self._enabled = QCheckBox("Enabled")
        self._enabled.setChecked(bool(int(initial.get("is_enabled", 1) or 1)))
        self._scope.currentTextChanged.connect(lambda _: self._load_scope_values(initial.get("scope_value_id")))
        self._load_scope_values(initial.get("scope_value_id"))
        form.addRow("Rule Name", self._name)
        form.addRow("Scope", self._scope)
        form.addRow("Scope Value", self._scope_value)
        form.addRow("Trigger", self._trigger)
        form.addRow("Channel", self._channel)
        form.addRow("Destination", self._destination)
        form.addRow("Re-notify (min)", self._renotify)
        form.addRow("", self._enabled)
        layout.addLayout(form)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load_scope_values(self, selected=None) -> None:
        self._scope_value.clear()
        scope = self._scope.currentText()
        self._scope_value.addItem("(none)", None)
        options = self._groups if scope == "Group" else self._checks if scope == "Check" else []
        for value_id, label in options:
            self._scope_value.addItem(label, value_id)
        if selected is not None:
            idx = self._scope_value.findData(selected)
            if idx >= 0:
                self._scope_value.setCurrentIndex(idx)

    def get_data(self) -> dict[str, object]:
        return {
            "rule_name": self._name.text().strip(),
            "scope_type": self._scope.currentText(),
            "scope_value_id": self._scope_value.currentData(),
            "trigger_event": self._trigger.currentText(),
            "channel_type": self._channel.currentText(),
            "destination": self._destination.text().strip() or None,
            "renotify_minutes": self._renotify.value(),
            "is_enabled": self._enabled.isChecked(),
        }


class MaintenanceWindowDialog(QDialog):
    def __init__(self, parent=None, initial: dict | None = None, groups: list[tuple[int, str]] | None = None, checks: list[tuple[int, str]] | None = None) -> None:
        super().__init__(parent)
        initial = initial or {}
        groups = groups or []
        checks = checks or []
        self.setWindowTitle("Maintenance Window")
        self.resize(500, 360)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self._name = QLineEdit(str(initial.get("window_name") or ""))
        self._scope = QComboBox()
        self._scope.addItems(["Global", "Group", "Check"])
        self._scope.setCurrentText(str(initial.get("scope_type") or "Global"))
        self._scope_value = QComboBox()
        self._groups = groups
        self._checks = checks
        self._start = QDateTimeEdit()
        self._start.setCalendarPopup(True)
        self._start.setDisplayFormat("yyyy-MM-dd HH:mm")
        self._end = QDateTimeEdit()
        self._end.setCalendarPopup(True)
        self._end.setDisplayFormat("yyyy-MM-dd HH:mm")
        now = datetime.now()
        self._start.setDateTime(now)
        self._end.setDateTime(now + timedelta(hours=1))
        if initial.get("start_utc"):
            self._start.setDateTime(datetime.fromisoformat(str(initial["start_utc"]).replace("Z", "+00:00")).astimezone().replace(tzinfo=None))
        if initial.get("end_utc"):
            self._end.setDateTime(datetime.fromisoformat(str(initial["end_utc"]).replace("Z", "+00:00")).astimezone().replace(tzinfo=None))
        self._reason = QPlainTextEdit(str(initial.get("reason") or ""))
        self._reason.setFixedHeight(90)
        self._enabled = QCheckBox("Enabled")
        self._enabled.setChecked(bool(int(initial.get("is_enabled", 1) or 1)))
        self._scope.currentTextChanged.connect(lambda _: self._load_scope_values(initial.get("scope_value_id")))
        self._load_scope_values(initial.get("scope_value_id"))
        form.addRow("Window Name", self._name)
        form.addRow("Scope", self._scope)
        form.addRow("Scope Value", self._scope_value)
        form.addRow("Start", self._start)
        form.addRow("End", self._end)
        form.addRow("Reason", self._reason)
        form.addRow("", self._enabled)
        layout.addLayout(form)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load_scope_values(self, selected=None) -> None:
        self._scope_value.clear()
        scope = self._scope.currentText()
        self._scope_value.addItem("(none)", None)
        options = self._groups if scope == "Group" else self._checks if scope == "Check" else []
        for value_id, label in options:
            self._scope_value.addItem(label, value_id)
        if selected is not None:
            idx = self._scope_value.findData(selected)
            if idx >= 0:
                self._scope_value.setCurrentIndex(idx)

    def get_data(self) -> dict[str, object]:
        start_local = self._start.dateTime().toPython()
        end_local = self._end.dateTime().toPython()
        return {
            "window_name": self._name.text().strip(),
            "scope_type": self._scope.currentText(),
            "scope_value_id": self._scope_value.currentData(),
            "start_utc": start_local.astimezone().astimezone(timezone.utc).isoformat(),
            "end_utc": end_local.astimezone().astimezone(timezone.utc).isoformat(),
            "reason": self._reason.toPlainText().strip() or None,
            "is_enabled": self._enabled.isChecked(),
        }


class MainWindow(QMainWindow):
    def __init__(self, user: AuthenticatedUser, conn: sqlite3.Connection, monitoring_engine: MonitoringEngine) -> None:
        super().__init__()
        self._conn = conn
        self._user = user
        self._repo = DashboardRepository(conn)
        self._settings_repo = SettingsRepository(conn)
        self._monitoring_repo = MonitoringRepository(conn)
        self._config_repo = ConfigRepository(conn)
        self._user_repo = UserRepository(conn)
        self._monitoring_engine = monitoring_engine
        self._theme_service = ThemeService(conn)
        self._selected_check_id: int | None = None
        self._hover_check_id: int | None = None
        self._tile_widgets: dict[int, QFrame] = {}
        self._history_range = "24h"
        self._selected_config_check_id: int | None = None
        self._selected_group_id: int | None = None
        self._selected_deleted_check_id: int | None = None
        self._selected_deleted_group_id: int | None = None
        self._selected_user_id: int | None = None
        self._details_panel_expanded = False
        self._history_rows_cache: list[dict] = []
        self._incident_show_all_events = False
        self._selected_incident_key: str | None = None
        self._selected_audit_key: tuple[str, str, str, str] | None = None
        self._incident_selector_refreshing = False
        self._incident_context_map: dict[str, dict[str, str | bool]] = {}
        self._check_details_popout: _CheckDetailsPopoutDialog | None = None
        self._last_incident_refresh_token: tuple | None = None
        self._incident_popout_dialog: _IncidentDetailsPopoutDialog | None = None
        self._detail_mode = "details"
        self._detail_action_active = False
        self._transfer_engine = TransferEngine()
        self._transfer_status_timer = QTimer(self)
        self._transfer_status_timer.setInterval(2000)
        self._transfer_status_timer.timeout.connect(self._refresh_transfer_status_ui)
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(1680, 960)
        self._build_ui()
        self._run_retention_cleanup(force=True)

        self._timer = QTimer(self)
        self._timer.setInterval(5000)
        self._timer.timeout.connect(self._run_cycle_and_refresh)

        self._run_cycle_and_refresh()
        self._timer.start()
        self._transfer_status_timer.start()
        self._maybe_auto_start_transfer()

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        self._header = QFrame()
        self._header.setObjectName("header")
        header_layout = QHBoxLayout(self._header)
        header_layout.setContentsMargins(14, 10, 14, 10)
        self._app_icon_label = QLabel("▣")
        self._app_icon_label.setObjectName("headerIcon")
        self._title_label = QLabel("OpsMonitor")
        self._title_label.setObjectName("headerTitle")
        self._workspace_mode_label = QLabel("MONITORING MODE")
        self._workspace_mode_label.setObjectName("headerPill")
        self._workspace_mode_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._workspace_mode_label.setMinimumWidth(170)
        self._mode_site_label = QLabel("LIVE")
        self._mode_site_label.setObjectName("headerPillAlt")
        self._mode_site_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._mode_site_label.setMinimumWidth(110)
        self._shared_state_label = QLabel("Shared State")
        self._shared_state_label.setObjectName("headerMeta")
        self._shared_state_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._shared_state_label.setMinimumWidth(120)
        self._user_label = QLabel(f"{self._user.display_name} ▾")
        self._user_label.setObjectName("headerUser")
        self._preview_role_combo = QComboBox()
        self._preview_role_combo.addItems(["Admin", "Operator", "Viewer"])
        self._preview_role_combo.setVisible(self._user.role_name in {"ConfigAdmin", "SystemAdmin"})
        self._preview_role_combo.currentIndexChanged.connect(self._apply_role_preview_ui)
        self._preview_banner = QLabel("")
        self._preview_banner.setObjectName("detailCard")
        self._preview_banner.setVisible(False)
        self._refresh_label = QLabel("Last Console Update: --")
        self._zoom_out_button = QPushButton("−")
        self._zoom_out_button.setObjectName("headerIconButton")
        self._zoom_out_button.setToolTip("Decrease UI zoom")
        self._zoom_out_button.clicked.connect(self._decrease_zoom)
        self._header_zoom_label = QLabel("100%")
        self._header_zoom_label.setObjectName("headerMeta")
        self._zoom_in_button = QPushButton("+")
        self._zoom_in_button.setObjectName("headerIconButton")
        self._zoom_in_button.setToolTip("Increase UI zoom")
        self._zoom_in_button.clicked.connect(self._increase_zoom)
        self._site_segment = QFrame()
        self._site_segment.setObjectName("headerSegment")
        site_segment_layout = QHBoxLayout(self._site_segment)
        site_segment_layout.setContentsMargins(4, 4, 4, 4)
        site_segment_layout.setSpacing(4)
        self._site1_button = QPushButton("Site 1")
        self._site1_button.setCheckable(True)
        self._site1_button.setObjectName("headerSegmentButton")
        self._site1_button.clicked.connect(lambda checked=False: self._set_active_site("1"))
        self._site2_button = QPushButton("Site 2")
        self._site2_button.setCheckable(True)
        self._site2_button.setObjectName("headerSegmentButton")
        self._site2_button.clicked.connect(lambda checked=False: self._set_active_site("2"))
        site_segment_layout.addWidget(self._site1_button)
        site_segment_layout.addWidget(self._site2_button)
        self._incident_toggle_button = QPushButton("Start Incident")
        self._incident_toggle_button.clicked.connect(self._toggle_incident_mode)
        header_layout.addWidget(self._app_icon_label)
        header_layout.addWidget(self._title_label)
        header_layout.addSpacing(18)
        header_layout.addWidget(self._workspace_mode_label)
        header_layout.addWidget(self._mode_site_label)
        header_layout.addWidget(self._shared_state_label)
        header_layout.addStretch()
        if self._user.role_name in {"ConfigAdmin", "SystemAdmin"}:
            header_layout.addWidget(QLabel("Preview"))
            header_layout.addWidget(self._preview_role_combo)
        header_layout.addWidget(self._zoom_out_button)
        header_layout.addWidget(self._header_zoom_label)
        header_layout.addWidget(self._zoom_in_button)
        header_layout.addWidget(self._site_segment)
        header_layout.addWidget(self._incident_toggle_button)
        header_layout.addWidget(self._user_label)
        root.addWidget(self._header)
        root.addWidget(self._preview_banner)

        self._status_banner = QLabel("")
        self._status_banner.setObjectName("statusBanner")
        self._status_banner.setVisible(False)
        self._status_banner.setWordWrap(True)
        root.addWidget(self._status_banner)

        self._main_tabs = QTabWidget()
        self._main_tabs.setDocumentMode(True)
        self._main_tabs.tabBar().setDrawBase(False)
        self._main_tabs.currentChanged.connect(self._on_tab_changed)
        root.addWidget(self._main_tabs, 1)

        self._build_dashboard_tab()
        self._build_configuration_tab()
        self._build_audit_tab()
        self._build_users_tab()
        self._build_transfer_tab()
        self._build_settings_tab()
        self._apply_role_preview_ui()
        self._on_tab_changed(self._main_tabs.currentIndex())

        footer_row = QHBoxLayout()
        footer_row.setContentsMargins(2, 0, 2, 0)
        footer_row.addWidget(self._refresh_label)
        footer_row.addStretch()
        footer = QLabel("Build 48.3.0: transfer integration foundation and mapping-driven file normalization.")
        footer.setAlignment(Qt.AlignmentFlag.AlignRight)
        footer_row.addWidget(footer)
        root.addLayout(footer_row)

    def _build_dashboard_tab(self) -> None:
        self._dashboard_tab = QWidget()
        self._main_tabs.addTab(self._dashboard_tab, "Dashboard")
        root = QVBoxLayout(self._dashboard_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        self._summary_frame = None
        self._summary_buttons: dict[str, QPushButton] = {}

        self._incident_banner = None

        self._dashboard_content = QGridLayout()
        self._dashboard_content.setColumnStretch(0, 5)
        self._dashboard_content.setColumnStretch(1, 0)
        self._dashboard_content.setHorizontalSpacing(12)
        root.addLayout(self._dashboard_content, 1)

        self._dashboard_panel = QFrame()
        self._dashboard_panel.setObjectName("panel")
        dashboard_root = QVBoxLayout(self._dashboard_panel)
        dashboard_root.setContentsMargins(12, 12, 12, 12)
        dashboard_root.setSpacing(10)
        panel_header_row = QHBoxLayout()
        self._panel_header = QLabel(f"Monitoring Overview ({self._settings_repo.get_setting('dashboard_density', 'Compact')} View)")
        self._panel_header.setObjectName("sectionTitle")
        panel_header_row.addWidget(self._panel_header)
        panel_header_row.addStretch()
        self._issues_only_checkbox = QCheckBox("Show only issues")
        self._issues_only_checkbox.toggled.connect(self._toggle_show_only_issues)
        panel_header_row.addWidget(self._issues_only_checkbox)
        dashboard_root.addLayout(panel_header_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        dashboard_root.addWidget(scroll)

        self._dashboard_container = QWidget()
        self._dashboard_layout = QVBoxLayout(self._dashboard_container)
        self._dashboard_layout.setContentsMargins(0, 0, 0, 0)
        self._dashboard_layout.setSpacing(10)
        scroll.setWidget(self._dashboard_container)

        self._right_panel_scroll = QScrollArea()
        self._right_panel_scroll.setWidgetResizable(True)
        self._right_panel_scroll.setFrameShape(QFrame.Shape.NoFrame)

        self._right_panel = QFrame()
        self._right_panel.setObjectName("panel")
        right_layout = QVBoxLayout(self._right_panel)
        right_layout.setContentsMargins(12, 12, 12, 12)
        right_layout.setSpacing(10)

        self._selected_header = QFrame()
        self._selected_header.setObjectName("selectedHeader")
        selected_header_layout = QHBoxLayout(self._selected_header)
        selected_header_layout.setContentsMargins(10, 8, 10, 8)
        selected_header_layout.setSpacing(6)
        header_text_layout = QVBoxLayout()
        selected_title = QLabel("Selected Check")
        selected_title.setObjectName("sectionTitle")
        self._selected_name = QLabel("Choose a check tile")
        self._selected_name.setObjectName("selectedCheckName")
        self._selected_status = QLabel("")
        self._selected_status.setObjectName("selectedStatusPill")
        header_text_layout.addWidget(selected_title)
        header_text_layout.addWidget(self._selected_name)
        header_text_layout.addWidget(self._selected_status)
        selected_header_layout.addLayout(header_text_layout, 1)
        self._popout_details_button = QPushButton("Pop Out")
        self._popout_details_button.clicked.connect(self._open_selected_check_popout)
        selected_header_layout.addWidget(self._popout_details_button)
        self._selected_header.setVisible(False)
        right_layout.addWidget(self._selected_header)

        self._detail_mode_bar = QFrame()
        self._detail_mode_bar.setObjectName("selectedHeader")
        detail_mode_layout = QHBoxLayout(self._detail_mode_bar)
        detail_mode_layout.setContentsMargins(10, 8, 10, 8)
        detail_mode_layout.setSpacing(6)
        self._detail_mode_buttons = {}
        for key, label in [("details", "Details"), ("history", "History"), ("graph", "Graph")]:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.clicked.connect(lambda checked=False, m=key: self._switch_monitoring_pane_mode(m))
            self._detail_mode_buttons[key] = btn
            detail_mode_layout.addWidget(btn)
        detail_mode_layout.addStretch()
        self._collapse_details_button = QPushButton("Hide Details")
        self._collapse_details_button.clicked.connect(self._collapse_details_panel)
        detail_mode_layout.addWidget(self._collapse_details_button)
        self._detail_mode_bar.setVisible(False)
        right_layout.addWidget(self._detail_mode_bar)

        self._status_card = QFrame()
        self._status_card.setObjectName("detailCard")
        status_card_layout = QVBoxLayout(self._status_card)
        status_card_layout.setContentsMargins(10, 10, 10, 10)
        status_card_layout.setSpacing(4)
        self._details_name = QLabel("Select a check")
        self._details_name.setObjectName("tileTitle")
        self._details_state = QLabel("")
        self._details_state.setObjectName("detailStatusLabel")
        self._details_summary = QLabel("Choose a tile to see current value and alert details.")
        self._details_summary.setWordWrap(True)
        self._details_last_update = QLabel("")
        self._details_last_update.setObjectName("tileMeta")
        self._details_alert = QLabel("")
        self._details_alert.setWordWrap(True)
        self._details_owner_combo = QComboBox()
        self._details_owner_combo.currentIndexChanged.connect(self._apply_alert_owner)
        self._details_owner_combo.setVisible(False)
        self._details_severity_combo = QComboBox()
        for severity in ["Low", "Medium", "High", "Critical"]:
            self._details_severity_combo.addItem(severity, severity)
        self._details_severity_combo.currentIndexChanged.connect(self._apply_alert_severity)
        self._details_severity_combo.setVisible(False)
        self._details_ack_button = QPushButton("Acknowledge")
        self._details_ack_button.setObjectName("ackAction")
        self._details_ack_button.clicked.connect(self._ack_selected_alert)
        self._details_escalate_button = QPushButton("Escalate")
        self._details_escalate_button.setObjectName("escalateAction")
        self._details_escalate_button.clicked.connect(self._escalate_selected_alert)
        self._details_salesforce_button = QPushButton("Prepare Salesforce Ticket")
        self._details_salesforce_button.setObjectName("salesforceAction")
        self._details_salesforce_button.clicked.connect(self._prepare_salesforce_ticket)
        self._tier1_doc_button = QPushButton("Open Tier 1 Guide")
        self._tier1_doc_button.clicked.connect(lambda: self._open_troubleshooting_doc(1))
        self._tier2_doc_button = QPushButton("Open Tier 2 Guide")
        self._tier2_doc_button.clicked.connect(lambda: self._open_troubleshooting_doc(2))
        self._escalation_banner = QLabel("")
        self._escalation_banner.setObjectName("escalationBanner")
        self._escalation_banner.setVisible(False)
        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addWidget(self._details_ack_button)
        action_row.addWidget(self._details_escalate_button)
        action_row.addWidget(self._details_salesforce_button)
        action_row.addWidget(self._tier1_doc_button)
        action_row.addWidget(self._tier2_doc_button)
        action_row.addStretch()
        status_card_layout.addWidget(self._details_name)
        status_card_layout.addWidget(self._details_state)
        status_card_layout.addWidget(self._details_summary)
        status_card_layout.addWidget(self._details_last_update)
        status_card_layout.addWidget(self._details_alert)
        status_card_layout.addLayout(action_row)
        status_card_layout.addWidget(self._escalation_banner)
        right_layout.addWidget(self._status_card)

        self._info_card = QFrame()
        self._info_card.setObjectName("detailCard")
        info_card_layout = QVBoxLayout(self._info_card)
        info_card_layout.setContentsMargins(10, 10, 10, 10)
        info_card_layout.setSpacing(4)
        info_title = QLabel("Check Info")
        info_title.setObjectName("groupTitle")
        self._details_meta = QLabel("")
        self._details_meta.setWordWrap(True)
        info_card_layout.addWidget(info_title)
        info_card_layout.addWidget(self._details_meta)
        right_layout.addWidget(self._info_card)

        self._incident_card = QFrame()
        self._incident_card.setObjectName("detailCard")
        incident_card_layout = QVBoxLayout(self._incident_card)
        incident_card_layout.setContentsMargins(10, 10, 10, 10)
        incident_card_layout.setSpacing(6)
        incident_card_title = QLabel("Incident")
        incident_card_title.setObjectName("groupTitle")
        self._incident_card_status = QLabel("Incident mode is currently off.")
        self._incident_card_status.setWordWrap(True)
        self._incident_card_summary = QLabel("")
        self._incident_card_summary.setWordWrap(True)
        self._incident_card_last_event = QLabel("")
        self._incident_card_last_event.setWordWrap(True)
        self._incident_open_tab_button = QPushButton("Open Incident Timeline")
        self._incident_open_tab_button.clicked.connect(self._open_incident_tab)
        incident_card_layout.addWidget(incident_card_title)
        incident_card_layout.addWidget(self._incident_card_status)
        incident_card_layout.addWidget(self._incident_card_summary)
        incident_card_layout.addWidget(self._incident_card_last_event)
        incident_card_layout.addWidget(self._incident_open_tab_button)
        self._incident_card.setVisible(False)
        right_layout.addWidget(self._incident_card)

        self._detail_tabs = QTabWidget()
        self._detail_tabs.setDocumentMode(True)
        self._detail_tabs.currentChanged.connect(self._on_detail_tab_changed)
        right_layout.addWidget(self._detail_tabs, 1)

        self._history_tab = QWidget()
        hist_layout = QVBoxLayout(self._history_tab)
        hist_layout.setContentsMargins(6, 6, 6, 6)
        hist_layout.setSpacing(10)
        self._history_header = QLabel("Select a check to explore history.")
        self._history_header.setWordWrap(True)
        hist_layout.addWidget(self._history_header)

        range_row = QHBoxLayout()
        self._range_buttons: dict[str, QPushButton] = {}
        for key, label in [("1h", "1H"), ("6h", "6H"), ("24h", "24H"), ("7d", "7D"), ("30d", "30D")]:
            button = QPushButton(label)
            button.clicked.connect(lambda _=False, k=key: self._set_history_range(k))
            self._range_buttons[key] = button
            range_row.addWidget(button)
        range_row.addStretch()
        self._graph_type_combo = QComboBox()
        self._graph_type_combo.addItems(["Line", "Area", "Step Line", "State Timeline"])
        self._graph_type_combo.currentTextChanged.connect(self._on_graph_type_changed)
        range_row.addWidget(QLabel("Graph"))
        range_row.addWidget(self._graph_type_combo)
        self._open_large_graph_button = QPushButton("Open Large Graph")
        self._open_large_graph_button.clicked.connect(self._open_large_graph)
        self._reset_zoom_button = QPushButton("Reset Zoom")
        self._reset_zoom_button.clicked.connect(self._reset_history_zoom)
        self._export_csv_button = QPushButton("Export CSV")
        self._export_csv_button.clicked.connect(self._export_history_csv)
        self._export_png_button = QPushButton("Export PNG")
        self._export_png_button.clicked.connect(self._export_history_png)
        range_row.addWidget(self._open_large_graph_button)
        range_row.addWidget(self._reset_zoom_button)
        range_row.addWidget(self._export_csv_button)
        range_row.addWidget(self._export_png_button)
        hist_layout.addLayout(range_row)

        self._history_chart = HistoryChartWidget()
        self._history_chart.zoom_state_changed.connect(self._on_history_zoom_state_changed)
        hist_layout.addWidget(self._history_chart)
        self._history_summary = QLabel("No history loaded yet.")
        self._history_summary.setWordWrap(True)
        hist_layout.addWidget(self._history_summary)
        self._detail_tabs.addTab(self._history_tab, "History")

        self._events_tab = QWidget()
        detail_events_layout = QVBoxLayout(self._events_tab)
        detail_events_layout.setContentsMargins(6, 6, 6, 6)
        detail_events_layout.setSpacing(10)
        ev_title = QLabel("Recent Activity")
        ev_title.setObjectName("groupTitle")
        detail_events_layout.addWidget(ev_title)
        self._detail_events_layout = QVBoxLayout()
        self._detail_events_layout.setSpacing(6)
        detail_events_layout.addLayout(self._detail_events_layout)
        detail_events_layout.addStretch()
        self._detail_tabs.addTab(self._events_tab, "Events")

        self._incident_tab = QWidget()
        incident_layout = QVBoxLayout(self._incident_tab)
        incident_layout.setContentsMargins(6, 6, 6, 6)
        incident_layout.setSpacing(10)
        self._incident_header = QLabel("Incident mode is currently off.")
        self._incident_header.setWordWrap(True)
        incident_layout.addWidget(self._incident_header)
        self._incident_commander_edit = QLineEdit()
        self._incident_bridge_edit = QLineEdit()
        self._incident_note_entry_edit = QPlainTextEdit()
        self._incident_note_entry_edit.setFixedHeight(90)
        self._incident_note_entry_edit.setPlaceholderText("Add a timeline entry. Each entry is appended for the incident and shared across consoles.")
        self._incident_add_entry_button = QPushButton("Add Timeline Entry")
        self._incident_add_entry_button.clicked.connect(self._add_incident_timeline_entry)
        self._incident_commander_dirty = False
        self._incident_bridge_dirty = False
        self._incident_note_entry_dirty = False
        self._incident_metadata_autosave_timer = QTimer(self)
        self._incident_metadata_autosave_timer.setSingleShot(True)
        self._incident_metadata_autosave_timer.setInterval(1200)
        self._incident_metadata_autosave_timer.timeout.connect(self._autosave_incident_metadata_if_dirty)
        self._incident_commander_edit.textEdited.connect(self._mark_incident_commander_dirty)
        self._incident_bridge_edit.textEdited.connect(self._mark_incident_bridge_dirty)
        self._incident_note_entry_edit.textChanged.connect(self._mark_incident_note_entry_dirty)
        self._incident_commander_edit.editingFinished.connect(self._autosave_incident_metadata_if_dirty)
        self._incident_bridge_edit.editingFinished.connect(self._autosave_incident_metadata_if_dirty)
        self._incident_commander_edit.setVisible(False)
        self._incident_bridge_edit.setVisible(False)
        self._incident_note_entry_edit.setVisible(False)
        self._incident_add_entry_button.setVisible(False)
        incident_selector_row = QHBoxLayout()
        incident_selector_label = QLabel("Select Incident:")
        self._incident_selector = QComboBox()
        self._incident_selector.currentIndexChanged.connect(self._on_incident_selection_changed)
        incident_selector_row.addWidget(incident_selector_label)
        incident_selector_row.addWidget(self._incident_selector, 1)
        incident_layout.addLayout(incident_selector_row)
        incident_action_row = QHBoxLayout()
        self._export_incident_text_button = QPushButton("Export Summary")
        self._export_incident_text_button.clicked.connect(self._export_incident_summary_text)
        self._export_incident_csv_button = QPushButton("Export Timeline CSV")
        self._export_incident_csv_button.clicked.connect(self._export_incident_timeline_csv)
        self._incident_popout_button = QPushButton("Pop Out Details")
        self._incident_popout_button.clicked.connect(self._open_incident_popout)
        self._incident_show_all_checkbox = QCheckBox("Show all events since incident start")
        self._incident_show_all_checkbox.toggled.connect(self._toggle_incident_show_all_events)
        incident_action_row.addWidget(self._export_incident_text_button)
        incident_action_row.addWidget(self._export_incident_csv_button)
        incident_action_row.addWidget(self._incident_popout_button)
        incident_action_row.addWidget(self._incident_show_all_checkbox)
        incident_action_row.addStretch()
        incident_layout.addLayout(incident_action_row)
        self._incident_summary = QPlainTextEdit()
        self._incident_summary.setReadOnly(True)
        self._incident_summary.setMinimumHeight(170)
        self._incident_summary.setPlaceholderText("Incident summary will appear here once an incident is selected.")
        incident_layout.addWidget(self._incident_summary)

        leadership_title = QLabel("Leadership Timeline")
        leadership_title.setObjectName("groupTitle")
        incident_layout.addWidget(leadership_title)
        self._incident_leadership_layout = QVBoxLayout()
        self._incident_leadership_layout.setSpacing(6)
        incident_layout.addLayout(self._incident_leadership_layout)

        system_title = QLabel("System Activity")
        system_title.setObjectName("groupTitle")
        incident_layout.addWidget(system_title)
        self._incident_system_layout = QVBoxLayout()
        self._incident_system_layout.setSpacing(6)
        incident_layout.addLayout(self._incident_system_layout)
        incident_layout.addStretch()
        self._detail_tabs.addTab(self._incident_tab, "Historical Incidents")
        if self._main_tabs.indexOf(self._incident_tab) < 0:
            self._main_tabs.addTab(self._incident_tab, "Historical Incidents")
        self._detail_tabs.tabBar().hide()
        self._detail_tabs.setVisible(False)
        self._incident_card.setVisible(False)
        self._history_chart.setMinimumHeight(320)
        self._history_chart.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        self._recent_activity_title = QLabel("Recent Activity")
        self._recent_activity_title.setObjectName("sectionTitle")
        self._recent_activity_title.setVisible(False)
        right_layout.addWidget(self._recent_activity_title)
        self._event_list_layout = QVBoxLayout()
        self._event_list_layout.setSpacing(8)
        right_layout.addLayout(self._event_list_layout)
        right_layout.addStretch()

        self._right_panel_scroll.setWidget(self._right_panel)

        self._dashboard_content.addWidget(self._dashboard_panel, 0, 0)
        self._dashboard_content.addWidget(self._right_panel_scroll, 0, 1)

        self._incident_workspace = QFrame()
        self._incident_workspace.setObjectName("panel")
        self._incident_workspace.setVisible(False)
        incident_workspace_layout = QHBoxLayout(self._incident_workspace)
        incident_workspace_layout.setContentsMargins(16, 16, 16, 16)
        incident_workspace_layout.setSpacing(16)

        impacted_panel = QFrame()
        impacted_panel.setObjectName("detailCard")
        impacted_layout = QVBoxLayout(impacted_panel)
        impacted_layout.setContentsMargins(10, 10, 10, 10)
        impacted_layout.setSpacing(8)
        impacted_title = QLabel("Impacted Checks")
        impacted_title.setObjectName("sectionTitle")
        self._incident_impacted_summary = QLabel("Only active issues are shown here so responders can focus on what still needs action.")
        self._incident_impacted_summary.setObjectName("tileMeta")
        self._incident_impacted_summary.setWordWrap(True)
        impacted_layout.addWidget(impacted_title)
        impacted_layout.addWidget(self._incident_impacted_summary)
        self._incident_checks_scroll = QScrollArea()
        self._incident_checks_scroll.setWidgetResizable(True)
        self._incident_checks_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._incident_checks_container = QWidget()
        self._incident_checks_layout = QVBoxLayout(self._incident_checks_container)
        self._incident_checks_layout.setContentsMargins(0, 0, 0, 0)
        self._incident_checks_layout.setSpacing(8)
        self._incident_checks_scroll.setWidget(self._incident_checks_container)
        impacted_layout.addWidget(self._incident_checks_scroll, 1)
        incident_workspace_layout.addWidget(impacted_panel, 3)

        timeline_panel = QFrame()
        timeline_panel.setObjectName("detailCard")
        timeline_layout = QVBoxLayout(timeline_panel)
        timeline_layout.setContentsMargins(10, 10, 10, 10)
        timeline_layout.setSpacing(8)
        timeline_title = QLabel("Timeline")
        timeline_title.setObjectName("sectionTitle")
        self._incident_live_counts = QLabel("")
        self._incident_live_counts.setObjectName("tileMeta")
        timeline_stats_row = QHBoxLayout()
        timeline_stats_row.setSpacing(8)
        self._incident_workspace_status = QLabel("Status: Standby")
        self._incident_workspace_started = QLabel("Started: --")
        self._incident_workspace_impacted = QLabel("Impacted: 0")
        self._incident_workspace_events = QLabel("Events: 0")
        for badge in [self._incident_workspace_status, self._incident_workspace_started, self._incident_workspace_impacted, self._incident_workspace_events]:
            badge.setObjectName("headerMeta")
            badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
            badge.setStyleSheet("background-color: rgba(59,130,246,0.14); border: 1px solid rgba(148,163,184,0.35); border-radius: 10px; padding: 6px 10px;")
            timeline_stats_row.addWidget(badge)
        timeline_stats_row.addStretch()
        self._incident_live_feed = QPlainTextEdit()
        self._incident_live_feed.setReadOnly(True)
        self._incident_live_feed.setPlaceholderText("Live incident events will appear here in real time.")
        timeline_layout.addWidget(timeline_title)
        timeline_layout.addWidget(self._incident_live_counts)
        timeline_layout.addLayout(timeline_stats_row)
        timeline_layout.addWidget(self._incident_live_feed, 1)
        incident_workspace_layout.addWidget(timeline_panel, 7)

        info_panel = QFrame()
        info_panel.setObjectName("detailCard")
        info_layout = QVBoxLayout(info_panel)
        info_layout.setContentsMargins(12, 12, 12, 12)
        info_layout.setSpacing(10)

        info_title = QLabel("Incident Info")
        info_title.setObjectName("sectionTitle")
        self._incident_mode_header = QLabel("Incident mode is currently off.")
        self._incident_mode_header.setWordWrap(True)

        self._incident_owner_value = QLabel("--")
        self._incident_owner_value.setObjectName("tileMeta")

        self._incident_title_edit = QLineEdit()
        self._incident_title_edit.setPlaceholderText("Incident title")
        self._incident_title_edit.textEdited.connect(self._mark_incident_title_dirty)
        self._incident_title_edit.editingFinished.connect(self._autosave_incident_metadata_if_dirty)

        self._incident_mode_commander_edit = QLineEdit()
        self._incident_mode_commander_edit.setPlaceholderText("Who is leading the incident?")
        self._incident_mode_commander_edit.textEdited.connect(self._sync_incident_mode_commander_to_primary)
        self._incident_mode_commander_edit.editingFinished.connect(self._autosave_incident_metadata_if_dirty)

        self._incident_mode_bridge_edit = QLineEdit()
        self._incident_mode_bridge_edit.setPlaceholderText("Bridge link")
        self._incident_mode_bridge_edit.textEdited.connect(self._sync_incident_mode_bridge_to_primary)
        self._incident_mode_bridge_edit.editingFinished.connect(self._autosave_incident_metadata_if_dirty)

        self._incident_status_combo = QComboBox()
        self._incident_status_combo.addItems(["Active", "Monitoring", "Resolved"])
        self._incident_status_combo.currentIndexChanged.connect(self._mark_incident_status_dirty)

        self._incident_started_value = QLabel("--")
        self._incident_started_value.setObjectName("tileMeta")

        info_form = QFormLayout()
        info_form.addRow("Owner", self._incident_owner_value)
        info_form.addRow("Incident Commander", self._incident_mode_commander_edit)
        info_form.addRow("Bridge Link", self._incident_mode_bridge_edit)
        info_form.addRow("Title", self._incident_title_edit)
        info_form.addRow("Status", self._incident_status_combo)
        info_form.addRow("Start Time", self._incident_started_value)

        details_title = QLabel("Details")
        details_title.setObjectName("sectionTitle")
        self._incident_overview_edit = QPlainTextEdit()
        self._incident_overview_edit.setFixedHeight(110)
        self._incident_overview_edit.setPlaceholderText("Add a short overview so responders can quickly understand the incident.")
        self._incident_overview_edit.textChanged.connect(self._mark_incident_overview_dirty)

        entry_title = QLabel("Add Timeline Entry")
        entry_title.setObjectName("groupTitle")
        self._incident_mode_entry_edit = QPlainTextEdit()
        self._incident_mode_entry_edit.setFixedHeight(110)
        self._incident_mode_entry_edit.setPlaceholderText("Add a timeline entry for the active incident.")
        self._incident_mode_entry_edit.textChanged.connect(self._sync_incident_mode_entry_to_primary)
        self._incident_mode_add_button = QPushButton("Add Timeline Entry")
        self._incident_mode_add_button.clicked.connect(self._add_incident_timeline_entry)
        self._incident_mode_export_summary_button = QPushButton("Export Summary")
        self._incident_mode_export_summary_button.clicked.connect(self._export_incident_summary_text)
        self._incident_mode_export_timeline_button = QPushButton("Export Timeline CSV")
        self._incident_mode_export_timeline_button.clicked.connect(self._export_incident_timeline_csv)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addWidget(self._incident_mode_add_button)

        incident_action_title = QLabel("Incident Actions")
        incident_action_title.setObjectName("groupTitle")
        self._incident_selected_check_label = QLabel("No impacted check selected")
        self._incident_selected_check_label.setObjectName("tileMeta")
        self._incident_mode_escalate_button = QPushButton("Escalate")
        self._incident_mode_escalate_button.clicked.connect(self._escalate_selected_alert)
        self._incident_mode_salesforce_button = QPushButton("Prepare Salesforce Ticket")
        self._incident_mode_salesforce_button.clicked.connect(self._prepare_salesforce_ticket)
        incident_action_row = QHBoxLayout()
        incident_action_row.setSpacing(8)
        incident_action_row.addWidget(self._incident_mode_escalate_button)
        incident_action_row.addWidget(self._incident_mode_salesforce_button)
        incident_action_row.addStretch()

        export_row = QHBoxLayout()
        export_row.setSpacing(8)
        export_row.addWidget(self._incident_mode_export_summary_button)
        export_row.addWidget(self._incident_mode_export_timeline_button)

        info_layout.addWidget(info_title)
        info_layout.addWidget(self._incident_mode_header)
        info_layout.addLayout(info_form)
        info_layout.addWidget(details_title)
        info_layout.addWidget(self._incident_overview_edit)
        info_layout.addWidget(entry_title)
        info_layout.addWidget(self._incident_mode_entry_edit)
        info_layout.addLayout(action_row)
        info_layout.addWidget(incident_action_title)
        info_layout.addWidget(self._incident_selected_check_label)
        info_layout.addLayout(incident_action_row)
        info_layout.addLayout(export_row)
        info_layout.addStretch()
        incident_workspace_layout.addWidget(info_panel, 3)

        root.addWidget(self._incident_workspace, 1)
        self._collapse_details_panel(initial=True)

    def _sync_incident_mode_commander_to_primary(self, value: str) -> None:
        if hasattr(self, "_incident_commander_edit") and self._incident_commander_edit.text() != value:
            self._incident_commander_edit.blockSignals(True)
            self._incident_commander_edit.setText(value)
            self._incident_commander_edit.blockSignals(False)
        self._mark_incident_commander_dirty()

    def _sync_incident_mode_bridge_to_primary(self, value: str) -> None:
        if hasattr(self, "_incident_bridge_edit") and self._incident_bridge_edit.text() != value:
            self._incident_bridge_edit.blockSignals(True)
            self._incident_bridge_edit.setText(value)
            self._incident_bridge_edit.blockSignals(False)
        self._mark_incident_bridge_dirty()

    def _sync_incident_mode_entry_to_primary(self) -> None:
        if hasattr(self, "_incident_note_entry_edit"):
            value = self._incident_mode_entry_edit.toPlainText()
            if self._incident_note_entry_edit.toPlainText() != value:
                self._incident_note_entry_edit.blockSignals(True)
                self._incident_note_entry_edit.setPlainText(value)
                self._incident_note_entry_edit.blockSignals(False)
        self._mark_incident_note_entry_dirty()

    def _set_plaintext_preserve_scroll(self, widget: QPlainTextEdit, text: str) -> None:
        if widget.toPlainText() == text:
            return
        scrollbar = widget.verticalScrollBar()
        value = scrollbar.value()
        maximum = max(1, scrollbar.maximum())
        ratio = value / maximum if maximum else 0.0
        updates_prev = widget.updatesEnabled()
        widget.setUpdatesEnabled(False)
        try:
            widget.setPlainText(text)
            new_max = scrollbar.maximum()
            target = new_max if value >= maximum - 2 else int(round(ratio * new_max)) if new_max > 0 else 0
            scrollbar.setValue(max(0, min(target, scrollbar.maximum())))
        finally:
            widget.setUpdatesEnabled(updates_prev)
            widget.viewport().update()

    def _build_incident_check_card(self, row) -> QFrame:
        state = str(row["operational_state"] or "Unknown")
        check_id = int(row["check_id"]) if "check_id" in row.keys() and row["check_id"] is not None else None
        frame = QFrame()
        frame.setObjectName("checkTile")
        frame.setProperty("state", state)
        frame.setProperty("selected", check_id is not None and check_id == self._selected_check_id)
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(10)

        dot = QLabel("●")
        dot.setObjectName("incidentDot")
        if state == "Unhealthy":
            dot.setStyleSheet("color: #EF4444;")
        elif state == "Stale":
            dot.setStyleSheet("color: #FBBF24;")
        else:
            dot.setStyleSheet("color: #34D399;")
        layout.addWidget(dot, 0, Qt.AlignmentFlag.AlignTop)

        text_col = QVBoxLayout()
        text_col.setSpacing(2)
        title = QLabel(str(row["check_label"]))
        title.setObjectName("groupTitle")
        summary = QLabel(str(row["last_detail_message"]) if "last_detail_message" in row.keys() and row["last_detail_message"] else self._format_combined_state(row).title())
        summary.setObjectName("tileMeta")
        summary.setWordWrap(True)
        text_col.addWidget(title)
        text_col.addWidget(summary)
        layout.addLayout(text_col, 1)

        chevron = QLabel("›")
        chevron.setObjectName("tileMeta")
        chevron.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(chevron)
        if check_id is not None:
            frame.mousePressEvent = lambda event, cid=check_id: self._select_incident_check(cid, event)
        return frame

    def _refresh_dashboard_workspace_mode(self) -> None:
        incident_active = self._is_incident_active()
        if hasattr(self, "_workspace_mode_label"):
            if incident_active:
                self._workspace_mode_label.setText("INCIDENT MODE")
                self._workspace_mode_label.setStyleSheet("background-color: #7C2D12; color: white; border: 1px solid #FDBA74; border-radius: 12px; padding: 6px 12px; font-weight: 800;")
            else:
                self._workspace_mode_label.setText("MONITORING MODE")
                self._workspace_mode_label.setStyleSheet("background-color: #1E3A5F; color: white; border: 1px solid #93C5FD; border-radius: 12px; padding: 6px 12px; font-weight: 800;")
        if self._summary_frame is not None:
            self._summary_frame.setVisible(False)
        if hasattr(self, "_dashboard_content"):
            for i in range(self._dashboard_content.count()):
                item = self._dashboard_content.itemAt(i)
                widget = item.widget() if item else None
                if widget is not None:
                    widget.setVisible(not incident_active)
        if hasattr(self, "_incident_workspace"):
            self._incident_workspace.setVisible(incident_active)

    def _refresh_incident_workspace(self, context) -> None:
        if not hasattr(self, "_incident_workspace"):
            return
        active = bool(context and context.get("is_active"))
        self._refresh_dashboard_workspace_mode()
        if not active:
            return
        rows = self._get_incident_timeline_rows(context) if context else []
        impacted = list(self._conn.execute(
            """
            SELECT c.check_id, c.display_label AS check_label, c.description, ccs.operational_state, ccs.condition_state, ccs.freshness_state,
                   ccs.is_acknowledged, ccs.severity, ccs.last_detail_message, u.display_name AS owner_name
            FROM current_check_status ccs
            JOIN checks c ON c.check_id = ccs.check_id
            LEFT JOIN users u ON u.user_id = ccs.owner_user_id
            WHERE ccs.operational_state != 'Healthy'
            ORDER BY CASE WHEN ccs.operational_state = 'Unhealthy' THEN 0 WHEN ccs.operational_state = 'Stale' THEN 1 ELSE 2 END,
                     c.display_label
            """
        ).fetchall())
        self._clear_layout(self._incident_checks_layout)
        if impacted:
            for row in impacted:
                self._incident_checks_layout.addWidget(self._build_incident_check_card(row))
        else:
            empty = QLabel("No impacted checks are currently active.")
            empty.setWordWrap(True)
            self._incident_checks_layout.addWidget(empty)
        self._incident_checks_layout.addStretch()
        self._update_incident_action_state()
        feed_lines = []
        for row in rows:
            line = f"{self._format_time(str(row['event_utc']))} • {row['event_type']}"
            if row.get('check_label'):
                line += f" • {row['check_label']}"
            if row.get('user_name'):
                line += f" • {row['user_name']}"
            line += f"\n{row['message']}"
            if row.get('detail'):
                line += f"\n{row['detail']}"
            feed_lines.append(line)
        self._incident_live_counts.setText(f"Impacted Checks: {len(impacted)} | Timeline Events: {len(rows)}")
        self._set_plaintext_preserve_scroll(self._incident_live_feed, "\n\n".join(feed_lines) if feed_lines else "No incident timeline events yet.")
        commander_value = self._settings_repo.get_setting(f"incident_commander_{str(context.get('key') or '')}", self._settings_repo.get_setting("incident_commander_current", ""))
        bridge_value = self._settings_repo.get_setting(f"incident_bridge_{str(context.get('key') or '')}", self._settings_repo.get_setting("incident_bridge_current", ""))
        if not self._incident_mode_commander_edit.hasFocus() and not getattr(self, "_incident_commander_dirty", False):
            self._incident_mode_commander_edit.blockSignals(True)
            self._incident_mode_commander_edit.setText(commander_value)
            self._incident_mode_commander_edit.blockSignals(False)
        if hasattr(self, "_incident_mode_bridge_edit") and not self._incident_mode_bridge_edit.hasFocus() and not getattr(self, "_incident_bridge_dirty", False):
            self._incident_mode_bridge_edit.blockSignals(True)
            self._incident_mode_bridge_edit.setText(bridge_value)
            self._incident_mode_bridge_edit.blockSignals(False)
        if not self._incident_mode_entry_edit.hasFocus() and not getattr(self, "_incident_note_entry_dirty", False):
            current_note = self._incident_note_entry_edit.toPlainText() if hasattr(self, "_incident_note_entry_edit") else ""
            if self._incident_mode_entry_edit.toPlainText() != current_note:
                self._incident_mode_entry_edit.blockSignals(True)
                self._incident_mode_entry_edit.setPlainText(current_note)
                self._incident_mode_entry_edit.blockSignals(False)
        incident_key = str(context.get("key") or "")
        if hasattr(self, "_incident_owner_value"):
            self._incident_owner_value.setText(str(context.get("started_by") or "") or "Unknown")
        title_value = self._settings_repo.get_setting(f"incident_title_{incident_key}", "")
        status_value = self._settings_repo.get_setting(f"incident_status_{incident_key}", "Active")
        overview_value = self._settings_repo.get_setting(f"incident_overview_{incident_key}", "")
        if hasattr(self, "_incident_started_value"):
            started_value = str(context.get("start_utc") or self._settings_repo.get_setting("incident_start_utc", ""))
            self._incident_started_value.setText(self._format_time(started_value) if started_value else "--")
        if hasattr(self, "_incident_title_edit") and (not self._incident_title_edit.hasFocus() and not getattr(self, "_incident_title_dirty", False)):
            self._incident_title_edit.blockSignals(True)
            self._incident_title_edit.setText(title_value)
            self._incident_title_edit.blockSignals(False)
        if hasattr(self, "_incident_status_combo") and not getattr(self, "_incident_status_dirty", False):
            idx = self._incident_status_combo.findText(status_value)
            if idx >= 0 and self._incident_status_combo.currentIndex() != idx:
                self._incident_status_combo.blockSignals(True)
                self._incident_status_combo.setCurrentIndex(idx)
                self._incident_status_combo.blockSignals(False)
        if hasattr(self, "_incident_overview_edit") and (not self._incident_overview_edit.hasFocus() and not getattr(self, "_incident_overview_dirty", False)):
            if self._incident_overview_edit.toPlainText() != overview_value:
                self._incident_overview_edit.blockSignals(True)
                self._incident_overview_edit.setPlainText(overview_value)
                self._incident_overview_edit.blockSignals(False)
        if hasattr(self, "_incident_workspace_status"):
            self._incident_workspace_status.setText(f"Status: {status_value or 'Active'}")
        self._incident_mode_header.setText(self._incident_header.text() if hasattr(self, "_incident_header") else "Active incident")
        if impacted:
            self._incident_impacted_summary.setText(f"{len(impacted)} active checks need attention right now. Cards stay focused on currently impacted services only.")
        else:
            self._incident_impacted_summary.setText("No active impacted checks. The command center is clear.")

    def _build_configuration_tab(self) -> None:
        self._config_tab = QWidget()
        self._main_tabs.addTab(self._config_tab, "Alert Configuration")
        root = QVBoxLayout(self._config_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        info = QLabel("Manage checks and groups inside the application. Changes are stored in the local database and audited.")
        info.setWordWrap(True)
        root.addWidget(info)

        layout = QGridLayout()
        layout.setColumnStretch(0, 3)
        layout.setColumnStretch(1, 2)
        layout.setHorizontalSpacing(12)
        root.addLayout(layout, 1)

        checks_panel = QFrame()
        checks_panel.setObjectName("panel")
        checks_layout = QVBoxLayout(checks_panel)
        checks_layout.setContentsMargins(12, 12, 12, 12)
        checks_layout.setSpacing(10)
        checks_title = QLabel("Checks")
        checks_title.setObjectName("sectionTitle")
        checks_layout.addWidget(checks_title)
        checks_button_row = QHBoxLayout()
        self._new_check_button = QPushButton("New Check")
        self._new_check_button.clicked.connect(self._new_check)
        self._edit_check_button = QPushButton("Edit Check")
        self._edit_check_button.clicked.connect(self._edit_selected_check)
        self._duplicate_check_button = QPushButton("Duplicate")
        self._duplicate_check_button.clicked.connect(self._duplicate_selected_check)
        self._delete_check_button = QPushButton("Delete")
        self._delete_check_button.clicked.connect(self._delete_selected_check)
        self._toggle_check_button = QPushButton("Enable / Disable")
        self._toggle_check_button.clicked.connect(self._toggle_selected_check)
        self._refresh_config_button = QPushButton("Refresh")
        self._refresh_config_button.clicked.connect(self._load_configuration_data)
        checks_button_row.addWidget(self._new_check_button)
        checks_button_row.addWidget(self._edit_check_button)
        checks_button_row.addWidget(self._duplicate_check_button)
        checks_button_row.addWidget(self._delete_check_button)
        checks_button_row.addWidget(self._toggle_check_button)
        checks_button_row.addStretch()
        checks_button_row.addWidget(self._refresh_config_button)
        checks_layout.addLayout(checks_button_row)
        self._checks_table = QTableWidget(0, 8)
        self._checks_table.setHorizontalHeaderLabels(["Check", "Group", "Rule", "Parser", "Site 1", "Site 2", "Timing", "Enabled"])
        self._checks_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._checks_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._checks_table.itemSelectionChanged.connect(self._on_config_check_selection_changed)
        self._checks_table.setAlternatingRowColors(True)
        self._checks_table.verticalHeader().setVisible(False)
        self._checks_table.setShowGrid(False)
        self._checks_table.setWordWrap(False)
        checks_layout.addWidget(self._checks_table)

        groups_panel = QFrame()
        groups_panel.setObjectName("panel")
        groups_layout = QVBoxLayout(groups_panel)
        groups_layout.setContentsMargins(12, 12, 12, 12)
        groups_layout.setSpacing(10)
        groups_title = QLabel("Groups")
        groups_title.setObjectName("sectionTitle")
        groups_layout.addWidget(groups_title)
        groups_button_row = QHBoxLayout()
        self._new_group_button = QPushButton("New Group")
        self._new_group_button.clicked.connect(self._new_group)
        self._edit_group_button = QPushButton("Edit Group")
        self._edit_group_button.clicked.connect(self._edit_selected_group)
        self._delete_group_button = QPushButton("Delete Group")
        self._delete_group_button.clicked.connect(self._delete_selected_group)
        groups_button_row.addWidget(self._new_group_button)
        groups_button_row.addWidget(self._edit_group_button)
        groups_button_row.addWidget(self._delete_group_button)
        groups_button_row.addStretch()
        groups_layout.addLayout(groups_button_row)
        self._groups_table = QTableWidget(0, 4)
        self._groups_table.setHorizontalHeaderLabels(["Group", "Internal Name", "Order", "Enabled"])
        self._groups_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._groups_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._groups_table.itemSelectionChanged.connect(self._on_group_selection_changed)
        self._groups_table.setAlternatingRowColors(True)
        self._groups_table.verticalHeader().setVisible(False)
        self._groups_table.setShowGrid(False)
        self._groups_table.setWordWrap(False)
        groups_layout.addWidget(self._groups_table)

        deleted_checks_panel = QFrame()
        deleted_checks_panel.setObjectName("panel")
        deleted_checks_layout = QVBoxLayout(deleted_checks_panel)
        deleted_checks_layout.setContentsMargins(12, 12, 12, 12)
        deleted_checks_layout.setSpacing(10)
        deleted_checks_title = QLabel("Deleted Checks")
        deleted_checks_title.setObjectName("sectionTitle")
        deleted_checks_layout.addWidget(deleted_checks_title)
        deleted_checks_layout.addWidget(QLabel("Deleted checks can be restored here. Historical event data is never removed."))
        self._deleted_checks_table = QTableWidget(0, 3)
        self._deleted_checks_table.setHorizontalHeaderLabels(["Check", "Group", "Deleted"])
        self._deleted_checks_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._deleted_checks_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._deleted_checks_table.verticalHeader().setVisible(False)
        self._deleted_checks_table.setShowGrid(False)
        self._deleted_checks_table.itemSelectionChanged.connect(self._on_deleted_check_selection_changed)
        deleted_checks_layout.addWidget(self._deleted_checks_table)
        deleted_check_buttons = QHBoxLayout()
        self._restore_check_button = QPushButton("Restore Check")
        self._restore_check_button.clicked.connect(self._restore_selected_check)
        self._retire_check_button = QPushButton("Permanent Delete")
        self._retire_check_button.clicked.connect(self._retire_selected_check)
        deleted_check_buttons.addWidget(self._restore_check_button)
        deleted_check_buttons.addWidget(self._retire_check_button)
        deleted_check_buttons.addStretch()
        deleted_checks_layout.addLayout(deleted_check_buttons)

        deleted_groups_panel = QFrame()
        deleted_groups_panel.setObjectName("panel")
        deleted_groups_layout = QVBoxLayout(deleted_groups_panel)
        deleted_groups_layout.setContentsMargins(12, 12, 12, 12)
        deleted_groups_layout.setSpacing(10)
        deleted_groups_title = QLabel("Deleted Groups")
        deleted_groups_title.setObjectName("sectionTitle")
        deleted_groups_layout.addWidget(deleted_groups_title)
        deleted_groups_layout.addWidget(QLabel("Deleted groups can be restored here. Historical event data is never removed."))
        self._deleted_groups_table = QTableWidget(0, 2)
        self._deleted_groups_table.setHorizontalHeaderLabels(["Group", "Deleted"])
        self._deleted_groups_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._deleted_groups_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._deleted_groups_table.verticalHeader().setVisible(False)
        self._deleted_groups_table.setShowGrid(False)
        self._deleted_groups_table.itemSelectionChanged.connect(self._on_deleted_group_selection_changed)
        deleted_groups_layout.addWidget(self._deleted_groups_table)
        deleted_group_buttons = QHBoxLayout()
        self._restore_group_button = QPushButton("Restore Group")
        self._restore_group_button.clicked.connect(self._restore_selected_group)
        self._retire_group_button = QPushButton("Permanent Delete")
        self._retire_group_button.clicked.connect(self._retire_selected_group)
        deleted_group_buttons.addWidget(self._restore_group_button)
        deleted_group_buttons.addWidget(self._retire_group_button)
        deleted_group_buttons.addStretch()
        deleted_groups_layout.addLayout(deleted_group_buttons)

        layout.addWidget(checks_panel, 0, 0)
        layout.addWidget(groups_panel, 0, 1)
        layout.addWidget(deleted_checks_panel, 1, 0)
        layout.addWidget(deleted_groups_panel, 1, 1)

        if not self._can_manage_config():
            for widget in [self._new_check_button, self._edit_check_button, self._duplicate_check_button, self._delete_check_button, self._toggle_check_button, self._new_group_button, self._edit_group_button, getattr(self, "_delete_group_button", None), getattr(self, "_restore_check_button", None), getattr(self, "_retire_check_button", None), getattr(self, "_restore_group_button", None), getattr(self, "_retire_group_button", None)]:
                if widget is None:
                    continue
                widget.setEnabled(False)


    def _build_audit_tab(self) -> None:
        self._audit_tab = QWidget()
        self._main_tabs.addTab(self._audit_tab, "Audit")
        root = QVBoxLayout(self._audit_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        info = QLabel("Administrative and operational actions are recorded here for accountability and review.")
        info.setWordWrap(True)
        root.addWidget(info)

        panel = QFrame()
        panel.setObjectName("panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        title = QLabel("Audit History")
        title.setObjectName("sectionTitle")
        layout.addWidget(title)
        self._audit_table = QTableWidget(0, 6)
        self._audit_table.setHorizontalHeaderLabels(["Time", "User", "Action", "Entity", "Name", "Message"])
        self._audit_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._audit_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._audit_table.itemSelectionChanged.connect(self._on_audit_selection_changed)
        self._audit_table.setAlternatingRowColors(True)
        self._audit_table.verticalHeader().setVisible(False)
        self._audit_table.setShowGrid(False)
        self._audit_table.setWordWrap(True)
        layout.addWidget(self._audit_table)
        root.addWidget(panel, 1)


    def _make_transfer_form_label(self, text: str, tooltip: str) -> QLabel:
        label = QLabel(text)
        label.setToolTip(tooltip)
        label.setWhatsThis(tooltip)
        return label

    def _configure_transfer_mapping_header_tooltips(self) -> None:
        if not hasattr(self, "_transfer_mappings_table"):
            return
        tips = {
            0: "Enable or disable this mapping without deleting it.",
            1: "Friendly name for this mapping. This is shown in transfer results and helps identify the file source.",
            2: "Remote filename or wildcard pattern to match on the source system. Example: TrainSchedule*.log",
            3: "Stable local filename the app will use after transfer. Alerts should point to this normalized name.",
            4: "Optional subfolder under the selected site's destination root. Leave blank to save directly in the site root.",
            5: "Choose whether this mapping applies to Site 1, Site 2, or Both.",
            6: "Keep a copy of the raw downloaded file in addition to the normalized stable file.",
        }
        for idx, tip in tips.items():
            item = self._transfer_mappings_table.horizontalHeaderItem(idx)
            if item is not None:
                item.setToolTip(tip)

    def _autosize_transfer_mapping_columns(self) -> None:
        if not hasattr(self, "_transfer_mappings_table"):
            return
        self._transfer_mappings_table.resizeColumnsToContents()
        header = self._transfer_mappings_table.horizontalHeader()
        try:
            header.setStretchLastSection(False)
        except Exception:
            pass

    def _remember_transfer_mapping_selection(self) -> tuple[str, int] | None:
        if not hasattr(self, "_transfer_mappings_table"):
            return None
        row = self._transfer_mappings_table.currentRow()
        if row < 0:
            return None
        item = self._transfer_mappings_table.item(row, 3) or self._transfer_mappings_table.item(row, 1)
        if item is None:
            return ("", row)
        return (item.text().strip(), row)

    def _restore_transfer_mapping_selection(self, selection: tuple[str, int] | None) -> None:
        if not selection or not hasattr(self, "_transfer_mappings_table"):
            return
        wanted_key, fallback_row = selection
        target_row = -1
        if wanted_key:
            for row in range(self._transfer_mappings_table.rowCount()):
                item = self._transfer_mappings_table.item(row, 3) or self._transfer_mappings_table.item(row, 1)
                if item and item.text().strip() == wanted_key:
                    target_row = row
                    break
        if target_row < 0 and 0 <= fallback_row < self._transfer_mappings_table.rowCount():
            target_row = fallback_row
        if target_row >= 0:
            self._transfer_mappings_table.selectRow(target_row)
            self._transfer_mappings_table.setCurrentCell(target_row, 1)

    def _set_all_transfer_mappings_enabled(self, enabled: bool) -> None:
        if not hasattr(self, "_transfer_mappings_table"):
            return
        state = Qt.CheckState.Checked if enabled else Qt.CheckState.Unchecked
        for row in range(self._transfer_mappings_table.rowCount()):
            item = self._transfer_mappings_table.item(row, 0)
            if item is not None:
                item.setCheckState(state)

    def _build_transfer_tab(self) -> None:
        self._transfer_tab = QWidget()
        self._main_tabs.addTab(self._transfer_tab, "Transfer")
        root = QVBoxLayout(self._transfer_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        info = QLabel("Manage live transfer settings, site connection profiles, and stable file mappings. The active site is controlled by the top banner switcher.")
        info.setWordWrap(True)
        root.addWidget(info)

        self._transfer_subtabs = QTabWidget()
        self._transfer_subtabs.setDocumentMode(True)
        root.addWidget(self._transfer_subtabs, 1)

        general_tab = QWidget()
        general_layout = QVBoxLayout(general_tab)
        general_layout.setContentsMargins(0, 0, 0, 0)
        general_layout.setSpacing(12)
        general_panel = QFrame()
        general_panel.setObjectName("panel")
        gp = QVBoxLayout(general_panel)
        gp.setContentsMargins(12, 12, 12, 12)
        gp.setSpacing(10)
        general_title = QLabel("General")
        general_title.setObjectName("sectionTitle")
        gp.addWidget(general_title)
        general_form = QFormLayout()
        self._transfer_enabled_checkbox = QCheckBox("Enable transfer engine")
        self._transfer_autostart_checkbox = QCheckBox("Auto-start transfer on app load/login")
        self._transfer_interval_spin = QSpinBox()
        self._transfer_interval_spin.setRange(5, 3600)
        self._transfer_interval_spin.setValue(18)
        self._transfer_putty_path_edit = QLineEdit()
        self._transfer_key_path_edit = QLineEdit()
        self._transfer_script_root_edit = QLineEdit()
        self._transfer_files_script_edit = QLineEdit()
        self._transfer_cycle_script_edit = QLineEdit()
        self._transfer_active_site_label = QLabel("Site 1")
        general_form.addRow(self._make_transfer_form_label("Enabled", "Turn the transfer engine on or off. When disabled, no automatic or manual transfer cycles will run."), self._transfer_enabled_checkbox)
        general_form.addRow(self._make_transfer_form_label("Auto-start", "Start the transfer engine automatically after the app finishes loading and the user is in."), self._transfer_autostart_checkbox)
        general_form.addRow(self._make_transfer_form_label("Cycle Interval (seconds)", "How often the transfer engine should run. Lower values check more often but create more activity."), self._transfer_interval_spin)
        general_form.addRow(self._make_transfer_form_label("psftp.exe Path", "Full path to psftp.exe used for the transfer. Example: C:\\Program Files\\PuTTY\\psftp.exe"), self._transfer_putty_path_edit)
        general_form.addRow(self._make_transfer_form_label("PPK Key Path", "Full path to the PuTTY private key (.ppk) used to authenticate to the customer system."), self._transfer_key_path_edit)
        general_form.addRow(self._make_transfer_form_label("Script Root", "Folder that contains the FTP script files used by psftp. The Files Script and Cycle Script values are resolved relative to this folder."), self._transfer_script_root_edit)
        general_form.addRow(self._make_transfer_form_label("Files Script", "FTP script used to pull the live data files from the active site."), self._transfer_files_script_edit)
        general_form.addRow(self._make_transfer_form_label("Cycle Script", "Optional FTP script used for the cycle/status file transfer step."), self._transfer_cycle_script_edit)
        general_form.addRow(self._make_transfer_form_label("Active Site", "Read-only. This follows the existing site switcher in the top banner and controls which site profile the transfer engine uses."), self._transfer_active_site_label)
        gp.addLayout(general_form)
        general_button_row = QHBoxLayout()
        self._transfer_start_button = QPushButton("Start Transfer")
        self._transfer_start_button.clicked.connect(self._start_transfer_engine)
        self._transfer_stop_button = QPushButton("Stop Transfer")
        self._transfer_stop_button.clicked.connect(self._stop_transfer_engine)
        self._transfer_run_once_button = QPushButton("Run One Cycle Now")
        self._transfer_run_once_button.clicked.connect(self._run_transfer_once)
        self._transfer_save_general_button = QPushButton("Save General Settings")
        self._transfer_save_general_button.clicked.connect(self._save_transfer_general_settings)
        general_button_row.addWidget(self._transfer_start_button)
        general_button_row.addWidget(self._transfer_stop_button)
        general_button_row.addWidget(self._transfer_run_once_button)
        general_button_row.addStretch()
        general_button_row.addWidget(self._transfer_save_general_button)
        gp.addLayout(general_button_row)
        self._transfer_status_label = QLabel("Status: Stopped")
        self._transfer_status_label.setWordWrap(True)
        self._transfer_last_success_label = QLabel("Last Success: --")
        self._transfer_last_success_label.setWordWrap(True)
        self._transfer_last_error_label = QLabel("Last Error: --")
        self._transfer_last_error_label.setWordWrap(True)
        gp.addWidget(self._transfer_status_label)
        gp.addWidget(self._transfer_last_success_label)
        gp.addWidget(self._transfer_last_error_label)
        general_layout.addWidget(general_panel)

        log_panel = QFrame()
        log_panel.setObjectName("panel")
        lp = QVBoxLayout(log_panel)
        lp.setContentsMargins(12, 12, 12, 12)
        lp.setSpacing(10)
        log_title = QLabel("Transfer Activity")
        log_title.setObjectName("sectionTitle")
        lp.addWidget(log_title)
        self._transfer_log_view = QPlainTextEdit()
        self._transfer_log_view.setReadOnly(True)
        self._transfer_log_view.setMaximumBlockCount(500)
        lp.addWidget(self._transfer_log_view, 1)
        general_layout.addWidget(log_panel, 1)
        self._transfer_subtabs.addTab(general_tab, "General")

        sites_tab = QWidget()
        sites_layout = QVBoxLayout(sites_tab)
        sites_layout.setContentsMargins(0, 0, 0, 0)
        sites_layout.setSpacing(12)
        profiles_panel = QFrame()
        profiles_panel.setObjectName("panel")
        pp = QVBoxLayout(profiles_panel)
        pp.setContentsMargins(12, 12, 12, 12)
        pp.setSpacing(10)
        profiles_title = QLabel("Site Profiles")
        profiles_title.setObjectName("sectionTitle")
        pp.addWidget(profiles_title)
        profiles_help = QLabel("These are saved connection settings for each site. They are configuration only. The active site is still chosen with the top banner switcher.")
        profiles_help.setWordWrap(True)
        pp.addWidget(profiles_help)
        profiles_grid = QGridLayout()
        profiles_grid.setColumnStretch(0, 1)
        profiles_grid.setColumnStretch(1, 1)

        self._transfer_site1_host_edit = QLineEdit()
        self._transfer_site1_user_edit = QLineEdit()
        self._transfer_site1_root_edit = QLineEdit()
        self._transfer_site2_host_edit = QLineEdit()
        self._transfer_site2_user_edit = QLineEdit()
        self._transfer_site2_root_edit = QLineEdit()

        site1_panel = QFrame()
        site1_panel.setObjectName("detailCard")
        s1 = QFormLayout(site1_panel)
        s1.addRow(self._make_transfer_form_label("Host / IP", "Customer host or IP address for Site 1. This is where the transfer connects when Site 1 is active."), self._transfer_site1_host_edit)
        s1.addRow(self._make_transfer_form_label("Username", "SSH/SFTP username for Site 1."), self._transfer_site1_user_edit)
        s1.addRow(self._make_transfer_form_label("Destination Root", "Local base folder for Site 1 normalized files. Destination Subfolder, if used, is created under this root."), self._transfer_site1_root_edit)
        site2_panel = QFrame()
        site2_panel.setObjectName("detailCard")
        s2 = QFormLayout(site2_panel)
        s2.addRow(self._make_transfer_form_label("Host / IP", "Customer host or IP address for Site 2. This is where the transfer connects when Site 2 is active."), self._transfer_site2_host_edit)
        s2.addRow(self._make_transfer_form_label("Username", "SSH/SFTP username for Site 2."), self._transfer_site2_user_edit)
        s2.addRow(self._make_transfer_form_label("Destination Root", "Local base folder for Site 2 normalized files. Destination Subfolder, if used, is created under this root."), self._transfer_site2_root_edit)
        profiles_grid.addWidget(self._make_transfer_form_label("Site 1", "Saved transfer settings for Site 1. The top banner switcher decides when these settings are active."), 0, 0)
        profiles_grid.addWidget(self._make_transfer_form_label("Site 2", "Saved transfer settings for Site 2. The top banner switcher decides when these settings are active."), 0, 1)
        profiles_grid.addWidget(site1_panel, 1, 0)
        profiles_grid.addWidget(site2_panel, 1, 1)
        pp.addLayout(profiles_grid)
        profile_buttons = QHBoxLayout()
        self._transfer_save_sites_button = QPushButton("Save Site Profiles")
        self._transfer_save_sites_button.clicked.connect(self._save_transfer_site_profiles)
        self._transfer_test_connection_button = QPushButton("Test Active Site Settings")
        self._transfer_test_connection_button.clicked.connect(self._test_transfer_configuration)
        profile_buttons.addWidget(self._transfer_save_sites_button)
        profile_buttons.addWidget(self._transfer_test_connection_button)
        profile_buttons.addStretch()
        pp.addLayout(profile_buttons)
        self._transfer_site_status_label = QLabel("These settings are saved even before live transfer is tested.")
        self._transfer_site_status_label.setWordWrap(True)
        pp.addWidget(self._transfer_site_status_label)
        sites_layout.addWidget(profiles_panel)
        self._transfer_subtabs.addTab(sites_tab, "Site Profiles")

        mappings_tab = QWidget()
        mappings_layout = QVBoxLayout(mappings_tab)
        mappings_layout.setContentsMargins(0, 0, 0, 0)
        mappings_layout.setSpacing(12)
        mappings_panel = QFrame()
        mappings_panel.setObjectName("panel")
        mp = QVBoxLayout(mappings_panel)
        mp.setContentsMargins(12, 12, 12, 12)
        mp.setSpacing(10)
        mappings_title = QLabel("Mappings")
        mappings_title.setObjectName("sectionTitle")
        mp.addWidget(mappings_title)
        mappings_help = QLabel("Mappings convert rotating remote filenames into stable local filenames for alerts. Destination Subfolder is optional; leave it blank to save directly in the site's destination root.")
        mappings_help.setWordWrap(True)
        mp.addWidget(mappings_help)
        self._transfer_mappings_table = QTableWidget(0, 7)
        self._transfer_mappings_table.setHorizontalHeaderLabels(["Enabled", "Mapping Name", "Remote Pattern", "Stable Local Filename", "Destination Subfolder", "Site Scope", "Keep Raw Copy"])
        self._transfer_mappings_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._transfer_mappings_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._transfer_mappings_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._transfer_mappings_table.verticalHeader().setVisible(False)
        self._transfer_mappings_table.setAlternatingRowColors(True)
        self._transfer_mappings_table.setShowGrid(False)
        self._transfer_mappings_table.setWordWrap(False)
        self._transfer_mappings_table.horizontalHeader().setStretchLastSection(False)
        self._configure_transfer_mapping_header_tooltips()
        mp.addWidget(self._transfer_mappings_table, 1)
        mapping_buttons = QHBoxLayout()
        self._transfer_add_mapping_button = QPushButton("Add Mapping")
        self._transfer_add_mapping_button.clicked.connect(self._add_transfer_mapping_row)
        self._transfer_remove_mapping_button = QPushButton("Remove Selected Mapping")
        self._transfer_remove_mapping_button.clicked.connect(self._remove_selected_transfer_mapping_row)
        self._transfer_load_defaults_button = QPushButton("Load Batch Defaults")
        self._transfer_load_defaults_button.clicked.connect(self._load_default_transfer_mappings)
        self._transfer_enable_all_button = QPushButton("Enable All")
        self._transfer_enable_all_button.clicked.connect(lambda: self._set_all_transfer_mappings_enabled(True))
        self._transfer_disable_all_button = QPushButton("Disable All")
        self._transfer_disable_all_button.clicked.connect(lambda: self._set_all_transfer_mappings_enabled(False))
        self._transfer_save_mappings_button = QPushButton("Save Mappings")
        self._transfer_save_mappings_button.clicked.connect(self._save_transfer_mappings)
        mapping_buttons.addWidget(self._transfer_add_mapping_button)
        mapping_buttons.addWidget(self._transfer_remove_mapping_button)
        mapping_buttons.addWidget(self._transfer_load_defaults_button)
        mapping_buttons.addWidget(self._transfer_enable_all_button)
        mapping_buttons.addWidget(self._transfer_disable_all_button)
        mapping_buttons.addStretch()
        mapping_buttons.addWidget(self._transfer_save_mappings_button)
        mp.addLayout(mapping_buttons)
        self._transfer_mapping_results_label = QLabel("")
        self._transfer_mapping_results_label.setWordWrap(True)
        mp.addWidget(self._transfer_mapping_results_label)
        mappings_layout.addWidget(mappings_panel, 1)
        self._transfer_subtabs.addTab(mappings_tab, "Mappings")

    def _default_transfer_general_config(self) -> dict:
        return {
            "enabled": True,
            "auto_start": True,
            "interval_seconds": 18,
            "putty_path": "C:/Program Files/PuTTY/psftp.exe",
            "key_path": "C:/Keys/pds.ppk",
            "script_root": "C:/Console/Scripts",
            "files_script_name": "TransferDataFiles.ftp",
            "cycle_script_name": "TransferDataCycleToServer.ftp",
            "site_profiles": {
                "site1": {"host": "10.12.70.172", "username": "pds", "local_root": self._settings_repo.get_setting("live_data_root_site1", "")},
                "site2": {"host": "10.13.70.172", "username": "pds", "local_root": self._settings_repo.get_setting("live_data_root_site2", "")},
            },
            "mappings": list(DEFAULT_MAPPINGS),
        }

    def _get_transfer_config(self) -> dict:
        raw = self._settings_repo.get_setting("transfer_config_json", "")
        config = self._default_transfer_general_config()
        if raw:
            try:
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    config.update({k: v for k, v in loaded.items() if k != "site_profiles" and k != "mappings"})
                    if isinstance(loaded.get("site_profiles"), dict):
                        config["site_profiles"].update(loaded["site_profiles"])
                    if isinstance(loaded.get("mappings"), list) and loaded["mappings"]:
                        config["mappings"] = loaded["mappings"]
            except Exception:
                logger.exception("Failed to parse transfer_config_json")
        return config

    def _save_transfer_config(self, config: dict) -> None:
        self._settings_repo.set_setting("transfer_config_json", json.dumps(config), self._user.user_id)

    def _load_transfer_data(self) -> None:
        if not hasattr(self, "_transfer_enabled_checkbox"):
            return
        config = self._get_transfer_config()
        self._transfer_enabled_checkbox.blockSignals(True)
        self._transfer_autostart_checkbox.blockSignals(True)
        self._transfer_interval_spin.blockSignals(True)
        self._transfer_enabled_checkbox.setChecked(bool(config.get("enabled", True)))
        self._transfer_autostart_checkbox.setChecked(bool(config.get("auto_start", True)))
        self._transfer_interval_spin.setValue(int(config.get("interval_seconds", 18) or 18))
        self._transfer_enabled_checkbox.blockSignals(False)
        self._transfer_autostart_checkbox.blockSignals(False)
        self._transfer_interval_spin.blockSignals(False)
        self._transfer_putty_path_edit.setText(str(config.get("putty_path", "")))
        self._transfer_key_path_edit.setText(str(config.get("key_path", "")))
        self._transfer_script_root_edit.setText(str(config.get("script_root", "")))
        self._transfer_files_script_edit.setText(str(config.get("files_script_name", "")))
        self._transfer_cycle_script_edit.setText(str(config.get("cycle_script_name", "")))
        active_site_id = self._settings_repo.get_setting("active_site_id", "1")
        self._transfer_active_site_label.setText(f"Site {active_site_id} (from top banner)")
        site_profiles = config.get("site_profiles", {})
        site1 = dict(site_profiles.get("site1") or {})
        site2 = dict(site_profiles.get("site2") or {})
        self._transfer_site1_host_edit.setText(str(site1.get("host", "")))
        self._transfer_site1_user_edit.setText(str(site1.get("username", "pds")))
        self._transfer_site1_root_edit.setText(str(site1.get("local_root", self._settings_repo.get_setting("live_data_root_site1", ""))))
        self._transfer_site2_host_edit.setText(str(site2.get("host", "")))
        self._transfer_site2_user_edit.setText(str(site2.get("username", "pds")))
        self._transfer_site2_root_edit.setText(str(site2.get("local_root", self._settings_repo.get_setting("live_data_root_site2", ""))))
        self._populate_transfer_mappings_table(config.get("mappings", []))
        self._autosize_transfer_mapping_columns()
        self._refresh_transfer_status_ui()

    def _populate_transfer_mappings_table(self, mappings: list[dict]) -> None:
        selection = self._remember_transfer_mapping_selection()
        self._transfer_mappings_table.setRowCount(0)
        for mapping in mappings:
            self._add_transfer_mapping_row(mapping)
        self._autosize_transfer_mapping_columns()
        self._restore_transfer_mapping_selection(selection)

    def _add_transfer_mapping_row(self, mapping: dict | None = None) -> None:
        mapping = dict(mapping or {
            "enabled": True, "name": "", "remote_pattern": "", "stable_local_name": "", "destination_subfolder": "", "site_scope": "Both", "keep_raw_copy": False
        })
        row = self._transfer_mappings_table.rowCount()
        self._transfer_mappings_table.insertRow(row)
        enabled_item = QTableWidgetItem("")
        enabled_item.setFlags(enabled_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
        enabled_item.setCheckState(Qt.CheckState.Checked if mapping.get("enabled", True) else Qt.CheckState.Unchecked)
        self._transfer_mappings_table.setItem(row, 0, enabled_item)
        for col, key in enumerate(["name", "remote_pattern", "stable_local_name", "destination_subfolder", "site_scope"], start=1):
            self._transfer_mappings_table.setItem(row, col, QTableWidgetItem(str(mapping.get(key, ""))))
        keep_raw_item = QTableWidgetItem("")
        keep_raw_item.setFlags(keep_raw_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
        keep_raw_item.setCheckState(Qt.CheckState.Checked if mapping.get("keep_raw_copy", False) else Qt.CheckState.Unchecked)
        self._transfer_mappings_table.setItem(row, 6, keep_raw_item)
        self._autosize_transfer_mapping_columns()

    def _remove_selected_transfer_mapping_row(self) -> None:
        row = self._transfer_mappings_table.currentRow()
        if row >= 0:
            self._transfer_mappings_table.removeRow(row)
            self._autosize_transfer_mapping_columns()

    def _collect_transfer_mappings_from_table(self) -> list[dict]:
        mappings: list[dict] = []
        for row in range(self._transfer_mappings_table.rowCount()):
            enabled_item = self._transfer_mappings_table.item(row, 0)
            keep_raw_item = self._transfer_mappings_table.item(row, 6)
            mapping = {
                "enabled": enabled_item.checkState() == Qt.CheckState.Checked if enabled_item else True,
                "name": (self._transfer_mappings_table.item(row, 1).text().strip() if self._transfer_mappings_table.item(row, 1) else ""),
                "remote_pattern": (self._transfer_mappings_table.item(row, 2).text().strip() if self._transfer_mappings_table.item(row, 2) else ""),
                "stable_local_name": (self._transfer_mappings_table.item(row, 3).text().strip() if self._transfer_mappings_table.item(row, 3) else ""),
                "destination_subfolder": (self._transfer_mappings_table.item(row, 4).text().strip() if self._transfer_mappings_table.item(row, 4) else ""),
                "site_scope": (self._transfer_mappings_table.item(row, 5).text().strip() if self._transfer_mappings_table.item(row, 5) else "Both"),
                "keep_raw_copy": keep_raw_item.checkState() == Qt.CheckState.Checked if keep_raw_item else False,
                "use_newest": True,
            }
            if mapping["name"] or mapping["remote_pattern"] or mapping["stable_local_name"]:
                mappings.append(mapping)
        return mappings

    def _save_transfer_general_settings(self) -> None:
        config = self._get_transfer_config()
        config["enabled"] = self._transfer_enabled_checkbox.isChecked()
        config["auto_start"] = self._transfer_autostart_checkbox.isChecked()
        config["interval_seconds"] = int(self._transfer_interval_spin.value())
        config["putty_path"] = self._transfer_putty_path_edit.text().strip()
        config["key_path"] = self._transfer_key_path_edit.text().strip()
        config["script_root"] = self._transfer_script_root_edit.text().strip()
        config["files_script_name"] = self._transfer_files_script_edit.text().strip()
        config["cycle_script_name"] = self._transfer_cycle_script_edit.text().strip()
        self._save_transfer_config(config)
        self._set_status_banner("Transfer general settings saved.")
        self._load_transfer_data()

    def _save_transfer_site_profiles(self) -> None:
        config = self._get_transfer_config()
        config.setdefault("site_profiles", {})
        config["site_profiles"]["site1"] = {
            "host": self._transfer_site1_host_edit.text().strip(),
            "username": self._transfer_site1_user_edit.text().strip() or "pds",
            "local_root": self._transfer_site1_root_edit.text().strip(),
        }
        config["site_profiles"]["site2"] = {
            "host": self._transfer_site2_host_edit.text().strip(),
            "username": self._transfer_site2_user_edit.text().strip() or "pds",
            "local_root": self._transfer_site2_root_edit.text().strip(),
        }
        self._save_transfer_config(config)
        self._transfer_site_status_label.setText("Site transfer profiles saved.")
        self._set_status_banner("Transfer site profiles saved.")
        self._load_transfer_data()

    def _save_transfer_mappings(self) -> None:
        config = self._get_transfer_config()
        config["mappings"] = self._collect_transfer_mappings_from_table()
        self._save_transfer_config(config)
        self._transfer_mapping_results_label.setText(f"Saved {len(config['mappings'])} transfer mapping(s).")
        self._set_status_banner("Transfer mappings saved.")
        self._load_transfer_data()

    def _load_default_transfer_mappings(self) -> None:
        self._populate_transfer_mappings_table(DEFAULT_MAPPINGS)
        self._transfer_mapping_results_label.setText("Loaded transfer mappings based on the current batch script rename rules. Save to keep them.")
        self._autosize_transfer_mapping_columns()

    def _refresh_transfer_status_ui(self) -> None:
        if not hasattr(self, "_transfer_status_label"):
            return
        snapshot = self._transfer_engine.snapshot()
        self._transfer_status_label.setText(
            f"Status: {snapshot.get('engine_state', 'Stopped')} | Active Site: Site {self._settings_repo.get_setting('active_site_id', '1')} | Last Cycle: {snapshot.get('last_cycle_utc') or '--'}\n{snapshot.get('last_summary') or ''}"
        )
        self._transfer_last_success_label.setText(f"Last Success: {snapshot.get('last_success_utc') or '--'}")
        self._transfer_last_error_label.setText(f"Last Error: {snapshot.get('last_error') or '--'}")
        self._transfer_log_view.setPlainText("\n".join(snapshot.get("recent_log_lines", [])))
        self._transfer_log_view.verticalScrollBar().setValue(self._transfer_log_view.verticalScrollBar().maximum())
        try:
            mapping_results = json.loads(snapshot.get("last_mapping_results_json") or "[]")
        except Exception:
            mapping_results = []
        if mapping_results:
            last_lines = []
            for item in mapping_results[:10]:
                if item.get("status") == "success":
                    last_lines.append(f"{item.get('name')}: {item.get('matched_file')} -> {item.get('destination')}")
                else:
                    last_lines.append(f"{item.get('name')}: {item.get('message')}")
            self._transfer_mapping_results_label.setText("Last mapping results:\n" + "\n".join(last_lines))

    def _start_transfer_engine(self) -> None:
        self._save_transfer_general_settings()
        self._save_transfer_site_profiles()
        self._save_transfer_mappings()
        config = self._get_transfer_config()
        if not config.get("enabled", True):
            self._set_status_banner("Transfer engine is disabled in settings.")
            return
        active_site_id = self._settings_repo.get_setting("active_site_id", "1")
        started = self._transfer_engine.start(config, active_site_id)
        if not started:
            self._set_status_banner("Transfer engine is already running.")
        self._refresh_transfer_status_ui()

    def _stop_transfer_engine(self) -> None:
        self._transfer_engine.stop()
        self._refresh_transfer_status_ui()

    def _run_transfer_once(self) -> None:
        self._save_transfer_general_settings()
        self._save_transfer_site_profiles()
        self._save_transfer_mappings()
        active_site_id = self._settings_repo.get_setting("active_site_id", "1")
        try:
            result = self._transfer_engine.run_once(self._get_transfer_config(), active_site_id)
            self._transfer_mapping_results_label.setText(result.get("summary", "Run-once cycle completed."))
            self._set_status_banner(result.get("summary", "Run-once cycle completed."))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Transfer run-once failed")
            self._set_status_banner(f"Transfer cycle error: {exc}")
        self._refresh_transfer_status_ui()

    def _test_transfer_configuration(self) -> None:
        config = self._get_transfer_config()
        active_site_id = self._settings_repo.get_setting("active_site_id", "1")
        site_key = "site1" if active_site_id == "1" else "site2"
        site = dict(config.get("site_profiles", {}).get(site_key) or {})
        checks = []
        for label, value in [("psftp.exe", config.get("putty_path", "")), ("PPK key", config.get("key_path", "")), ("Script root", config.get("script_root", "")), ("Host", site.get("host", "")), ("Destination root", site.get("local_root", ""))]:
            checks.append(f"{label}: {'OK' if str(value).strip() else 'Missing'}")
        self._transfer_site_status_label.setText(f"Active site is Site {active_site_id}.\n" + "\n".join(checks))

    def _maybe_auto_start_transfer(self) -> None:
        config = self._get_transfer_config()
        if not config.get("enabled", True):
            return
        if not config.get("auto_start", True):
            return
        if self._settings_repo.get_setting("active_mode", "Test") != "Live":
            return
        self._start_transfer_engine()

    def _build_settings_tab(self) -> None:
        self._settings_tab = QWidget()
        self._main_tabs.addTab(self._settings_tab, "Settings")
        root = QVBoxLayout(self._settings_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        info = QLabel("Manage application appearance, runtime data locations, and core backup/configuration actions.")
        info.setWordWrap(True)
        root.addWidget(info)

        top = QGridLayout()
        top.setColumnStretch(0, 3)
        top.setColumnStretch(1, 2)
        top.setHorizontalSpacing(12)
        root.addLayout(top, 1)

        appearance = QFrame()
        appearance.setObjectName("panel")
        a_layout = QVBoxLayout(appearance)
        a_layout.setContentsMargins(12, 12, 12, 12)
        a_layout.setSpacing(10)
        a_title = QLabel("Appearance & Runtime")
        a_title.setObjectName("sectionTitle")
        a_layout.addWidget(a_title)
        form = QFormLayout()
        self._theme_combo = QComboBox()
        for row in self._settings_repo.list_themes():
            self._theme_combo.addItem(str(row["theme_name"]), str(row["theme_name"]))
        self._theme_combo.currentIndexChanged.connect(self._apply_selected_theme)
        self._zoom_combo = QComboBox()
        for pct in [90, 100, 110, 125, 150]:
            self._zoom_combo.addItem(f"{pct}%", str(pct))
        self._zoom_combo.currentIndexChanged.connect(self._apply_zoom_combo)
        self._density_combo = QComboBox()
        self._density_combo.addItem("Compact", "Compact")
        self._density_combo.addItem("Standard", "Standard")
        self._density_combo.currentIndexChanged.connect(self._apply_density_setting)
        self._mode_combo = QComboBox()
        self._mode_combo.addItem("Test", "Test")
        self._mode_combo.addItem("Live", "Live")
        self._mode_combo.currentIndexChanged.connect(self._apply_mode_setting)
        self._site_combo = QComboBox()
        self._site_combo.addItem("Site 1", "1")
        self._site_combo.addItem("Site 2", "2")
        self._site_combo.currentIndexChanged.connect(self._apply_active_site_setting)
        self._test_root_edit = QLabel(self._settings_repo.get_setting("test_data_root_path", "TestData"))
        self._live_site1_root_edit = QLabel(self._settings_repo.get_setting("live_data_root_site1", ""))
        self._live_site2_root_edit = QLabel(self._settings_repo.get_setting("live_data_root_site2", ""))
        self._show_only_issues_setting = QCheckBox("Hide healthy checks on the dashboard")
        self._show_only_issues_setting.toggled.connect(self._apply_show_only_issues_setting)
        form.addRow("Theme", self._theme_combo)
        form.addRow("Zoom", self._zoom_combo)
        form.addRow("Dashboard Density", self._density_combo)
        form.addRow("Runtime Mode", self._mode_combo)
        form.addRow("Active Site", self._site_combo)
        form.addRow("Test Data Root", self._test_root_edit)
        form.addRow("Live Root - Site 1", self._live_site1_root_edit)
        form.addRow("Live Root - Site 2", self._live_site2_root_edit)
        form.addRow("Dashboard Filter", self._show_only_issues_setting)
        a_layout.addLayout(form)
        self._browse_test_root_button = QPushButton("Set Test Data Root Folder")
        self._browse_test_root_button.clicked.connect(self._choose_test_root_folder)
        self._browse_live_site1_root_button = QPushButton("Set Live Root Folder - Site 1")
        self._browse_live_site1_root_button.clicked.connect(lambda: self._choose_live_root_folder(1))
        self._browse_live_site2_root_button = QPushButton("Set Live Root Folder - Site 2")
        self._browse_live_site2_root_button.clicked.connect(lambda: self._choose_live_root_folder(2))
        a_layout.addWidget(self._browse_test_root_button)
        a_layout.addWidget(self._browse_live_site1_root_button)
        a_layout.addWidget(self._browse_live_site2_root_button)
        self._data_path_label = QLabel("")
        self._data_path_label.setWordWrap(True)
        a_layout.addWidget(self._data_path_label)
        self._set_data_path_button = QPushButton("Set Persistent Data Folder")
        self._set_data_path_button.clicked.connect(self._choose_persistent_data_folder)
        a_layout.addWidget(self._set_data_path_button)
        self._retention_combo = QComboBox()
        for label, value in [("30 days", "30"), ("90 days", "90"), ("180 days", "180"), ("365 days", "365"), ("Keep indefinitely", "keep")]:
            self._retention_combo.addItem(label, value)
        self._retention_combo.currentIndexChanged.connect(self._apply_retention_setting)
        self._incident_lookback_combo = QComboBox()
        for label, value in [("15 minutes", "15"), ("30 minutes", "30"), ("60 minutes", "60"), ("120 minutes", "120")]:
            self._incident_lookback_combo.addItem(label, value)
        self._incident_lookback_combo.currentIndexChanged.connect(self._apply_incident_lookback_setting)
        self._retention_status = QLabel("")
        self._retention_status.setWordWrap(True)
        form.addRow("Data Retention", self._retention_combo)
        form.addRow("Incident Lookback", self._incident_lookback_combo)
        a_layout.addWidget(self._retention_status)
        self._compact_value = QLabel("")
        self._compact_value.setWordWrap(True)
        a_layout.addWidget(self._compact_value)
        a_layout.addStretch()

        backup = QFrame()
        backup.setObjectName("panel")
        b_layout = QVBoxLayout(backup)
        b_layout.setContentsMargins(12, 12, 12, 12)
        b_layout.setSpacing(10)
        b_title = QLabel("Backup & Export")
        b_title.setObjectName("sectionTitle")
        b_layout.addWidget(b_title)
        self._backup_status = QLabel("")
        self._backup_status.setWordWrap(True)
        b_layout.addWidget(self._backup_status)
        self._backup_button = QPushButton("Backup Database Now")
        self._backup_button.clicked.connect(self._backup_database)
        self._export_config_button = QPushButton("Export Configuration JSON")
        self._export_config_button.clicked.connect(self._export_configuration_json)
        self._import_config_button = QPushButton("Import Configuration JSON")
        self._import_config_button.clicked.connect(self._import_configuration_json)
        self._manage_notifications_button = QPushButton("Manage Notification Rules")
        self._manage_notifications_button.clicked.connect(self._manage_notification_rules)
        self._manage_maintenance_button = QPushButton("Manage Maintenance Windows")
        self._manage_maintenance_button.clicked.connect(self._manage_maintenance_windows)
        self._open_notifications_log_button = QPushButton("Open Notifications Log")
        self._open_notifications_log_button.clicked.connect(self._open_notifications_log)
        self._open_logs_button = QPushButton("Open Logs Folder")
        self._open_logs_button.clicked.connect(self._open_logs_folder)
        b_layout.addWidget(self._backup_button)
        b_layout.addWidget(self._export_config_button)
        b_layout.addWidget(self._import_config_button)
        b_layout.addWidget(self._manage_notifications_button)
        b_layout.addWidget(self._manage_maintenance_button)
        b_layout.addWidget(self._open_notifications_log_button)
        b_layout.addWidget(self._open_logs_button)
        b_layout.addStretch()

        top.addWidget(appearance, 0, 0)
        top.addWidget(backup, 0, 1)

        if not self._can_manage_config():
            for widget in [self._backup_button, self._export_config_button, self._import_config_button, self._manage_notifications_button, self._manage_maintenance_button]:
                widget.setEnabled(False)


    def _build_users_tab(self) -> None:
        self._users_tab = QWidget()
        self._main_tabs.addTab(self._users_tab, "Users")
        root = QVBoxLayout(self._users_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        info = QLabel("Manage application users, passwords, and roles. Only SystemAdmin users can make changes here.")
        info.setWordWrap(True)
        root.addWidget(info)

        panel = QFrame()
        panel.setObjectName("panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        title = QLabel("User Management")
        title.setObjectName("sectionTitle")
        layout.addWidget(title)

        button_row = QHBoxLayout()
        self._new_user_button = QPushButton("Add User")
        self._new_user_button.clicked.connect(self._new_user)
        self._edit_user_button = QPushButton("Edit User")
        self._edit_user_button.clicked.connect(self._edit_selected_user)
        self._reset_password_button = QPushButton("Reset Password")
        self._reset_password_button.clicked.connect(self._reset_selected_user_password)
        self._toggle_user_button = QPushButton("Enable / Disable")
        self._toggle_user_button.clicked.connect(self._toggle_selected_user)
        self._refresh_users_button = QPushButton("Refresh")
        self._refresh_users_button.clicked.connect(self._load_users_data)
        button_row.addWidget(self._new_user_button)
        button_row.addWidget(self._edit_user_button)
        button_row.addWidget(self._reset_password_button)
        button_row.addWidget(self._toggle_user_button)
        button_row.addStretch()
        button_row.addWidget(self._refresh_users_button)
        layout.addLayout(button_row)

        self._users_table = QTableWidget(0, 6)
        self._users_table.setHorizontalHeaderLabels(["Username", "Display Name", "Role", "Active", "Created", "Last Login"])
        self._users_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._users_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._users_table.itemSelectionChanged.connect(self._on_user_selection_changed)
        self._users_table.setAlternatingRowColors(True)
        self._users_table.verticalHeader().setVisible(False)
        self._users_table.setShowGrid(False)
        self._users_table.setWordWrap(False)
        layout.addWidget(self._users_table)

        self._users_status = QLabel("")
        self._users_status.setWordWrap(True)
        layout.addWidget(self._users_status)
        root.addWidget(panel, 1)

        if self._user.role_name != "SystemAdmin":
            for widget in [self._new_user_button, self._edit_user_button, self._reset_password_button, self._toggle_user_button, self._refresh_users_button]:
                widget.setEnabled(False)

    def _run_cycle_and_refresh(self) -> None:
        right_scrollbar = self._right_panel_scroll.verticalScrollBar() if hasattr(self, "_right_panel_scroll") else None
        right_scroll_value = right_scrollbar.value() if right_scrollbar is not None else 0
        incident_scrollbar = self._incident_summary.verticalScrollBar() if hasattr(self, "_incident_summary") else None
        incident_scroll_value = incident_scrollbar.value() if incident_scrollbar is not None else 0
        try:
            self._run_retention_cleanup(force=False)
            self._monitoring_engine.run_cycle()
            self._load_data()

            current_index = self._main_tabs.currentIndex()
            current_text = self._main_tabs.tabText(current_index) if current_index >= 0 else ""
            if current_text == "Audit":
                self._load_audit_data()
            elif current_text == "Settings":
                self._load_settings_data()
            elif current_text == "Transfer":
                self._load_transfer_data()

            self._set_status_banner("")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Monitoring cycle refresh failed")
            self._set_status_banner(f"Monitoring cycle error: {exc}")
            QMessageBox.warning(self, "Monitoring cycle", f"A monitoring cycle error occurred:\n{exc}")
        finally:
            if right_scrollbar is not None:
                QTimer.singleShot(0, lambda sb=right_scrollbar, value=right_scroll_value: sb.setValue(min(value, sb.maximum())))
            if incident_scrollbar is not None:
                def _restore_incident_scroll(sb=incident_scrollbar, value=incident_scroll_value):
                    sb.setValue(min(value, sb.maximum()))
                QTimer.singleShot(0, _restore_incident_scroll)
                QTimer.singleShot(50, _restore_incident_scroll)


    def _set_status_banner(self, message: str) -> None:
        self._status_banner.setVisible(bool(message))
        self._status_banner.setText(message)

    def _is_shared_data_mode(self) -> bool:
        data_root = str(self._data_root_path())
        return data_root.startswith("\\") or data_root.startswith("//")

    def _refresh_shared_mode_indicator(self) -> None:
        shared = self._is_shared_data_mode()
        label = "SHARED" if shared else "LOCAL"
        background = "#1D4ED8" if shared else "#374151"
        border = "#93C5FD" if shared else "#9CA3AF"
        self._shared_state_label.setText(label)
        self._shared_state_label.setStyleSheet(
            f"background-color: {background}; color: white; border: 1px solid {border}; border-radius: 12px; padding: 6px 12px; font-weight: 800;"
        )

    def _refresh_mode_site_indicator(self, mode_name: str | None = None, site_name: str | None = None) -> None:
        mode = str(mode_name or self._settings_repo.get_setting("active_mode", "Test")).upper()
        active_site_id = str(self._settings_repo.get_setting("active_site_id", "1"))
        if mode == "LIVE":
            background = "#166534"
            border = "#22C55E"
        else:
            background = "#92400E"
            border = "#FBBF24"
        self._mode_site_label.setText(mode)
        self._mode_site_label.setStyleSheet(
            f"background-color: {background}; color: white; border: 1px solid {border}; border-radius: 12px; padding: 6px 12px; font-weight: 800;"
        )
        if hasattr(self, "_site1_button"):
            self._site1_button.blockSignals(True)
            self._site2_button.blockSignals(True)
            self._site1_button.setChecked(active_site_id == "1")
            self._site2_button.setChecked(active_site_id == "2")
            self._site1_button.blockSignals(False)
            self._site2_button.blockSignals(False)
            can_switch = self._can_switch_site()
            self._site_segment.setVisible(can_switch)
            self._site1_button.setEnabled(can_switch)
            self._site2_button.setEnabled(can_switch)

    def _set_details_panel_visible(self, visible: bool) -> None:
        self._details_panel_expanded = visible
        self._settings_repo.set_setting("dashboard_details_visible", "1" if visible else "0", self._user.user_id)
        pane_has_active_content = bool(self._detail_action_active and self._selected_check_id is not None)
        effective_visible = bool(visible and pane_has_active_content)

        self._right_panel_scroll.setVisible(effective_visible)
        self._right_panel.setVisible(effective_visible)
        self._right_panel_scroll.setMinimumWidth(0)
        self._right_panel.setMinimumWidth(0)
        if effective_visible:
            self._right_panel_scroll.setMaximumWidth(16777215)
            self._right_panel.setMaximumWidth(16777215)
            self._right_panel_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        else:
            self._right_panel_scroll.setMaximumWidth(0)
            self._right_panel.setMaximumWidth(0)
            self._right_panel_scroll.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)

        self._dashboard_content.setColumnStretch(0, 5 if not effective_visible else 3)
        self._dashboard_content.setColumnStretch(1, 0 if not effective_visible else 2)
        self._collapse_details_button.setVisible(effective_visible)

    def _collapse_details_panel(self, initial: bool = False) -> None:
        self._detail_action_active = False
        self._selected_check_id = None
        self._detail_mode = "details"
        self._set_details_panel_visible(False)
        if hasattr(self, "_selected_header"):
            self._selected_header.setVisible(False)
        if hasattr(self, "_detail_mode_bar"):
            self._detail_mode_bar.setVisible(False)
        if hasattr(self, "_status_card"):
            self._status_card.setVisible(False)
        if hasattr(self, "_info_card"):
            self._info_card.setVisible(False)
        if hasattr(self, "_detail_tabs"):
            self._detail_tabs.setVisible(False)
        if hasattr(self, "_recent_activity_title"):
            self._recent_activity_title.setVisible(False)
        if hasattr(self, "_load_selected_check_details"):
            self._load_selected_check_details()
        if not initial:
            self._apply_tile_focus_state()
            self._update_incident_action_state()

    def _expand_details_panel(self) -> None:
        self._set_details_panel_visible(True)

    def _detail_role(self) -> str:
        role = self._effective_role_name()
        if role in {"SystemAdmin", "ConfigAdmin", "Admin"}:
            return "admin"
        if role == "Operator":
            return "operator"
        return "viewer"

    def _effective_role_name(self) -> str:
        if self._user.role_name in {"ConfigAdmin", "SystemAdmin"} and hasattr(self, "_preview_role_combo"):
            preview = self._preview_role_combo.currentText()
            if preview == "Admin":
                return "ConfigAdmin"
            return preview
        return str(self._user.role_name)

    def _apply_role_preview_ui(self) -> None:
        effective = self._effective_role_name()
        preview_active = self._user.role_name in {"ConfigAdmin", "SystemAdmin"} and self._preview_role_combo.currentText() != "Admin"
        self._preview_banner.setVisible(preview_active)
        if preview_active:
            self._preview_banner.setText(f"Preview Mode: {self._preview_role_combo.currentText()}")
        can_manage = effective in {"ConfigAdmin", "SystemAdmin"}
        can_ack = effective in {"Operator", "ConfigAdmin", "SystemAdmin"}
        can_switch_site = effective in {"Operator", "ConfigAdmin", "SystemAdmin"}
        can_manage_environment = effective in {"ConfigAdmin", "SystemAdmin"}
        can_manage_users = self._user.role_name == "SystemAdmin" and effective in {"ConfigAdmin", "SystemAdmin"}

        for widget in [getattr(self, "_config_tab", None), getattr(self, "_audit_tab", None), getattr(self, "_settings_tab", None)]:
            if widget is None:
                continue
            idx = self._main_tabs.indexOf(widget)
            if idx >= 0:
                self._main_tabs.setTabVisible(idx, can_manage)

        users_idx = self._main_tabs.indexOf(getattr(self, "_users_tab", None))
        if users_idx >= 0:
            self._main_tabs.setTabVisible(users_idx, can_manage_users)

        current_widget = self._main_tabs.currentWidget()
        restricted = {w for w in [getattr(self, "_config_tab", None), getattr(self, "_audit_tab", None), getattr(self, "_settings_tab", None), getattr(self, "_users_tab", None)] if w is not None}
        if current_widget in restricted and not can_manage:
            self._main_tabs.setCurrentWidget(self._dashboard_tab)

        if hasattr(self, "_details_ack_button"):
            self._details_ack_button.setVisible(self._details_ack_button.isVisible() and can_ack)
        if hasattr(self, "_site_segment"):
            self._site_segment.setVisible(can_switch_site)
            self._site1_button.setEnabled(can_switch_site)
            self._site2_button.setEnabled(can_switch_site)
        for widget in [getattr(self, "_mode_combo", None), getattr(self, "_browse_test_root_button", None), getattr(self, "_browse_live_site1_root_button", None), getattr(self, "_browse_live_site2_root_button", None)]:
            if widget is not None:
                widget.setEnabled(can_manage_environment)
        if hasattr(self, "_site_combo"):
            self._site_combo.setEnabled(can_manage_environment)

        self._load_data()
        if hasattr(self, "_users_tab") and self._main_tabs.currentWidget() == getattr(self, "_users_tab", None):
            self._load_users_data()

    def _apply_zoom_setting(self) -> None:
        app = QApplication.instance()
        if app is None:
            return
        theme_name = self._settings_repo.get_setting("current_theme", "Charcoal Blue")
        zoom = int(self._settings_repo.get_setting("ui_zoom_percent", "100"))
        self._theme_service.apply_theme(app, theme_name, zoom)

    def _set_active_site(self, new_value: str) -> None:
        if not self._can_switch_site():
            QMessageBox.warning(self, "Not authorized", "Your role cannot switch sites.")
            self._refresh_mode_site_indicator()
            return
        current = self._settings_repo.get_setting("active_site_id", "1")
        if new_value == current:
            self._refresh_mode_site_indicator()
            return
        self._settings_repo.set_setting("active_site_id", new_value, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ActiveSiteUpdated', 'Settings', 'active_site_id', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Active site changed: Site {current} -> Site {new_value}')
        )
        self._conn.commit()
        self._selected_check_id = None
        self._load_settings_data()
        self._handle_transfer_site_change()
        self._run_cycle_and_refresh()

    def _toggle_site(self) -> None:
        current = self._settings_repo.get_setting("active_site_id", "1")
        self._set_active_site("2" if current == "1" else "1")

    def _on_tab_changed(self, index: int) -> None:
        if index < 0:
            return
        tab_name = self._main_tabs.tabText(index)
        if tab_name == "Historical Incidents":
            self._load_incident_panel(preserve_editor_state=False)
        elif tab_name == "Alert Configuration":
            self._load_configuration_data()
        elif tab_name == "Audit":
            self._load_audit_data()
        elif tab_name == "Settings":
            self._load_settings_data()
        elif tab_name == "Transfer":
            self._load_transfer_data()
        elif tab_name == "Users":
            self._load_users_data()

    def _set_history_range(self, range_key: str) -> None:
        self._history_range = range_key
        if self._details_panel_expanded and self._detail_action_active and self._selected_check_id is not None and self._detail_mode == "graph":
            self._load_history_tab()

    def _load_data(self) -> None:
        summary = self._repo.get_summary()
        density = self._settings_repo.get_setting("dashboard_density", "Compact")
        show_only_issues = self._settings_repo.get_setting("dashboard_show_only_issues", "0") == "1"
        incident_active = self._is_incident_active()
        self._refresh_dashboard_workspace_mode()
        if incident_active:
            show_only_issues = True
        self._issues_only_checkbox.blockSignals(True)
        self._issues_only_checkbox.setChecked(show_only_issues)
        self._issues_only_checkbox.blockSignals(False)
        self._refresh_mode_site_indicator(summary.mode_name, summary.site_name)
        self._refresh_shared_mode_indicator()
        self._set_details_panel_visible(self._details_panel_expanded)
        header_suffix = " • Incident Mode" if incident_active else ""
        self._panel_header.setText(f"Monitoring Dashboard ({density} View){header_suffix}")
        self._refresh_label.setText(f"Last Console Update: {datetime.now().strftime('%H:%M:%S')}")
        if self._summary_frame is not None:
            self._summary_frame.setVisible(False)
        for key, button in self._range_buttons.items():
            button.setProperty("activeRange", key == self._history_range)
            button.style().unpolish(button)
            button.style().polish(button)

        self._tile_widgets = {}
        self._clear_layout(self._dashboard_layout)
        grouped_rows: dict[str, list] = defaultdict(list)
        all_rows = self._repo.get_groups_with_checks()
        if show_only_issues:
            all_rows = [r for r in all_rows if str(r["operational_state"]) != "Healthy"]
        for row in all_rows:
            grouped_rows[str(row["group_label"])].append(row)
        for group_label, rows in grouped_rows.items():
            group_frame = QFrame()
            group_frame.setObjectName("groupPanel")
            group_layout = QVBoxLayout(group_frame)
            group_layout.setContentsMargins(10, 10, 10, 10)
            group_layout.setSpacing(8)
            title = QLabel(group_label)
            title.setObjectName("groupTitle")
            group_layout.addWidget(title)

            grid = QGridLayout()
            grid.setHorizontalSpacing(8)
            grid.setVerticalSpacing(8)
            group_layout.addLayout(grid)
            sorted_rows = sorted(
                rows,
                key=lambda r: (
                    0 if str(r["alert_state"] or "") == "ActiveUnacknowledged" else
                    1 if int(r["is_escalated"] or 0) == 1 else
                    2 if str(r["operational_state"]) == "Stale" else
                    3 if str(r["alert_state"] or "") == "ActiveAcknowledged" else
                    4 if str(r["alert_state"] or "") == "SuppressedMaintenance" else
                    5,
                    str(r["check_label"]),
                ),
            )
            columns = 3 if density == "Compact" else 2
            for idx, row in enumerate(sorted_rows):
                grid.addWidget(self._build_check_tile(row), idx // columns, idx % columns)
            self._dashboard_layout.addWidget(group_frame)

        if not grouped_rows:
            empty = QLabel("No checks match the current dashboard filter. Disable 'Show only issues' to view healthy checks.")
            empty.setWordWrap(True)
            empty.setObjectName("detailCard")
            self._dashboard_layout.addWidget(empty)
        self._dashboard_layout.addStretch()
        if self._details_panel_expanded and self._detail_action_active and self._selected_check_id is not None:
            self._render_monitoring_pane(self._detail_mode, self._selected_check_id, make_visible=False)
            self._refresh_selected_check_popout()
        self._apply_tile_focus_state()
        self._refresh_incident_runtime_ui()
        self._refresh_incident_workspace(self._get_selected_incident_context())
        self._update_incident_action_state()

        self._clear_layout(self._event_list_layout)
        self._event_list_layout.addStretch()

    @staticmethod
    def _row_condition_state(row) -> str:
        return str(row['condition_state'] or ('Unhealthy' if str(row['operational_state'] or '') == 'Unhealthy' else 'Healthy'))

    @staticmethod
    def _row_freshness_state(row) -> str:
        return str(row['freshness_state'] or ('Stale' if str(row['operational_state'] or '') == 'Stale' else 'Fresh'))

    def _format_combined_state(self, row) -> str:
        condition = self._row_condition_state(row)
        freshness = self._row_freshness_state(row)
        if condition == 'Unhealthy' and freshness == 'Stale':
            return 'UNHEALTHY (STALE DATA)'
        if condition == 'Unhealthy':
            return 'UNHEALTHY'
        if freshness == 'Stale':
            return 'STALE'
        return 'HEALTHY'

    def _build_check_tile(self, row) -> QFrame:
        state = str(row["operational_state"] or "Unknown")
        freshness = self._row_freshness_state(row)
        check_id = int(row["check_id"])
        frame = QFrame()
        frame.setProperty("state", state)
        frame.setProperty("selected", check_id == self._selected_check_id)
        frame.setProperty("hoverPersistent", check_id == self._hover_check_id)
        frame.setObjectName("checkTile")
        frame.setMouseTracking(True)
        frame.installEventFilter(_TileHoverFilter(self, check_id))
        self._tile_widgets[check_id] = frame
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        top_row = QHBoxLayout()
        top_row.setSpacing(6)
        title = QLabel(str(row["check_label"]))
        title.setObjectName("tileTitle")
        top_row.addWidget(title, 1)
        if freshness == 'Stale' and state == 'Unhealthy':
            stale_badge = QLabel('STALE')
            stale_badge.setObjectName('ackBadge')
            stale_badge.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            top_row.addWidget(stale_badge)
        if str(row["alert_state"] or "") == "ActiveAcknowledged":
            ack_badge = QLabel("ACK")
            ack_badge.setObjectName("ackBadge")
            ack_badge.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            top_row.addWidget(ack_badge)
        elif str(row["alert_state"] or "") == "SuppressedMaintenance":
            ack_badge = QLabel("MW")
            ack_badge.setObjectName("ackBadge")
            ack_badge.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            top_row.addWidget(ack_badge)
        if int(row["is_escalated"] or 0) == 1:
            esc_badge = QLabel("ESC")
            esc_badge.setObjectName("escBadge")
            esc_badge.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            top_row.addWidget(esc_badge)
        top_row.addStretch()
        layout.addLayout(top_row)

        detail = str(row["last_detail_message"] or row["description"] or "No detail yet").replace("\n", " ")
        detail_label = QLabel(detail[:72])
        detail_label.setWordWrap(True)
        detail_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(detail_label)

        meta_parts = []
        if row["last_value_numeric"] is not None:
            meta_parts.append(f"Value: {row['last_value_numeric']}")
        elif row["last_value_text"]:
            compact = str(row["last_value_text"]).strip().replace("\n", " ")
            meta_parts.append(f"Value: {compact[:28]}")
        if row["last_result_utc"]:
            meta_parts.append(f"Updated: {self._format_time(str(row['last_result_utc']))}")
        meta_label = QLabel(" • ".join(meta_parts) if meta_parts else "Waiting for first result")
        meta_label.setObjectName("tileMeta")
        meta_label.setWordWrap(True)
        meta_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(meta_label)

        button_row = QHBoxLayout()
        button_row.setSpacing(6)
        if str(row["alert_state"] or "") == "ActiveUnacknowledged" and self._can_ack():
            ack_button = QPushButton("ACK")
            ack_button.clicked.connect(lambda _=False, cid=check_id: self._ack_check(cid))
            button_row.addWidget(ack_button)
        button_row.addStretch()
        layout.addLayout(button_row)
        frame.mousePressEvent = lambda event, cid=check_id: self._open_check_details(cid)
        title.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        return frame

    def _apply_tile_focus_state(self) -> None:
        for check_id, widget in self._tile_widgets.items():
            widget.setProperty("selected", check_id == self._selected_check_id)
            widget.setProperty("hoverPersistent", check_id == self._hover_check_id)
            widget.style().unpolish(widget)
            widget.style().polish(widget)
            widget.update()

    def _load_selected_check_details(self) -> None:
        if self._selected_check_id is None:
            self._selected_name.setText("Choose a check tile")
            self._selected_status.setText("")
            self._details_name.setText("Select a check")
            self._details_state.setText("")
            self._details_summary.setText("Choose a tile to see current value and alert details.")
            self._details_last_update.setText("")
            self._details_meta.setText("")
            self._details_alert.setText("")
            self._details_owner_combo.blockSignals(True)
            self._details_owner_combo.clear()
            self._details_owner_combo.blockSignals(False)
            self._details_ack_button.setVisible(False)
            self._details_escalate_button.setVisible(False)
            self._details_salesforce_button.setVisible(False)
            self._tier1_doc_button.setVisible(False)
            self._tier2_doc_button.setVisible(False)
            self._escalation_banner.setVisible(False)
            self._info_card.setVisible(False)
            self._incident_card.setVisible(False)
            self._selected_header.setVisible(False)
            self._update_incident_action_state()
            return
        row = self._repo.get_check_details(self._selected_check_id)
        if row is None:
            self._selected_check_id = None
            return
        role = self._detail_role()
        state = str(row["operational_state"] or "Unknown")
        freshness = self._row_freshness_state(row)
        alert_state = str(row["alert_state"] or "None")
        self._selected_name.setText(str(row["display_label"]))
        self._selected_status.setText(self._format_combined_state(row))
        self._selected_status.setProperty("state", state)
        self._selected_status.style().unpolish(self._selected_status)
        self._selected_status.style().polish(self._selected_status)
        self._selected_header.setVisible(False)
        self._incident_card.setVisible(False)

        self._details_name.setText(str(row["display_label"]))
        self._details_state.setText(self._format_combined_state(row))
        self._details_state.setProperty("state", state)
        self._details_state.style().unpolish(self._details_state)
        self._details_state.style().polish(self._details_state)
        self._details_summary.setText(str(row["last_detail_message"] or row["description"] or "No details available"))
        last_result = self._format_time(str(row['last_result_utc'])) if row['last_result_utc'] else '(never)'
        value_display = row["last_value_numeric"] if row["last_value_numeric"] is not None else row["last_value_text"]
        self._details_last_update.setText(f"Last Update: {last_result}")

        if alert_state == "SuppressedMaintenance":
            self._details_alert.setText("Issue detected but suppressed by an active maintenance window.")
        elif row["alert_start_utc"]:
            ack_info = "Not acknowledged"
            if row["acknowledged_by_name"]:
                ack_info = f"Acknowledged by {row['acknowledged_by_name']} at {self._format_time(str(row['acknowledged_utc']))}"
            stale_note = " Data is currently stale." if freshness == 'Stale' else ""
            self._details_alert.setText(f"Active alert since {self._format_time(str(row['alert_start_utc']))}. {ack_info}.{stale_note}")
        else:
            self._details_alert.setText("No active alert.")
        owners = [(0, "Unassigned")] + [(int(u["user_id"]), str(u["display_name"])) for u in self._conn.execute("SELECT user_id, display_name FROM users WHERE is_active = 1 ORDER BY display_name").fetchall()]
        current_owner = str(row["owner_name"] or "")
        current_severity = str(row["severity"] or "Medium")
        self._details_owner_combo.blockSignals(True)
        self._details_owner_combo.clear()
        for owner_id, owner_name in owners:
            self._details_owner_combo.addItem(owner_name, owner_id)
            if owner_name == current_owner or (not current_owner and owner_id == 0):
                self._details_owner_combo.setCurrentIndex(self._details_owner_combo.count() - 1)
        self._details_owner_combo.blockSignals(False)
        self._details_severity_combo.blockSignals(True)
        idx = self._details_severity_combo.findData(current_severity)
        if idx >= 0:
            self._details_severity_combo.setCurrentIndex(idx)
        self._details_severity_combo.blockSignals(False)
        owner_enabled = self._can_ack() and alert_state in {"ActiveUnacknowledged", "ActiveAcknowledged"}
        self._details_owner_combo.setEnabled(owner_enabled)
        self._details_severity_combo.setEnabled(self._can_ack())
        if hasattr(self, "_header_severity_combo"):
            self._header_severity_combo.blockSignals(True)
            idx = self._header_severity_combo.findData(current_severity)
            if idx >= 0:
                self._header_severity_combo.setCurrentIndex(idx)
            self._header_severity_combo.setEnabled(self._can_ack())
            self._header_severity_combo.blockSignals(False)
        is_escalated = int(row["is_escalated"] or 0) == 1
        if is_escalated:
            esc_text = f"Escalation: Active"
            if row["escalated_by_name"]:
                esc_text += f" (Escalated by {row['escalated_by_name']} at {self._format_time(str(row['escalated_utc']))})"
            self._escalation_banner.setText(esc_text)
            self._escalation_banner.setVisible(True)
        else:
            self._escalation_banner.setVisible(False)

        show_ack = alert_state == "ActiveUnacknowledged" and self._can_ack()
        self._details_ack_button.setVisible(show_ack)
        self._details_escalate_button.setVisible(alert_state in {"ActiveUnacknowledged", "ActiveAcknowledged"} and self._can_ack() and not is_escalated)
        show_salesforce = alert_state in {"ActiveUnacknowledged", "ActiveAcknowledged"} or self._selected_check_has_recent_alert_context()
        self._details_salesforce_button.setVisible(show_salesforce)
        self._tier1_doc_button.setVisible(bool(row["troubleshooting_tier1_url"]))
        self._tier2_doc_button.setVisible(bool(row["troubleshooting_tier2_url"]))
        graph_type = str(row["graph_type"] or "Line")
        self._graph_type_combo.blockSignals(True)
        idx = self._graph_type_combo.findText(graph_type)
        if idx >= 0:
            self._graph_type_combo.setCurrentIndex(idx)
        self._graph_type_combo.blockSignals(False)

        info_lines = []
        threshold_parts = []
        if row['threshold_min'] is not None:
            threshold_parts.append(f"Min {row['threshold_min']}")
        if row['threshold_max'] is not None:
            threshold_parts.append(f"Max {row['threshold_max']}")
        if role in {"operator", "admin"}:
            if threshold_parts:
                info_lines.append(f"Threshold: {' | '.join(threshold_parts)}")
            if value_display is not None:
                info_lines.append(f"Current Value: {value_display}")
        if role == "admin":
            info_lines.extend([
                f"Parser: {row['parser_type']} | Rule: {row['rule_type']}",
                f"Pattern: {row['target_pattern'] or '(none)'}",
                f"Timing: expected={row['expected_interval_seconds']}s stale={row['stale_timeout_seconds']}s",
                f"Parse: {row['last_parse_status'] or '(n/a)'} | Age: {row['last_source_age_seconds'] if row['last_source_age_seconds'] is not None else '(n/a)'}s",
                f"Group: {row['group_label']}",
            ])
        elif role == "viewer":
            if value_display is not None:
                info_lines.append(f"Current Value: {value_display}")
        self._details_meta.setText("\n".join(info_lines))
        self._info_card.setVisible(bool(info_lines))


    def _load_history_tab(self) -> None:
        if self._selected_check_id is None:
            self._history_header.setText("Select a check to explore history.")
            self._history_summary.setText("No history loaded yet.")
            self._history_rows_cache = []
            theme_name = self._settings_repo.get_setting("current_theme", "Charcoal Blue")
            self._history_chart.apply_theme(self._theme_service.get_theme_tokens(theme_name))
            self._history_chart.set_data("History", [])
            self._reset_zoom_button.setEnabled(False)
            return
        details = self._repo.get_check_details(self._selected_check_id)
        history_rows = self._repo.get_check_history_for_range(self._selected_check_id, self._history_range)
        rows_as_dicts = [dict(row) for row in history_rows]
        self._history_rows_cache = rows_as_dicts
        title = f"{details['display_label']} • {self._history_range.upper()}"
        self._history_header.setText(title)
        threshold_min = float(details['threshold_min']) if details['threshold_min'] is not None else None
        threshold_max = float(details['threshold_max']) if details['threshold_max'] is not None else None

        # annotate alert markers from events when available
        event_rows = [dict(r) for r in self._repo.get_check_recent_events(self._selected_check_id)]
        if rows_as_dicts and event_rows:
            for event in event_rows:
                evt = str(event.get('event_type') or '')
                if evt not in {'AlertStarted', 'AlertCleared'}:
                    continue
                evt_time = str(event.get('event_utc') or '')
                best = min(rows_as_dicts, key=lambda r: abs(datetime.fromisoformat(str(r['evaluated_utc'])) - datetime.fromisoformat(evt_time)))
                best['event_marker'] = 'AlertStart' if evt == 'AlertStarted' else 'AlertEnd'

        theme_name = self._settings_repo.get_setting("current_theme", "Charcoal Blue")
        self._history_chart.apply_theme(self._theme_service.get_theme_tokens(theme_name))
        self._history_chart.set_graph_type(self._graph_type_combo.currentText())
        self._history_chart.set_data(title, rows_as_dicts, threshold_min, threshold_max)
        self._history_chart.chart().layout().invalidate()
        self._history_chart.chart().update()
        self._history_chart.viewport().update()
        self._history_chart.update()
        self._reset_zoom_button.setEnabled(self._history_chart.can_zoom())
        state_counts = defaultdict(int)
        numeric_values = []
        for row in rows_as_dicts:
            state_counts[str(row.get("operational_state") or "Unknown")] += 1
            if row.get("value_numeric") is not None:
                numeric_values.append(float(row["value_numeric"]))
        summary_parts = [f"Samples: {len(rows_as_dicts)}"]
        for key in ["Healthy", "Unhealthy", "Stale"]:
            if state_counts[key]:
                summary_parts.append(f"{key}: {state_counts[key]}")
        if numeric_values:
            summary_parts.append(f"Min/Max: {min(numeric_values):.2f} / {max(numeric_values):.2f}")
            summary_parts.append(f"Latest: {numeric_values[-1]:.2f}")
        recent_events = [dict(r) for r in self._repo.get_check_recent_events(self._selected_check_id, 6)]
        event_lines = []
        for event in recent_events[:4]:
            event_lines.append(f"{self._format_time(str(event.get('event_utc') or ''))} • {str(event.get('event_type') or '')}: {str(event.get('message') or '')}")
        base_summary = " | ".join(summary_parts) if summary_parts else "No history in selected range."
        if event_lines:
            self._history_summary.setText(base_summary + "\n\nRecent activity:\n" + "\n".join(event_lines))
        else:
            self._history_summary.setText(base_summary)

    def _load_history_activity_panel(self) -> None:
        self._history_header.setText("Activity History")
        self._history_summary.setText("Recent activity for the selected check.")
        self._clear_layout(self._detail_events_layout)
        if self._selected_check_id is None:
            empty = QLabel("Select a check to view activity history.")
            empty.setWordWrap(True)
            empty.setObjectName("detailCard")
            self._detail_events_layout.addWidget(empty)
            self._detail_events_layout.addStretch()
            return
        events = self._repo.get_check_recent_events(self._selected_check_id, 50)
        if not events:
            empty = QLabel("No recent activity recorded for this check.")
            empty.setWordWrap(True)
            empty.setObjectName("detailCard")
            self._detail_events_layout.addWidget(empty)
            self._detail_events_layout.addStretch()
            return
        for event_row in events:
            text = f"{self._format_time(str(event_row['event_utc']))} • {event_row['event_type']}"
            if event_row["user_name"]:
                text += f" • {event_row['user_name']}"
            text += f"\n{event_row['message']}"
            if event_row["detail"]:
                text += f"\n{event_row['detail']}"
            label = QLabel(text)
            label.setWordWrap(True)
            label.setObjectName("detailCard")
            self._detail_events_layout.addWidget(label)
        self._detail_events_layout.addStretch()

    def _load_configuration_data(self) -> None:
        self._checks_table.setUpdatesEnabled(False)
        self._checks_table.setRowCount(0)
        selected_row_to_restore = None
        for row_idx, row in enumerate(self._config_repo.list_checks()):
            self._checks_table.insertRow(row_idx)
            self._checks_table.setItem(row_idx, 0, self._table_item(str(row["display_label"]), int(row["check_id"])))
            self._checks_table.setItem(row_idx, 1, self._table_item(str(row["group_label"])))
            self._checks_table.setItem(row_idx, 2, self._table_item(str(row["rule_type"])))
            self._checks_table.setItem(row_idx, 3, self._table_item(str(row["parser_type"])))
            self._checks_table.setItem(row_idx, 4, self._table_item(str(row["relative_path_site1"] or "")))
            self._checks_table.setItem(row_idx, 5, self._table_item(str(row["relative_path_site2"] or "")))
            self._checks_table.setItem(row_idx, 6, self._table_item(f"{row['expected_interval_seconds']}s / {row['stale_timeout_seconds']}s"))
            self._checks_table.setItem(row_idx, 7, self._table_item("Yes" if int(row["is_enabled"]) == 1 else "No"))
            if self._selected_config_check_id is not None and int(row["check_id"]) == int(self._selected_config_check_id):
                selected_row_to_restore = row_idx
        self._checks_table.resizeColumnsToContents()
        if selected_row_to_restore is not None:
            self._checks_table.blockSignals(True)
            self._checks_table.selectRow(selected_row_to_restore)
            self._checks_table.setCurrentCell(selected_row_to_restore, 0)
            self._checks_table.blockSignals(False)
        self._checks_table.setUpdatesEnabled(True)

        self._groups_table.setUpdatesEnabled(False)
        self._groups_table.setRowCount(0)
        selected_group_row = None
        for row_idx, row in enumerate(self._config_repo.list_groups()):
            self._groups_table.insertRow(row_idx)
            self._groups_table.setItem(row_idx, 0, self._table_item(str(row["display_label"]), int(row["group_id"])))
            self._groups_table.setItem(row_idx, 1, self._table_item(str(row["group_name"])))
            self._groups_table.setItem(row_idx, 2, self._table_item(str(row["display_order"])))
            self._groups_table.setItem(row_idx, 3, self._table_item("Yes" if int(row["is_enabled"]) == 1 else "No"))
            if self._selected_group_id is not None and int(row["group_id"]) == int(self._selected_group_id):
                selected_group_row = row_idx
        self._groups_table.resizeColumnsToContents()
        if selected_group_row is not None:
            self._groups_table.blockSignals(True)
            self._groups_table.selectRow(selected_group_row)
            self._groups_table.setCurrentCell(selected_group_row, 0)
            self._groups_table.blockSignals(False)
        self._groups_table.setUpdatesEnabled(True)

        if hasattr(self, "_deleted_checks_table"):
            self._deleted_checks_table.setUpdatesEnabled(False)
            self._deleted_checks_table.setRowCount(0)
            restore_row = None
            for row_idx, row in enumerate(self._config_repo.list_deleted_checks()):
                self._deleted_checks_table.insertRow(row_idx)
                self._deleted_checks_table.setItem(row_idx, 0, self._table_item(str(row["display_label"]), int(row["check_id"])))
                self._deleted_checks_table.setItem(row_idx, 1, self._table_item(str(row["group_label"] or "")))
                self._deleted_checks_table.setItem(row_idx, 2, self._table_item(self._format_time(str(row["deleted_utc"])) if row["deleted_utc"] else ""))
                if self._selected_deleted_check_id is not None and int(row["check_id"]) == int(self._selected_deleted_check_id):
                    restore_row = row_idx
            self._deleted_checks_table.resizeColumnsToContents()
            if restore_row is not None:
                self._deleted_checks_table.blockSignals(True)
                self._deleted_checks_table.selectRow(restore_row)
                self._deleted_checks_table.setCurrentCell(restore_row, 0)
                self._deleted_checks_table.blockSignals(False)
            self._deleted_checks_table.setUpdatesEnabled(True)

        if hasattr(self, "_deleted_groups_table"):
            self._deleted_groups_table.setUpdatesEnabled(False)
            self._deleted_groups_table.setRowCount(0)
            restore_group_row = None
            for row_idx, row in enumerate(self._config_repo.list_deleted_groups()):
                self._deleted_groups_table.insertRow(row_idx)
                self._deleted_groups_table.setItem(row_idx, 0, self._table_item(str(row["display_label"]), int(row["group_id"])))
                self._deleted_groups_table.setItem(row_idx, 1, self._table_item(self._format_time(str(row["deleted_utc"])) if row["deleted_utc"] else ""))
                if self._selected_deleted_group_id is not None and int(row["group_id"]) == int(self._selected_deleted_group_id):
                    restore_group_row = row_idx
            self._deleted_groups_table.resizeColumnsToContents()
            if restore_group_row is not None:
                self._deleted_groups_table.blockSignals(True)
                self._deleted_groups_table.selectRow(restore_group_row)
                self._deleted_groups_table.setCurrentCell(restore_group_row, 0)
                self._deleted_groups_table.blockSignals(False)
            self._deleted_groups_table.setUpdatesEnabled(True)


    def _load_users_data(self) -> None:
        if not hasattr(self, "_users_table"):
            return
        self._users_table.setUpdatesEnabled(False)
        self._users_table.setRowCount(0)
        selected_row_to_restore = None
        users = self._user_repo.list_users()
        for row_idx, row in enumerate(users):
            self._users_table.insertRow(row_idx)
            self._users_table.setItem(row_idx, 0, self._table_item(str(row["username"]), int(row["user_id"])))
            self._users_table.setItem(row_idx, 1, self._table_item(str(row["display_name"])))
            self._users_table.setItem(row_idx, 2, self._table_item(str(row["role_name"])))
            self._users_table.setItem(row_idx, 3, self._table_item("Yes" if int(row["is_active"]) == 1 else "No"))
            self._users_table.setItem(row_idx, 4, self._table_item(self._format_time(str(row["created_utc"]))))
            self._users_table.setItem(row_idx, 5, self._table_item(self._format_time(str(row["last_login_utc"])) if row["last_login_utc"] else "Never"))
            if self._selected_user_id is not None and int(row["user_id"]) == int(self._selected_user_id):
                selected_row_to_restore = row_idx
        self._users_table.resizeColumnsToContents()
        if selected_row_to_restore is not None:
            self._users_table.blockSignals(True)
            self._users_table.selectRow(selected_row_to_restore)
            self._users_table.setCurrentCell(selected_row_to_restore, 0)
            self._users_table.blockSignals(False)
        self._users_table.setUpdatesEnabled(True)
        if users:
            active_count = sum(1 for r in users if int(r['is_active']) == 1)
            self._users_status.setText(f"Users: {len(users)} total | {active_count} active")
        else:
            self._users_status.setText("No users found.")

    def _on_user_selection_changed(self) -> None:
        items = self._users_table.selectedItems() if hasattr(self, "_users_table") else []
        self._selected_user_id = int(items[0].data(Qt.ItemDataRole.UserRole)) if items else None

    def _new_user(self) -> None:
        if self._user.role_name != "SystemAdmin":
            return
        dialog = UserDialog(self._user_repo.list_role_names(), self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        data = dialog.get_data()
        if not data['username'] or not data['display_name'] or not data['password']:
            QMessageBox.warning(self, "Create user", "Username, display name, and password are required.")
            return
        try:
            user_id = self._user_repo.create_user(self._user.user_id, str(data['username']), str(data['display_name']), str(data['password']), str(data['role_name']), bool(data['is_active']))
            self._selected_user_id = user_id
            self._load_users_data()
            self._load_audit_data()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Create user", str(exc))

    def _edit_selected_user(self) -> None:
        if self._user.role_name != "SystemAdmin" or self._selected_user_id is None:
            return
        row = self._user_repo.get_user(self._selected_user_id)
        if row is None:
            return
        dialog = UserDialog(self._user_repo.list_role_names(), self, dict(row))
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        data = dialog.get_data()
        if not data['username'] or not data['display_name']:
            QMessageBox.warning(self, "Update user", "Username and display name are required.")
            return
        try:
            self._user_repo.update_user(self._selected_user_id, self._user.user_id, str(data['username']), str(data['display_name']), str(data['role_name']), bool(data['is_active']))
            self._load_users_data()
            self._load_audit_data()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Update user", str(exc))

    def _reset_selected_user_password(self) -> None:
        if self._user.role_name != "SystemAdmin" or self._selected_user_id is None:
            return
        row = self._user_repo.get_user(self._selected_user_id)
        if row is None:
            return
        dialog = PasswordResetDialog(str(row['display_name']), self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        if not dialog.passwords_match():
            QMessageBox.warning(self, "Reset password", "Passwords must match and cannot be blank.")
            return
        try:
            self._user_repo.reset_password(self._selected_user_id, self._user.user_id, dialog.get_password())
            self._load_audit_data()
            QMessageBox.information(self, "Reset password", f"Password reset for {row['display_name']}.")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Reset password", str(exc))

    def _toggle_selected_user(self) -> None:
        if self._user.role_name != "SystemAdmin" or self._selected_user_id is None:
            return
        try:
            self._user_repo.toggle_user_active(self._selected_user_id, self._user.user_id)
            self._load_users_data()
            self._load_audit_data()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Enable / Disable user", str(exc))

    def _is_incident_active(self) -> bool:
        return self._settings_repo.get_setting("incident_active", "0") == "1"

    def _is_incident_active(self) -> bool:
        return self._settings_repo.get_setting("incident_active", "0") == "1"

    def _get_incident_history(self) -> list[dict[str, str]]:
        raw = self._settings_repo.get_setting("incident_history", "[]")
        try:
            data = json.loads(raw)
        except Exception:
            logger.exception("Failed to parse incident history")
            return []
        history: list[dict[str, str]] = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("start_utc"):
                    history.append({
                        "start_utc": str(item.get("start_utc", "")),
                        "end_utc": str(item.get("end_utc", "")),
                        "started_by": str(item.get("started_by", "")),
                        "ended_by": str(item.get("ended_by", "")),
                    })
        return history

    def _set_incident_history(self, history: list[dict[str, str]]) -> None:
        self._settings_repo.set_setting("incident_history", json.dumps(history), self._user.user_id)

    @staticmethod
    def _make_incident_key(start_utc: str, end_utc: str = "", is_active: bool = False) -> str:
        return f"active|{start_utc}" if is_active else f"history|{start_utc}|{end_utc}"

    def _get_available_incidents(self) -> list[dict[str, str | bool]]:
        incidents: list[dict[str, str | bool]] = []
        if self._is_incident_active():
            start_utc = self._settings_repo.get_setting("incident_start_utc", "")
            if start_utc:
                incidents.append({
                    "key": self._make_incident_key(start_utc, is_active=True),
                    "start_utc": start_utc,
                    "end_utc": "",
                    "started_by": self._settings_repo.get_setting("incident_started_by", ""),
                    "ended_by": "",
                    "is_active": True,
                })
        for item in self._get_incident_history():
            start_utc = str(item.get("start_utc", ""))
            end_utc = str(item.get("end_utc", ""))
            if not start_utc:
                continue
            incidents.append({
                "key": self._make_incident_key(start_utc, end_utc=end_utc, is_active=False),
                "start_utc": start_utc,
                "end_utc": end_utc,
                "started_by": str(item.get("started_by", "")),
                "ended_by": str(item.get("ended_by", "")),
                "is_active": False,
            })
        incidents.sort(key=lambda item: str(item.get("start_utc") or ""), reverse=True)
        return incidents[:10]

    def _format_incident_choice(self, incident: dict[str, str | bool]) -> str:
        start_text = self._format_time(str(incident.get("start_utc") or ""))
        end_utc = str(incident.get("end_utc") or "")
        end_text = "Present" if bool(incident.get("is_active")) and not end_utc else self._format_time(end_utc)
        starter = str(incident.get("started_by") or "")
        prefix = "ACTIVE: " if bool(incident.get("is_active")) else ""
        return f"{prefix}{start_text} → {end_text} | {starter}"

    def _refresh_incident_selector(self) -> None:
        if not hasattr(self, "_incident_selector"):
            return
        incidents = self._get_available_incidents()
        self._incident_context_map = {str(item.get("key") or ""): item for item in incidents if str(item.get("key") or "")}
        target_key = self._selected_incident_key
        if not target_key and incidents:
            target_key = str(incidents[0].get("key") or "")
        self._incident_selector_refreshing = True
        self._incident_selector.blockSignals(True)
        self._incident_selector.clear()
        if incidents:
            index_to_select = 0
            for idx, incident in enumerate(incidents):
                incident_key = str(incident.get("key") or "")
                self._incident_selector.addItem(self._format_incident_choice(incident), incident_key)
                if target_key and incident_key == target_key:
                    index_to_select = idx
            self._incident_selector.setCurrentIndex(index_to_select)
            self._selected_incident_key = str(self._incident_selector.itemData(index_to_select) or "") or str(incidents[index_to_select].get("key") or "")
            self._incident_selector.setEnabled(True)
        else:
            self._incident_selector.addItem("No incident history available", "")
            self._incident_selector.setCurrentIndex(0)
            self._incident_selector.setEnabled(False)
            self._selected_incident_key = None
        self._incident_selector.blockSignals(False)
        self._incident_selector_refreshing = False

    def _on_incident_selection_changed(self, index: int) -> None:
        if self._incident_selector_refreshing or index < 0:
            return
        data = self._incident_selector.itemData(index)
        incident_key = str(data or "")
        self._selected_incident_key = incident_key or None
        self._load_incident_panel()

    def _get_selected_incident_context(self) -> dict[str, str | bool] | None:
        incident_key = None
        if hasattr(self, "_incident_selector") and self._incident_selector.count() > 0 and self._incident_selector.isEnabled():
            incident_key = str(self._incident_selector.currentData() or "")
        if not incident_key:
            incident_key = self._selected_incident_key or ""
        if not self._incident_context_map:
            self._incident_context_map = {str(item.get("key") or ""): item for item in self._get_available_incidents() if str(item.get("key") or "")}
        context = self._incident_context_map.get(incident_key) if incident_key else None
        if context is not None:
            self._selected_incident_key = incident_key
            return context
        incidents = list(self._incident_context_map.values())
        if incidents:
            first = incidents[0]
            self._selected_incident_key = str(first.get("key") or "")
            return first
        return None

    def _toggle_incident_mode(self) -> None:
        now = datetime.utcnow().isoformat()
        if not self._is_incident_active():
            self._settings_repo.set_setting("incident_active", "1", self._user.user_id)
            self._settings_repo.set_setting("incident_start_utc", now, self._user.user_id)
            self._settings_repo.set_setting("incident_started_by", self._user.display_name, self._user.user_id)
            self._conn.execute("INSERT INTO events(event_utc, event_type, user_id, message, detail) VALUES (?, 'IncidentStarted', ?, ?, ?)", (now, self._user.user_id, f'Incident mode started by {self._user.display_name}', 'Incident mode enabled from header toggle'))
            self._selected_incident_key = self._make_incident_key(now, is_active=True)
            self._conn.commit()
        else:
            start_utc = self._settings_repo.get_setting("incident_start_utc", "")
            started_by = self._settings_repo.get_setting("incident_started_by", "")
            self._conn.execute("INSERT INTO events(event_utc, event_type, user_id, message, detail) VALUES (?, 'IncidentEnded', ?, ?, ?)", (now, self._user.user_id, f'Incident mode ended by {self._user.display_name}', 'Incident mode disabled from header toggle'))
            self._settings_repo.set_setting("incident_last_end_utc", now, self._user.user_id)
            self._settings_repo.set_setting("incident_last_ended_by", self._user.display_name, self._user.user_id)
            self._settings_repo.set_setting("incident_active", "0", self._user.user_id)
            if start_utc:
                history = self._get_incident_history()
                history.insert(0, {
                    "start_utc": start_utc,
                    "end_utc": now,
                    "started_by": started_by,
                    "ended_by": self._user.display_name,
                })
                deduped: list[dict[str, str]] = []
                seen: set[tuple[str, str]] = set()
                for item in history:
                    key = (str(item.get("start_utc", "")), str(item.get("end_utc", "")))
                    if not key[0] or key in seen:
                        continue
                    seen.add(key)
                    deduped.append(item)
                self._set_incident_history(deduped[:50])
                self._selected_incident_key = self._make_incident_key(start_utc, end_utc=now, is_active=False)
            self._conn.commit()
        self._load_data()
        self._refresh_incident_runtime_ui(force=True)
        if self._is_incident_active() and hasattr(self, "_main_tabs") and hasattr(self, "_dashboard_tab"):
            dashboard_idx = self._main_tabs.indexOf(self._dashboard_tab)
            if dashboard_idx >= 0:
                self._main_tabs.setCurrentIndex(dashboard_idx)
            self._refresh_dashboard_workspace_mode()
            self._refresh_incident_workspace(self._get_selected_incident_context())
        self._load_audit_data()

    @staticmethod
    def _format_alert_type_display(row) -> str:
        alert_type = str(row['alert_type'] or '')
        detail = str(row['start_message'] or row['clear_message'] or '')
        if alert_type == 'Unhealthy' and 'stale' in detail.lower():
            return 'Unhealthy (Stale Data)'
        return alert_type or 'Unknown'

    def _get_alerts_active_at_incident_start(self, start_utc: str) -> list[sqlite3.Row]:
        return list(self._conn.execute(
            """
            SELECT ai.alert_instance_id, ai.check_id, ai.site_id, ai.alert_type, ai.start_utc, ai.end_utc, ai.is_active,
                   ai.start_message, ai.clear_message, c.display_label, s.site_name AS site_label
            FROM alert_instances ai
            JOIN checks c ON c.check_id = ai.check_id
            JOIN sites s ON s.site_id = ai.site_id
            WHERE ai.start_utc <= ?
              AND (ai.end_utc IS NULL OR ai.end_utc >= ?)
            ORDER BY ai.start_utc ASC, c.display_label ASC
            """,
            (start_utc, start_utc),
        ))

    @staticmethod
    def _format_duration_between(start_utc: str, end_utc: str) -> str:
        try:
            start_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end_utc.replace("Z", "+00:00"))
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            total_seconds = max(int((end_dt - start_dt).total_seconds()), 0)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except ValueError:
            return "Unknown"

    def _get_incident_timeline_rows(self, context: dict[str, str | bool] | None = None) -> list[dict[str, str | None]]:
        context = context or self._get_selected_incident_context()
        if not context:
            return []
        start_utc = str(context.get("start_utc") or "")
        end_utc = str(context.get("end_utc") or "")
        if not start_utc:
            return []
        lookback_minutes_raw = self._settings_repo.get_setting("incident_lookback_minutes", "30")
        try:
            lookback_minutes = max(0, int(lookback_minutes_raw))
        except ValueError:
            lookback_minutes = 30
        try:
            start_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            start_dt = datetime.now(timezone.utc)
        window_start_dt = start_dt - timedelta(minutes=lookback_minutes)
        previous_incident_end = ""
        for item in self._get_incident_history():
            item_end = str(item.get("end_utc") or "")
            if item_end and item_end < start_utc and item_end > previous_incident_end:
                previous_incident_end = item_end
        if previous_incident_end:
            try:
                prev_dt = datetime.fromisoformat(previous_incident_end.replace("Z", "+00:00"))
                if prev_dt.tzinfo is None:
                    prev_dt = prev_dt.replace(tzinfo=timezone.utc)
                if prev_dt > window_start_dt:
                    window_start_dt = prev_dt
            except ValueError:
                pass
        window_start_utc = window_start_dt.isoformat()
        query = """
            SELECT e.event_utc, e.event_type, e.message, e.detail, c.display_label AS check_label,
                   u.display_name AS user_name, e.alert_instance_id
            FROM events e
            LEFT JOIN checks c ON c.check_id = e.check_id
            LEFT JOIN users u ON u.user_id = e.user_id
            WHERE e.event_utc >= ?
        """
        params: list[str] = [window_start_utc]
        if end_utc:
            query += " AND e.event_utc <= ?"
            params.append(end_utc)
        if not self._incident_show_all_events:
            query += " AND (e.event_type LIKE 'Alert%' OR e.event_type LIKE 'Incident%' OR e.event_type IN ('DataBecameStale','DataBecameFresh','TicketPrepared','UserCreated','UserUpdated','UserDisabled','UserEnabled','PasswordReset','IncidentMetadataUpdated','IncidentNoteAdded'))"
        query += " ORDER BY e.event_utc DESC LIMIT 250"
        timeline_rows = [
            {
                "event_utc": str(row["event_utc"]),
                "event_type": str(row["event_type"]),
                "message": str(row["message"]),
                "detail": str(row["detail"]) if row["detail"] else None,
                "check_label": str(row["check_label"]) if row["check_label"] else None,
                "user_name": str(row["user_name"]) if row["user_name"] else None,
                "alert_instance_id": str(row["alert_instance_id"]) if row["alert_instance_id"] is not None else None,
            }
            for row in self._conn.execute(query, params)
        ]
        existing_starts = {
            int(row["alert_instance_id"])
            for row in timeline_rows
            if row.get("alert_instance_id") is not None and str(row.get("event_type") or "") == "AlertStarted"
        }
        for alert_row in self._get_alerts_active_at_incident_start(start_utc):
            alert_instance_id = int(alert_row["alert_instance_id"])
            if alert_instance_id in existing_starts:
                continue
            start_event = self._conn.execute(
                """
                SELECT e.event_utc, e.message, e.detail, u.display_name AS user_name
                FROM events e
                LEFT JOIN users u ON u.user_id = e.user_id
                WHERE e.alert_instance_id = ? AND e.event_type = 'AlertStarted'
                ORDER BY e.event_utc ASC
                LIMIT 1
                """,
                (alert_instance_id,),
            ).fetchone()
            event_utc = str(start_event["event_utc"]) if start_event else str(alert_row["start_utc"])
            message = str(start_event["message"]) if start_event else str(alert_row["start_message"])
            detail = str(start_event["detail"]) if start_event and start_event["detail"] else "Alert was already active when Incident Mode started."
            timeline_rows.append({
                "event_utc": event_utc,
                "event_type": "AlertStarted",
                "message": message,
                "detail": detail,
                "check_label": str(alert_row["display_label"]),
                "user_name": str(start_event["user_name"]) if start_event and start_event["user_name"] else None,
                "alert_instance_id": str(alert_instance_id),
            })
        timeline_rows.sort(key=lambda row: str(row.get("event_utc") or ""), reverse=True)
        return timeline_rows[:250]

    def _get_incident_end_utc(self, context: dict[str, str | bool] | None = None) -> str:
        context = context or self._get_selected_incident_context()
        if not context:
            return ""
        return str(context.get("end_utc") or "")

    def _build_incident_summary_lines(self, context: dict[str, str | bool] | None) -> list[str]:
        if not context:
            return ["No incident is selected."]
        start_utc = str(context.get("start_utc") or "")
        end_utc = str(context.get("end_utc") or "")
        rows = self._get_incident_timeline_rows(context)
        impacted_alerts = self._get_incident_impacted_alerts(start_utc, end_utc)
        active_alerts = [row for row in impacted_alerts if int(row["is_active"] or 0) == 1]
        alerts_active_at_start = self._get_alerts_active_at_incident_start(start_utc)
        lines = [
            f"Started: {self._format_time(start_utc)}",
            f"Started By: {str(context.get('started_by') or '') or 'Unknown'}",
        ]
        if end_utc:
            lines.append(f"Ended: {self._format_time(end_utc)}")
            lines.append(f"Ended By: {str(context.get('ended_by') or '') or 'Unknown'}")
        else:
            lines.append("Ended: Incident still active")
        lookback_minutes = self._settings_repo.get_setting("incident_lookback_minutes", "30")
        lines.extend(["", f"Pre-Incident Context Window: last {lookback_minutes} minutes (stops at previous incident boundary)", f"Alerts Active At Incident Start: {len(alerts_active_at_start)}"])
        if alerts_active_at_start:
            for row in alerts_active_at_start:
                duration = self._format_duration_between(str(row['start_utc']), start_utc)
                state_text = "still active" if int(row["is_active"] or 0) == 1 else f"cleared {self._format_time(str(row['end_utc']))}" if row["end_utc"] else "cleared"
                lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) | first started {self._format_time(str(row['start_utc']))} | active for {duration} before incident start | {state_text}")
        else:
            lines.append("- None")
        lines.extend(["", f"Active Alerts At Export: {len(active_alerts)}"])
        if active_alerts:
            for row in active_alerts:
                lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) since {self._format_time(str(row['start_utc']))}")
        else:
            lines.append("- None")
        lines.extend(["", f"Impacted Checks During Incident: {len(impacted_alerts)}"])
        if impacted_alerts:
            for row in impacted_alerts:
                state_text = "active" if int(row["is_active"] or 0) == 1 else f"cleared {self._format_time(str(row['end_utc']))}" if row["end_utc"] else "cleared"
                lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) | started {self._format_time(str(row['start_utc']))} | {state_text}")
        else:
            lines.append("- None")
        lines.extend(["", f"Timeline Events: {len(rows)}"])
        if rows:
            for row in reversed(rows):
                lines.append(f"- {self._format_time(str(row['event_utc']))} | {row['event_type']} | {row['message']}")
        else:
            lines.append("- None")
        return lines

    def _set_incident_summary_text_preserve_scroll(self, text: str) -> None:
        if not hasattr(self, "_incident_summary"):
            return
        if hasattr(self._incident_summary, "toPlainText") and self._incident_summary.toPlainText() == text:
            return
        scrollbar = self._incident_summary.verticalScrollBar() if hasattr(self._incident_summary, "verticalScrollBar") else None
        cursor = self._incident_summary.textCursor() if hasattr(self._incident_summary, "textCursor") else None
        cursor_position = cursor.position() if cursor is not None else 0
        scroll_value = scrollbar.value() if scrollbar is not None else 0
        old_max = scrollbar.maximum() if scrollbar is not None else 0
        updates_prev = self._incident_summary.updatesEnabled()
        self._incident_summary.setUpdatesEnabled(False)
        try:
            if hasattr(self._incident_summary, 'setPlainText'):
                self._incident_summary.setPlainText(text)
            else:
                self._incident_summary.setText(text)
            if scrollbar is not None:
                if old_max > 0 and scroll_value >= old_max - 2:
                    target = scrollbar.maximum()
                else:
                    target = min(scroll_value, scrollbar.maximum())
                scrollbar.setValue(max(0, target))
            if hasattr(self._incident_summary, 'textCursor'):
                cur = self._incident_summary.textCursor()
                cur.setPosition(max(0, min(cursor_position, len(self._incident_summary.toPlainText()))))
                self._incident_summary.setTextCursor(cur)
        finally:
            self._incident_summary.setUpdatesEnabled(updates_prev)
            self._incident_summary.viewport().update()

    def _load_incident_panel(self, preserve_editor_state: bool = True) -> None:
        if not hasattr(self, "_incident_header"):
            return
        self._refresh_incident_selector()
        context = self._get_selected_incident_context()
        active = self._is_incident_active()
        started = self._settings_repo.get_setting("incident_start_utc", "")
        started_by = self._settings_repo.get_setting("incident_started_by", "")
        self._incident_toggle_button.setText("End Incident" if active else "Start Incident")
        if self._incident_banner is not None:
            self._incident_banner.setVisible(active)
            if active and started:
                self._incident_banner.setText(f"Incident Mode Active • Started {self._format_time(started)} by {started_by}. Dashboard is focused on active issues.")
            else:
                self._incident_banner.setVisible(False)
        if context:
            ctx_start = str(context.get("start_utc") or "")
            ctx_end = str(context.get("end_utc") or "")
            ctx_started_by = str(context.get("started_by") or "")
            ctx_ended_by = str(context.get("ended_by") or "")
            if bool(context.get("is_active")) and not ctx_end:
                self._incident_header.setText(f"Viewing Active Incident: {self._format_time(ctx_start)} → Present | {ctx_started_by}")
            else:
                end_text = self._format_time(ctx_end) if ctx_end else "Present"
                extra = f" | Ended by {ctx_ended_by}" if ctx_ended_by else ""
                self._incident_header.setText(f"Viewing Incident: {self._format_time(ctx_start)} → {end_text} | {ctx_started_by}{extra}")
        else:
            self._incident_header.setText("Incident mode is currently off. No incident history available.")
        self._clear_layout(self._incident_leadership_layout)
        self._clear_layout(self._incident_system_layout)
        rows = self._get_incident_timeline_rows(context) if context else []
        active_alerts = self._conn.execute("SELECT COUNT(1) AS c FROM alert_instances WHERE is_active = 1").fetchone()
        unack = self._conn.execute("SELECT COUNT(1) AS c FROM current_check_status WHERE operational_state != 'Healthy' AND is_acknowledged = 0").fetchone()
        counts_line = " | ".join([
            f"Active Alerts: {int(active_alerts['c']) if active_alerts else 0}",
            f"Unacknowledged: {int(unack['c']) if unack else 0}",
            f"Timeline Events: {len(rows)}",
        ])
        if context:
            commander_value = self._settings_repo.get_setting(f"incident_commander_{str(context.get('key') or '')}", self._settings_repo.get_setting("incident_commander_current", ""))
            bridge_value = self._settings_repo.get_setting(f"incident_bridge_{str(context.get('key') or '')}", self._settings_repo.get_setting("incident_bridge_current", ""))
            if (not preserve_editor_state) or (not self._incident_commander_edit.hasFocus() and not getattr(self, "_incident_commander_dirty", False) and self._incident_commander_edit.text() != commander_value):
                self._incident_commander_edit.setText(commander_value)
            if (not preserve_editor_state) or (not self._incident_bridge_edit.hasFocus() and not getattr(self, "_incident_bridge_dirty", False) and self._incident_bridge_edit.text() != bridge_value):
                self._incident_bridge_edit.setText(bridge_value)
            if hasattr(self, "_incident_mode_commander_edit") and ((not preserve_editor_state) or (not self._incident_mode_commander_edit.hasFocus() and not getattr(self, "_incident_commander_dirty", False) and self._incident_mode_commander_edit.text() != commander_value)):
                self._incident_mode_commander_edit.blockSignals(True)
                self._incident_mode_commander_edit.setText(commander_value)
                self._incident_mode_commander_edit.blockSignals(False)
            if hasattr(self, "_incident_mode_bridge_edit") and ((not preserve_editor_state) or (not self._incident_mode_bridge_edit.hasFocus() and not getattr(self, "_incident_bridge_dirty", False) and self._incident_mode_bridge_edit.text() != bridge_value)):
                self._incident_mode_bridge_edit.blockSignals(True)
                self._incident_mode_bridge_edit.setText(bridge_value)
                self._incident_mode_bridge_edit.blockSignals(False)
            title_value = self._settings_repo.get_setting(f"incident_title_{str(context.get('key') or '')}", "")
            status_value = self._settings_repo.get_setting(f"incident_status_{str(context.get('key') or '')}", "Active")
            overview_value = self._settings_repo.get_setting(f"incident_overview_{str(context.get('key') or '')}", "")
            if hasattr(self, "_incident_title_edit") and ((not preserve_editor_state) or (not self._incident_title_edit.hasFocus() and not getattr(self, "_incident_title_dirty", False) and self._incident_title_edit.text() != title_value)):
                self._incident_title_edit.setText(title_value)
            if hasattr(self, "_incident_status_combo") and ((not preserve_editor_state) or (not getattr(self, "_incident_status_dirty", False))):
                idx = self._incident_status_combo.findText(status_value)
                if idx >= 0:
                    self._incident_status_combo.blockSignals(True)
                    self._incident_status_combo.setCurrentIndex(idx)
                    self._incident_status_combo.blockSignals(False)
            if hasattr(self, "_incident_overview_edit") and ((not preserve_editor_state) or (not self._incident_overview_edit.hasFocus() and not getattr(self, "_incident_overview_dirty", False) and self._incident_overview_edit.toPlainText() != overview_value)):
                self._incident_overview_edit.blockSignals(True)
                self._incident_overview_edit.setPlainText(overview_value)
                self._incident_overview_edit.blockSignals(False)
            if hasattr(self, "_incident_started_value"):
                self._incident_started_value.setText(self._format_time(ctx_start) if ctx_start else "--")
            if hasattr(self, "_incident_owner_value"):
                self._incident_owner_value.setText(ctx_started_by or "Unknown")
        summary_lines = self._build_incident_summary_lines(context)
        incident_summary_text = counts_line + "\n\n" + "\n".join(summary_lines)
        self._set_incident_summary_text_preserve_scroll(incident_summary_text)
        leadership_rows = [row for row in rows if str(row.get('event_type') or '') == 'IncidentNoteAdded']
        system_rows = [row for row in rows if str(row.get('event_type') or '') != 'IncidentNoteAdded']

        if not leadership_rows:
            self._incident_leadership_layout.addWidget(QLabel("No leadership timeline entries available yet."))
        else:
            for row in leadership_rows[:50]:
                text = f"{self._format_time(str(row['event_utc']))}"
                if row.get('user_name'):
                    text += f" • {row['user_name']}"
                text += f"\n{row['message']}"
                label = QLabel(text)
                label.setWordWrap(True)
                label.setObjectName("detailCard")
                self._incident_leadership_layout.addWidget(label)
        self._incident_leadership_layout.addStretch()

        if not system_rows:
            self._incident_system_layout.addWidget(QLabel("No system activity available yet."))
        else:
            for row in system_rows[:50]:
                text = f"{self._format_time(str(row['event_utc']))} • {row['event_type']}"
                if row.get('check_label'):
                    text += f" • {row['check_label']}"
                if row.get('user_name'):
                    text += f" • {row['user_name']}"
                text += f"\n{row['message']}"
                if row.get('detail'):
                    text += f"\n{row['detail']}"
                label = QLabel(text)
                label.setWordWrap(True)
                label.setObjectName("detailCard")
                self._incident_system_layout.addWidget(label)
        self._incident_system_layout.addStretch()
        self._refresh_incident_workspace(context)

    def _open_incident_tab(self) -> None:
        if hasattr(self, "_main_tabs"):
            idx = self._main_tabs.indexOf(self._incident_tab)
            if idx >= 0:
                self._main_tabs.setCurrentIndex(idx)
                self._load_incident_panel(preserve_editor_state=False)
                return
        if hasattr(self, "_detail_tabs"):
            idx = self._detail_tabs.indexOf(self._incident_tab)
            if idx >= 0:
                self._detail_tabs.setCurrentIndex(idx)
                self._expand_details_panel()
                self._load_incident_panel(preserve_editor_state=False)

    def _open_selected_check_popout(self) -> None:
        if self._selected_check_id is None:
            QMessageBox.information(self, "Selected Check", "Select a check first.")
            return
        if self._check_details_popout is None:
            self._check_details_popout = _CheckDetailsPopoutDialog(self)
        self._refresh_selected_check_popout()
        self._check_details_popout.show()
        self._check_details_popout.raise_()
        self._check_details_popout.activateWindow()

    def _build_selected_check_popout_payload(self) -> dict[str, object] | None:
        if self._selected_check_id is None:
            return None
        row = self._repo.get_check_details(self._selected_check_id)
        if row is None:
            return None
        owners = [(0, "Unassigned")] + [(int(u["user_id"]), str(u["display_name"])) for u in self._conn.execute("SELECT user_id, display_name FROM users WHERE is_active = 1 ORDER BY display_name").fetchall()]
        meta_lines = []
        if row["last_result_utc"]:
            meta_lines.append(f"Last Update: {self._format_time(str(row['last_result_utc']))}")
        if row["graph_type"]:
            meta_lines.append(f"Graph: {row['graph_type']}")
        if row["last_value_text"] not in {None, ''}:
            meta_lines.append(f"Last Value: {row['last_value_text']}")
        elif row["last_value_numeric"] is not None:
            meta_lines.append(f"Last Value: {row['last_value_numeric']}")
        row_keys = set(row.keys()) if hasattr(row, "keys") else set()
        owner_user_id = int(row["owner_user_id"] or 0) if "owner_user_id" in row_keys else 0
        severity = str(row["severity"] or "Medium") if "severity" in row_keys else "Medium"
        payload = {
            "title": str(row["display_label"]),
            "status": self._format_combined_state(row),
            "meta": " | ".join(meta_lines),
            "summary": str(row["last_detail_message"] or row["description"] or "No details available"),
            "alert": str(self._details_alert.text() if hasattr(self, "_details_alert") else ""),
            "info": str(self._details_meta.text() if hasattr(self, "_details_meta") else ""),
            "owners": owners,
            "owner_user_id": owner_user_id,
            "severity": severity,
            "can_edit": self._can_ack(),
            "owner_enabled": self._details_owner_combo.isEnabled() if hasattr(self, "_details_owner_combo") else False,
            "show_ack": self._details_ack_button.isVisible() if hasattr(self, "_details_ack_button") else False,
        }
        return payload

    def _refresh_selected_check_popout(self) -> None:
        if self._check_details_popout is None or not self._check_details_popout.isVisible():
            return
        payload = self._build_selected_check_popout_payload()
        if payload is None:
            self._check_details_popout.hide()
            return
        self._check_details_popout.set_payload(payload)

    def _current_incident_refresh_token(self) -> tuple:
        selected_key = self._selected_incident_key or ""
        active = self._is_incident_active()
        start_utc = self._settings_repo.get_setting("incident_start_utc", "")
        history_raw = self._settings_repo.get_setting("incident_history", "[]")
        event_row = self._conn.execute("SELECT COALESCE(MAX(event_id), 0) AS max_id FROM events").fetchone()
        max_event_id = int(event_row["max_id"]) if event_row else 0
        return (active, start_utc, history_raw, selected_key, self._incident_show_all_events, max_event_id)

    def _refresh_incident_runtime_ui(self, force: bool = False) -> None:
        if not hasattr(self, "_incident_toggle_button"):
            return
        token = self._current_incident_refresh_token()
        self._incident_toggle_button.setText("End Incident" if token[0] else "Start Incident")
        if not force and token == self._last_incident_refresh_token:
            return
        self._last_incident_refresh_token = token
        self._load_incident_panel()

    def _selected_check_has_recent_alert_context(self) -> bool:
        if self._selected_check_id is None:
            return False
        recent_events = self._repo.get_check_recent_events(self._selected_check_id, 12)
        alert_event_types = {"AlertStarted", "AlertCleared", "AlertAcknowledged", "AlertEscalated", "AlertImported"}
        if any(str(event["event_type"] or "") in alert_event_types for event in recent_events):
            return True
        if self._is_incident_active():
            for event in recent_events:
                if str(event["event_type"] or "") in alert_event_types:
                    return True
        return False

    def _toggle_incident_show_all_events(self, checked: bool) -> None:
        self._incident_show_all_events = checked
        self._load_incident_panel()

    def _get_incident_impacted_alerts(self, start_utc: str, end_utc: str) -> list[sqlite3.Row]:
        window_end = end_utc or datetime.now(timezone.utc).isoformat()
        return list(self._conn.execute(
            """
            SELECT c.display_label, ai.alert_type, ai.start_utc, ai.end_utc, ai.is_active, ai.start_message, ai.clear_message
            FROM alert_instances ai
            JOIN checks c ON c.check_id = ai.check_id
            WHERE ai.start_utc <= ?
              AND (ai.end_utc IS NULL OR ai.end_utc >= ?)
            ORDER BY ai.start_utc ASC, c.display_label ASC
            """,
            (window_end, start_utc),
        ))

    def _export_incident_summary_text(self) -> None:
        try:
            context = self._get_selected_incident_context()
            if not context:
                QMessageBox.information(self, "Export Incident Summary", "No incident is available to export yet.")
                return
            start_utc = str(context.get("start_utc") or "")
            end_utc = str(context.get("end_utc") or "")
            rows = self._get_incident_timeline_rows(context)
            impacted_alerts = self._get_incident_impacted_alerts(start_utc, end_utc)
            active_alerts = [row for row in impacted_alerts if int(row["is_active"] or 0) == 1]
            alerts_active_at_start = self._get_alerts_active_at_incident_start(start_utc)
            lines = [
                "Incident Summary",
                f"Started: {self._format_time(start_utc)}",
                f"Started By: {str(context.get('started_by') or '') or 'Unknown'}",
            ]
            if end_utc:
                lines.append(f"Ended: {self._format_time(end_utc)}")
                lines.append(f"Ended By: {str(context.get('ended_by') or '') or 'Unknown'}")
            else:
                lines.append("Ended: Incident still active")
            lines.extend(["", "Alerts Active At Incident Start:"])
            if alerts_active_at_start:
                for row in alerts_active_at_start:
                    duration = self._format_duration_between(str(row['start_utc']), start_utc)
                    state_text = "still active" if int(row["is_active"] or 0) == 1 else f"cleared {self._format_time(str(row['end_utc']))}" if row["end_utc"] else "cleared"
                    lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) first alert started {self._format_time(str(row['start_utc']))} | active for {duration} before incident start | {state_text}")
            else:
                lines.append("- None")
            lines.extend(["", "Active Alerts At Export:"])
            if active_alerts:
                for row in active_alerts:
                    lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) since {self._format_time(str(row['start_utc']))}")
            else:
                lines.append("- None")
            lines.extend(["", "Impacted Checks During Incident:"])
            if impacted_alerts:
                for row in impacted_alerts:
                    state_text = "active" if int(row["is_active"] or 0) == 1 else f"cleared {self._format_time(str(row['end_utc']))}" if row["end_utc"] else "cleared"
                    lines.append(f"- {row['display_label']} ({self._format_alert_type_display(row)}) started {self._format_time(str(row['start_utc']))} | {state_text}")
            else:
                lines.append("- None")
            lines.append("")
            lines.append("Timeline:")
            for row in reversed(rows):
                line = f"- {self._format_time(str(row['event_utc']))} | {row['event_type']} | {row['message']}"
                if row.get('check_label'):
                    line += f" | {row['check_label']}"
                lines.append(line)
            default_name = f"opsmonitor_incident_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            backups_dir = self._resolve_runtime_path("backups")
            backups_dir.mkdir(parents=True, exist_ok=True)
            selected_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Incident Summary",
                str(backups_dir / default_name),
                "Text Files (*.txt);;All Files (*)",
            )
            if not selected_path:
                return
            path = Path(selected_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(lines), encoding='utf-8')
            QMessageBox.information(self, "Export complete", f"Incident summary exported to:\n{path}")
        except Exception as exc:
            logger.exception("Failed to export incident summary")
            QMessageBox.critical(self, "Export Incident Summary", f"Failed to export incident summary.\n\n{exc}")

    def _export_incident_timeline_csv(self) -> None:
        try:
            context = self._get_selected_incident_context()
            if not context:
                QMessageBox.information(self, "Export Incident Timeline", "No incident is available to export yet.")
                return
            rows = self._get_incident_timeline_rows(context)
            default_name = f"opsmonitor_incident_timeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            backups_dir = self._resolve_runtime_path("backups")
            backups_dir.mkdir(parents=True, exist_ok=True)
            selected_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Incident Timeline CSV",
                str(backups_dir / default_name),
                "CSV Files (*.csv);;All Files (*)",
            )
            if not selected_path:
                return
            path = Path(selected_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(["event_time_et", "event_type", "check_label", "user_name", "message", "detail"])
                for row in rows:
                    writer.writerow([self._format_time(str(row['event_utc'])), row.get('event_type') or '', row.get('check_label') or '', row.get('user_name') or '', row.get('message') or '', row.get('detail') or ''])
            QMessageBox.information(self, "Export complete", f"Incident timeline exported to:\n{path}")
        except Exception as exc:
            logger.exception("Failed to export incident timeline")
            QMessageBox.critical(self, "Export Incident Timeline", f"Failed to export incident timeline.\n\n{exc}")

    def _load_audit_data(self) -> None:
        if not hasattr(self, "_audit_table"):
            return
        self._audit_table.setUpdatesEnabled(False)
        self._audit_table.setRowCount(0)
        selected_row_to_restore = None
        for row_idx, row in enumerate(self._settings_repo.get_audit_entries(200)):
            self._audit_table.insertRow(row_idx)
            key = (
                str(row["audit_utc"]),
                str(row["action_type"]),
                str(row["entity_type"]),
                str(row["message"] or ""),
            )
            time_item = self._table_item(self._format_time(str(row["audit_utc"])), key)
            self._audit_table.setItem(row_idx, 0, time_item)
            self._audit_table.setItem(row_idx, 1, self._table_item(str(row["user_name"] or "System")))
            self._audit_table.setItem(row_idx, 2, self._table_item(str(row["action_type"])))
            self._audit_table.setItem(row_idx, 3, self._table_item(str(row["entity_type"])))
            self._audit_table.setItem(row_idx, 4, self._table_item(str(row["entity_name"] or "")))
            self._audit_table.setItem(row_idx, 5, self._table_item(str(row["message"] or "")))
            if self._selected_audit_key is not None and key == self._selected_audit_key:
                selected_row_to_restore = row_idx
        self._audit_table.resizeColumnsToContents()
        if selected_row_to_restore is not None:
            self._audit_table.blockSignals(True)
            self._audit_table.selectRow(selected_row_to_restore)
            self._audit_table.setCurrentCell(selected_row_to_restore, 0)
            self._audit_table.blockSignals(False)
        self._audit_table.setUpdatesEnabled(True)

    def _load_settings_data(self) -> None:
        if not hasattr(self, "_theme_combo"):
            return
        current_theme = self._settings_repo.get_setting("current_theme", "Charcoal Blue")
        idx = self._theme_combo.findData(current_theme)
        if idx >= 0 and self._theme_combo.currentIndex() != idx:
            self._theme_combo.blockSignals(True)
            self._theme_combo.setCurrentIndex(idx)
            self._theme_combo.blockSignals(False)
        current_zoom = self._settings_repo.get_setting("ui_zoom_percent", "100")
        if hasattr(self, "_header_zoom_label"):
            self._header_zoom_label.setText(f"{current_zoom}%")
        zoom_idx = self._zoom_combo.findData(current_zoom)
        if zoom_idx >= 0 and self._zoom_combo.currentIndex() != zoom_idx:
            self._zoom_combo.blockSignals(True)
            self._zoom_combo.setCurrentIndex(zoom_idx)
            self._zoom_combo.blockSignals(False)
        current_density = self._settings_repo.get_setting("dashboard_density", "Compact")
        density_idx = self._density_combo.findData(current_density)
        if density_idx >= 0 and self._density_combo.currentIndex() != density_idx:
            self._density_combo.blockSignals(True)
            self._density_combo.setCurrentIndex(density_idx)
            self._density_combo.blockSignals(False)
        current_mode = self._settings_repo.get_setting("active_mode", "Test")
        retention_value = self._settings_repo.get_setting("retention_days", "90")
        incident_lookback_value = self._settings_repo.get_setting("incident_lookback_minutes", "30")
        mode_idx = self._mode_combo.findData(current_mode)
        if mode_idx >= 0 and self._mode_combo.currentIndex() != mode_idx:
            self._mode_combo.blockSignals(True)
            self._mode_combo.setCurrentIndex(mode_idx)
            self._mode_combo.blockSignals(False)
        current_site = self._settings_repo.get_setting("active_site_id", "1")
        site_idx = self._site_combo.findData(current_site)
        if site_idx >= 0 and self._site_combo.currentIndex() != site_idx:
            self._site_combo.blockSignals(True)
            self._site_combo.setCurrentIndex(site_idx)
            self._site_combo.blockSignals(False)
        test_root = self._settings_repo.get_setting("test_data_root_path", "TestData")
        resolved_test_root = self._resolve_runtime_path(test_root) if test_root else self._app_root_path() / 'TestData'
        self._test_root_edit.setText(str(resolved_test_root))
        live_site1 = self._settings_repo.get_setting("live_data_root_site1", "")
        live_site2 = self._settings_repo.get_setting("live_data_root_site2", "")
        self._live_site1_root_edit.setText(str(self._resolve_runtime_path(live_site1)) if live_site1 else "(not set)")
        self._live_site2_root_edit.setText(str(self._resolve_runtime_path(live_site2)) if live_site2 else "(not set)")
        show_only_issues = self._settings_repo.get_setting("dashboard_show_only_issues", "0") == "1"
        self._show_only_issues_setting.blockSignals(True)
        self._show_only_issues_setting.setChecked(show_only_issues)
        self._show_only_issues_setting.blockSignals(False)
        retention_idx = self._retention_combo.findData(retention_value)
        if retention_idx >= 0 and self._retention_combo.currentIndex() != retention_idx:
            self._retention_combo.blockSignals(True)
            self._retention_combo.setCurrentIndex(retention_idx)
            self._retention_combo.blockSignals(False)
        lookback_idx = self._incident_lookback_combo.findData(incident_lookback_value)
        if lookback_idx >= 0 and self._incident_lookback_combo.currentIndex() != lookback_idx:
            self._incident_lookback_combo.blockSignals(True)
            self._incident_lookback_combo.setCurrentIndex(lookback_idx)
            self._incident_lookback_combo.blockSignals(False)
        data_root = self._data_root_path()
        schema_version = self._settings_repo.get_setting('schema_version', '2')
        app_version = self._settings_repo.get_setting('app_version', APP_VERSION)
        migration_status = self._settings_repo.get_setting('last_startup_migration_status', 'Unknown')
        last_backup = self._settings_repo.get_setting('last_startup_backup_path', '') or '(none this startup)'
        self._data_path_label.setText(f"Active Data Path: {data_root}\nData Mode: {'Shared (network path)' if self._is_shared_data_mode() else 'Local'}\nSchema Version: {schema_version} | App Version: {app_version}\nStartup Migration: {migration_status}\nLast Startup Backup: {last_backup}\nLast Console Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._backup_status.setText(f"Database: {self._database_file_path()}\nBackup folder: {self._settings_repo.get_setting('backup_path_hint', str(self._resolve_runtime_path('backups')))}\nExport folder: {self._settings_repo.get_setting('export_path_hint', str(self._resolve_runtime_path('exports')))}\nData path changes require a full application restart.")
        self._refresh_shared_mode_indicator()
        self._set_data_path_button.setEnabled(self._can_manage_environment())
        self._compact_value.setText(
            "Compact shows more checks per row and is best for operations. Standard uses larger tiles for readability. The dashboard can also hide healthy checks when you want an issue-only view. Persistent data now lives outside the build folder so upgrades can reuse the same database, logs, backups, and exports. Incident lookback controls how much pre-incident context appears before a new incident starts, while still stopping at the prior incident boundary."
        )
        if current_mode == "Test" and not resolved_test_root.exists():
            self._set_status_banner(f"Test data root not found: {resolved_test_root}. Update it in Settings before running validation.")
        elif current_mode == "Live":
            active_site = self._settings_repo.get_setting("active_site_id", "1")
            live_root = live_site1 if active_site == "1" else live_site2
            if not live_root:
                self._set_status_banner(f"Live mode is active but the live folder for Site {active_site} is not configured yet.")
            else:
                resolved_live = self._resolve_runtime_path(live_root)
                if not resolved_live.exists():
                    self._set_status_banner(f"Live data root not found: {resolved_live}. Update it in Settings before running live mode.")
        self._apply_zoom_setting()

    def _apply_retention_setting(self) -> None:
        if not self._can_manage_environment():
            self._load_settings_data()
            return
        value = str(self._retention_combo.currentData() or "90")
        self._settings_repo.set_setting("retention_days", value, self._user.user_id)
        self._run_retention_cleanup(force=True)
        self._load_settings_data()

    def _apply_incident_lookback_setting(self) -> None:
        if not self._can_manage_environment():
            self._load_settings_data()
            return
        value = str(self._incident_lookback_combo.currentData() or "30")
        self._settings_repo.set_setting("incident_lookback_minutes", value, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'IncidentLookbackUpdated', 'Settings', 'incident_lookback_minutes', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f"Incident lookback updated to {value} minutes")
        )
        self._conn.commit()
        self._load_settings_data()
        self._refresh_incident_runtime_ui(force=True)

    def _run_retention_cleanup(self, force: bool = False) -> None:
        try:
            result = self._settings_repo.run_retention_cleanup(self._user.user_id, force=force)
            if str(result.get("ran") or "0") == "1":
                self._conn.execute(
                    "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'RetentionCleanup', 'Settings', 'retention_days', ?)",
                    (datetime.utcnow().isoformat(), self._user.user_id, f"Retention cleanup completed. Rows purged: {result.get('rows_purged', 0)}")
                )
                self._conn.commit()
        except Exception as exc:
            logger.exception("Retention cleanup failed")
            self._set_status_banner(f"Retention cleanup error: {exc}")

    def _apply_alert_owner(self) -> None:
        if self._selected_check_id is None or not hasattr(self, "_details_owner_combo"):
            return
        if self._details_owner_combo.signalsBlocked() or not self._can_ack():
            return
        owner_id = self._details_owner_combo.currentData()
        owner_user_id = int(owner_id) if owner_id not in {None, 0, "0"} else None
        self._monitoring_repo.set_alert_owner(self._selected_check_id, owner_user_id, self._user.user_id)
        self._load_data()

    def _apply_alert_severity(self) -> None:
        if self._selected_check_id is None or not hasattr(self, "_details_severity_combo"):
            return
        if self._details_severity_combo.signalsBlocked() or not self._can_ack():
            return
        severity = str(self._details_severity_combo.currentData() or self._details_severity_combo.currentText() or "Medium")
        self._monitoring_repo.set_alert_severity(self._selected_check_id, severity, self._user.user_id)
        self._load_data()

    def _apply_header_alert_severity(self) -> None:
        if not hasattr(self, "_header_severity_combo") or self._header_severity_combo.signalsBlocked() or not self._can_ack():
            return
        if hasattr(self, "_details_severity_combo"):
            idx = self._details_severity_combo.findData(self._header_severity_combo.currentData())
            self._details_severity_combo.blockSignals(True)
            if idx >= 0:
                self._details_severity_combo.setCurrentIndex(idx)
            self._details_severity_combo.blockSignals(False)
        self._apply_alert_severity()

    def _open_incident_popout(self) -> None:
        if not hasattr(self, "_incident_popout_dialog") or self._incident_popout_dialog is None:
            self._incident_popout_dialog = _IncidentDetailsPopoutDialog(self)
        self._refresh_incident_popout()
        self._incident_popout_dialog.show()
        self._incident_popout_dialog.raise_()
        self._incident_popout_dialog.activateWindow()

    def _refresh_incident_popout(self) -> None:
        if not hasattr(self, "_incident_popout_dialog") or self._incident_popout_dialog is None:
            return
        context = self._get_selected_incident_context()
        rows = self._get_incident_timeline_rows(context) if context else []
        timeline_lines = []
        for row in rows:
            line = f"{self._format_time(str(row['event_utc']))} | {row['event_type']}"
            if row.get('check_label'):
                line += f" | {row['check_label']}"
            if row.get('user_name'):
                line += f" | {row['user_name']}"
            line += f"\n{row['message']}"
            timeline_lines.append(line)
        payload = {
            "header": self._incident_header.text() if hasattr(self, "_incident_header") else "Incident",
            "summary": self._incident_summary.toPlainText() if hasattr(self, "_incident_summary") else "",
            "timeline": "\n\n".join(timeline_lines) if timeline_lines else "No incident timeline events available yet.",
        }
        self._incident_popout_dialog.set_payload(payload)

    def _save_incident_metadata(self, silent: bool = False) -> None:
        key = self._selected_incident_key or (self._make_incident_key(self._settings_repo.get_setting("incident_start_utc", ""), is_active=True) if self._is_incident_active() else "")
        commander = self._incident_commander_edit.text().strip()
        bridge = self._incident_bridge_edit.text().strip()
        title = self._incident_title_edit.text().strip() if hasattr(self, "_incident_title_edit") else ""
        status = self._incident_status_combo.currentText().strip() if hasattr(self, "_incident_status_combo") else "Active"
        overview = self._incident_overview_edit.toPlainText().strip() if hasattr(self, "_incident_overview_edit") else ""
        if not key:
            if not silent:
                QMessageBox.information(self, "Incident Details", "No active or selected incident is available.")
            return
        current_saved_commander = self._settings_repo.get_setting(f"incident_commander_{key}", self._settings_repo.get_setting("incident_commander_current", ""))
        current_saved_bridge = self._settings_repo.get_setting(f"incident_bridge_{key}", self._settings_repo.get_setting("incident_bridge_current", ""))
        current_saved_title = self._settings_repo.get_setting(f"incident_title_{key}", "")
        current_saved_status = self._settings_repo.get_setting(f"incident_status_{key}", "Active")
        current_saved_overview = self._settings_repo.get_setting(f"incident_overview_{key}", "")
        if commander == current_saved_commander and bridge == current_saved_bridge and title == current_saved_title and status == current_saved_status and overview == current_saved_overview:
            self._incident_commander_dirty = False
            self._incident_bridge_dirty = False
            self._incident_title_dirty = False
            self._incident_status_dirty = False
            self._incident_overview_dirty = False
            return
        self._settings_repo.set_setting(f"incident_commander_{key}", commander, self._user.user_id)
        self._settings_repo.set_setting(f"incident_bridge_{key}", bridge, self._user.user_id)
        self._settings_repo.set_setting(f"incident_title_{key}", title, self._user.user_id)
        self._settings_repo.set_setting(f"incident_status_{key}", status, self._user.user_id)
        self._settings_repo.set_setting(f"incident_overview_{key}", overview, self._user.user_id)
        if self._is_incident_active():
            self._settings_repo.set_setting("incident_commander_current", commander, self._user.user_id)
            self._settings_repo.set_setting("incident_bridge_current", bridge, self._user.user_id)
        self._incident_commander_dirty = False
        self._incident_bridge_dirty = False
        self._incident_title_dirty = False
        self._incident_status_dirty = False
        self._incident_overview_dirty = False
        self._monitoring_repo.insert_event({
            "event_utc": datetime.now(timezone.utc).isoformat(), "event_type": "IncidentMetadataUpdated", "check_id": None, "alert_instance_id": None,
            "site_id": None, "mode_name": self._settings_repo.get_setting("active_mode", "Test"), "user_id": self._user.user_id,
            "message": "Incident metadata updated", "detail": f"Commander={commander or '(blank)'} | Title={title or '(blank)'} | Status={status or '(blank)'} | Bridge={bridge or '(blank)'} | Overview={'set' if overview else '(blank)'}",
        })
        self._conn.commit()
        self._refresh_incident_runtime_ui(force=True)


    def _mark_incident_commander_dirty(self, *_args) -> None:
        self._incident_commander_dirty = True
        self._schedule_incident_metadata_autosave()

    def _mark_incident_bridge_dirty(self, *_args) -> None:
        self._incident_bridge_dirty = True
        self._schedule_incident_metadata_autosave()


    def _mark_incident_title_dirty(self, *_args) -> None:
        self._incident_title_dirty = True
        self._schedule_incident_metadata_autosave()

    def _mark_incident_status_dirty(self, *_args) -> None:
        self._incident_status_dirty = True
        self._schedule_incident_metadata_autosave()

    def _mark_incident_overview_dirty(self) -> None:
        self._incident_overview_dirty = True
        self._schedule_incident_metadata_autosave()
    def _schedule_incident_metadata_autosave(self) -> None:
        if hasattr(self, "_incident_metadata_autosave_timer"):
            self._incident_metadata_autosave_timer.start()

    def _autosave_incident_metadata_if_dirty(self) -> None:
        if not (getattr(self, "_incident_commander_dirty", False) or getattr(self, "_incident_bridge_dirty", False) or getattr(self, "_incident_title_dirty", False) or getattr(self, "_incident_status_dirty", False) or getattr(self, "_incident_overview_dirty", False)):
            return
        self._save_incident_metadata(silent=True)

    def _mark_incident_note_entry_dirty(self) -> None:
        self._incident_note_entry_dirty = bool(self._incident_note_entry_edit.toPlainText().strip())

    def _add_incident_timeline_entry(self) -> None:
        key = self._selected_incident_key or (self._make_incident_key(self._settings_repo.get_setting("incident_start_utc", ""), is_active=True) if self._is_incident_active() else "")
        note = self._incident_note_entry_edit.toPlainText().strip()
        if not note and hasattr(self, "_incident_mode_entry_edit"):
            note = self._incident_mode_entry_edit.toPlainText().strip()
        if not key:
            QMessageBox.information(self, "Timeline Entry", "No active or selected incident is available.")
            return
        if not note:
            QMessageBox.information(self, "Timeline Entry", "Enter a timeline entry before adding it.")
            return
        self._monitoring_repo.insert_event({
            "event_utc": datetime.now(timezone.utc).isoformat(), "event_type": "IncidentNoteAdded", "check_id": None, "alert_instance_id": None,
            "site_id": None, "mode_name": self._settings_repo.get_setting("active_mode", "Test"), "user_id": self._user.user_id,
            "message": note, "detail": f"IncidentKey={key}",
        })
        self._conn.commit()
        self._incident_note_entry_edit.blockSignals(True)
        self._incident_note_entry_edit.clear()
        self._incident_note_entry_edit.blockSignals(False)
        if hasattr(self, "_incident_mode_entry_edit"):
            self._incident_mode_entry_edit.blockSignals(True)
            self._incident_mode_entry_edit.clear()
            self._incident_mode_entry_edit.blockSignals(False)
        self._incident_note_entry_dirty = False
        self._refresh_incident_runtime_ui(force=True)

    def _apply_mode_setting(self) -> None:
        if not self._can_manage_environment():
            self._load_settings_data()
            return
        previous_mode = self._settings_repo.get_setting("active_mode", "Test")
        mode_name = str(self._mode_combo.currentData() or self._mode_combo.currentText())
        if mode_name == previous_mode:
            self._refresh_mode_site_indicator()
            return
        if mode_name == "Live":
            reply = QMessageBox.question(
                self,
                "Confirm Live Mode",
                "You are switching to LIVE monitoring sources. Continue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                self._load_settings_data()
                return
        self._settings_repo.set_setting("active_mode", mode_name, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'RuntimeModeUpdated', 'Settings', 'active_mode', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Runtime mode changed: {previous_mode} -> {mode_name}')
        )
        self._conn.commit()
        self._load_settings_data()
        if mode_name == "Live":
            self._maybe_auto_start_transfer()
        else:
            self._stop_transfer_engine()
        self._run_cycle_and_refresh()

    def _apply_active_site_setting(self) -> None:
        if not self._can_switch_site():
            self._load_settings_data()
            return
        previous_site = self._settings_repo.get_setting("active_site_id", "1")
        new_site = str(self._site_combo.currentData() or self._site_combo.currentText().replace("Site ", ""))
        if new_site == previous_site:
            self._refresh_mode_site_indicator()
            return
        self._settings_repo.set_setting("active_site_id", new_site, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ActiveSiteUpdated', 'Settings', 'active_site_id', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Active site changed: Site {previous_site} -> Site {new_site}')
        )
        self._conn.commit()
        self._selected_check_id = None
        self._load_settings_data()
        self._handle_transfer_site_change()
        self._run_cycle_and_refresh()


    def _handle_transfer_site_change(self) -> None:
        if self._transfer_engine.snapshot().get("is_running"):
            self._stop_transfer_engine()
            self._start_transfer_engine()
        else:
            self._refresh_transfer_status_ui()

    def closeEvent(self, event) -> None:
        try:
            self._stop_transfer_engine()
        finally:
            super().closeEvent(event)
    def _apply_selected_theme(self) -> None:
        theme_name = str(self._theme_combo.currentData() or self._theme_combo.currentText())
        self._settings_repo.set_setting("current_theme", theme_name, self._user.user_id)
        self._apply_zoom_setting()

    def _apply_zoom_combo(self) -> None:
        zoom = str(self._zoom_combo.currentData() or self._zoom_combo.currentText().replace("%", ""))
        self._settings_repo.set_setting("ui_zoom_percent", zoom, self._user.user_id)
        self._apply_zoom_setting()

    def _decrease_zoom(self) -> None:
        self._step_zoom(-1)

    def _increase_zoom(self) -> None:
        self._step_zoom(1)

    def _step_zoom(self, direction: int) -> None:
        levels = [90, 100, 110, 125, 150]
        current = int(self._settings_repo.get_setting("ui_zoom_percent", "100"))
        try:
            idx = levels.index(current)
        except ValueError:
            idx = 1
        idx = max(0, min(len(levels) - 1, idx + direction))
        self._settings_repo.set_setting("ui_zoom_percent", str(levels[idx]), self._user.user_id)
        self._apply_zoom_setting()

    def _apply_density_setting(self) -> None:
        density = str(self._density_combo.currentData() or self._density_combo.currentText())
        self._settings_repo.set_setting("dashboard_density", density, self._user.user_id)
        self._load_data()
        self._load_settings_data()

    def _toggle_show_only_issues(self, checked: bool) -> None:
        self._settings_repo.set_setting("dashboard_show_only_issues", "1" if checked else "0", self._user.user_id)
        self._load_data()
        self._load_settings_data()

    def _apply_show_only_issues_setting(self) -> None:
        checked = self._show_only_issues_setting.isChecked()
        self._settings_repo.set_setting("dashboard_show_only_issues", "1" if checked else "0", self._user.user_id)
        self._load_data()

    def _choose_test_root_folder(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Select Test Data Root", str(self._resolve_runtime_path(self._settings_repo.get_setting("test_data_root_path", "TestData"))))
        if not path:
            return
        self._settings_repo.set_setting("test_data_root_path", path, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'TestRootUpdated', 'Settings', 'test_data_root_path', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Test data root set to {path}')
        )
        self._conn.commit()
        self._load_settings_data()
        self._run_cycle_and_refresh()

    def _choose_live_root_folder(self, site_id: int) -> None:
        setting_key = "live_data_root_site1" if site_id == 1 else "live_data_root_site2"
        current_value = self._settings_repo.get_setting(setting_key, "")
        start_path = str(self._resolve_runtime_path(current_value)) if current_value else str(self._app_root_path())
        path = QFileDialog.getExistingDirectory(self, f"Select Live Data Root - Site {site_id}", start_path)
        if not path:
            return
        self._settings_repo.set_setting(setting_key, path, self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'LiveRootUpdated', 'Settings', ?, ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, setting_key, f'Live data root for Site {site_id} set to {path}')
        )
        self._conn.commit()
        self._load_settings_data()
        if self._settings_repo.get_setting("active_mode", "Test") == "Live" and self._settings_repo.get_setting("active_site_id", "1") == str(site_id):
            self._run_cycle_and_refresh()

    def _data_root_path(self) -> Path:
        hinted = self._settings_repo.get_setting("data_root_path_hint", "")
        if hinted:
            return Path(hinted).resolve()
        db_path = Path(self._database_file_path()).resolve()
        return db_path.parent.parent if db_path.parent.name.lower() == "database" else db_path.parent

    def _app_root_path(self) -> Path:
        return self._data_root_path()

    def _resolve_runtime_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return (self._data_root_path() / path).resolve()

    def _choose_persistent_data_folder(self) -> None:
        if not self._can_manage_environment():
            return
        current_hint = self._settings_repo.get_setting("data_root_path_hint", str(self._data_root_path()))
        path = QFileDialog.getExistingDirectory(self, "Select Persistent Data Folder", current_hint)
        if not path:
            return
        try:
            candidate = Path(path)
            candidate.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(prefix="opsmonitor_write_test_", dir=str(candidate), delete=True) as _tmp:
                _tmp.write(b"ok")
                _tmp.flush()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Persistent Data Folder", f"Could not use the selected folder.\n\n{exc}")
            return
        save_bootstrap_data_path_override(str(candidate))
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'DataPathUpdated', 'Settings', 'data_path_override', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f"Persistent data path override set to {candidate}. Restart required.")
        )
        self._conn.commit()
        QMessageBox.information(self, "Restart Required", f"Persistent data path saved:\n{candidate}\n\nThis change will take effect after a full application restart.")
        self._load_audit_data()
        self._load_settings_data()

    def _user_id_for_name(self, display_name: str | None) -> int | None:
        if not display_name:
            return None
        row = self._conn.execute("SELECT user_id FROM users WHERE display_name = ? OR username = ? LIMIT 1", (display_name, display_name)).fetchone()
        return int(row["user_id"]) if row else None

    def _site_id_for_name(self, site_name: str | None) -> int | None:
        if not site_name:
            return None
        row = self._conn.execute("SELECT site_id FROM sites WHERE site_name = ? LIMIT 1", (site_name,)).fetchone()
        return int(row["site_id"]) if row else None

    def _check_id_for_label(self, check_label: str | None) -> int | None:
        if not check_label:
            return None
        row = self._conn.execute("SELECT check_id FROM checks WHERE display_label = ? OR internal_name = ? LIMIT 1", (check_label, check_label)).fetchone()
        return int(row["check_id"]) if row else None

    def _database_file_path(self) -> str:
        row = self._conn.execute("PRAGMA database_list").fetchone()
        return str(row[2]) if row and len(row) > 2 else "OpsMonitor.db"

    def _backup_database(self) -> None:
        db_path = self._database_file_path()
        default_name = f"OpsMonitor_Backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        backup_dir = self._resolve_runtime_path("backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        path, _ = QFileDialog.getSaveFileName(self, "Backup Database", str(backup_dir / default_name), "SQLite Database (*.db)")
        if not path:
            return
        self._conn.commit()
        shutil.copy2(db_path, path)
        self._settings_repo.set_setting("backup_path_hint", str(Path(path).parent), self._user.user_id)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'DatabaseBackup', 'System', 'SQLite', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Backup created at {path}')
        )
        self._conn.commit()
        QMessageBox.information(self, "Backup complete", f"Database backup created:\n{path}")
        self._load_audit_data()
        self._load_settings_data()

    def _import_database_backup(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Database Backup", "", "SQLite Database (*.db)")
        if not path:
            return
        if QMessageBox.question(self, "Import Database Backup", "This will replace the current database contents with the selected backup. Continue?") != QMessageBox.StandardButton.Yes:
            return
        source = sqlite3.connect(path)
        try:
            self._conn.commit()
            source.backup(self._conn)
            initialize_database(self._conn)
            self._conn.execute(
                "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'DatabaseImport', 'System', 'SQLite', ?)",
                (datetime.utcnow().isoformat(), self._user.user_id, f'Database imported from {path}')
            )
            self._conn.commit()
        finally:
            source.close()
        self._load_configuration_data()
        self._load_audit_data()
        self._load_settings_data()
        self._load_data()
        QMessageBox.information(self, "Import complete", f"Database backup imported from:\n{path}")

    def _import_configuration_json(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Configuration", "", "JSON Files (*.json)")
        if not path:
            return
        if QMessageBox.question(self, "Import Configuration", "Import groups and checks from this configuration file? Existing matching records will be updated.") != QMessageBox.StandardButton.Yes:
            return
        with open(path, 'r', encoding='utf-8') as handle:
            payload = json.load(handle)
        groups = payload.get('groups', [])
        checks = payload.get('checks', [])
        now = datetime.utcnow().isoformat()
        imported_groups = 0
        imported_checks = 0
        for group in groups:
            group_name = str(group.get('group_name') or group.get('display_label') or '').strip()
            if not group_name:
                continue
            display_label = str(group.get('display_label') or group_name)
            display_order = int(group.get('display_order') or 0)
            is_enabled = 1 if int(group.get('is_enabled', 1) or 0) == 1 else 0
            description = group.get('description')
            existing = self._conn.execute("SELECT group_id FROM groups WHERE group_name = ?", (group_name,)).fetchone()
            if existing:
                self._conn.execute("UPDATE groups SET display_label = ?, display_order = ?, is_enabled = ?, description = ?, updated_utc = ? WHERE group_id = ?", (display_label, display_order, is_enabled, description, now, int(existing['group_id'])))
            else:
                self._conn.execute("INSERT INTO groups(group_name, display_label, display_order, is_enabled, description, created_utc, updated_utc) VALUES (?, ?, ?, ?, ?, ?, ?)", (group_name, display_label, display_order, is_enabled, description, now, now))
            imported_groups += 1

        group_map = {str(r['display_label']): int(r['group_id']) for r in self._conn.execute("SELECT group_id, display_label FROM groups")}
        for check in checks:
            internal_name = str(check.get('internal_name') or '').strip()
            if not internal_name:
                continue
            group_label = str(check.get('group_label') or '').strip()
            group_id = group_map.get(group_label)
            if group_id is None:
                continue
            display_label = str(check.get('display_label') or internal_name)
            is_enabled = 1 if int(check.get('is_enabled', 1) or 0) == 1 else 0
            display_order = int(check.get('display_order') or 0)
            existing = self._conn.execute("SELECT check_id FROM checks WHERE internal_name = ?", (internal_name,)).fetchone()
            if existing:
                check_id = int(existing['check_id'])
                self._conn.execute("UPDATE checks SET display_label = ?, group_id = ?, is_enabled = ?, display_order = ?, updated_utc = ?, troubleshooting_tier1_url = ?, troubleshooting_tier2_url = ?, graph_type = ? WHERE check_id = ?", (display_label, group_id, is_enabled, display_order, now, check.get('troubleshooting_tier1_url'), check.get('troubleshooting_tier2_url'), check.get('graph_type') or 'Line', check_id))
            else:
                self._conn.execute("INSERT INTO checks(internal_name, display_label, group_id, description, is_enabled, display_order, applies_to_site1, applies_to_site2, created_utc, updated_utc, created_by_user_id, updated_by_user_id, troubleshooting_tier1_url, troubleshooting_tier2_url, graph_type) VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, ?, ?, ?)", (internal_name, display_label, group_id, None, is_enabled, display_order, now, now, self._user.user_id, self._user.user_id, check.get('troubleshooting_tier1_url'), check.get('troubleshooting_tier2_url'), check.get('graph_type') or 'Line'))
                check_id = int(self._conn.execute("SELECT check_id FROM checks WHERE internal_name = ?", (internal_name,)).fetchone()['check_id'])
            source_exists = self._conn.execute("SELECT 1 FROM check_source_config WHERE check_id = ?", (check_id,)).fetchone()
            source_vals = (str(check.get('relative_path_site1') or ''), str(check.get('relative_path_site2') or ''), str(check.get('parser_type') or 'RawText'), str(check.get('target_pattern') or ''), check_id)
            if source_exists:
                self._conn.execute("UPDATE check_source_config SET relative_path_site1 = ?, relative_path_site2 = ?, parser_type = ?, target_pattern = ? WHERE check_id = ?", source_vals)
            else:
                self._conn.execute("INSERT INTO check_source_config(check_id, source_type, relative_path_site1, relative_path_site2, file_pattern, parser_type, match_strategy, case_sensitive, target_pattern, secondary_pattern, notes) VALUES (?, 'File', ?, ?, NULL, ?, 'FirstMatch', 0, ?, NULL, NULL)", (check_id, str(check.get('relative_path_site1') or ''), str(check.get('relative_path_site2') or ''), str(check.get('parser_type') or 'RawText'), str(check.get('target_pattern') or '')))
            rule_exists = self._conn.execute("SELECT 1 FROM check_rule_config WHERE check_id = ?", (check_id,)).fetchone()
            rule_data = (str(check.get('rule_type') or 'FreshnessOnly'), check.get('operator'), check.get('threshold_min'), check.get('threshold_max'), int(check.get('expected_interval_seconds') or 60), int(check.get('stale_timeout_seconds') or 300), check_id)
            if rule_exists:
                self._conn.execute("UPDATE check_rule_config SET rule_type = ?, operator = ?, threshold_min = ?, threshold_max = ?, expected_interval_seconds = ?, stale_timeout_seconds = ? WHERE check_id = ?", rule_data)
            else:
                self._conn.execute("INSERT INTO check_rule_config(check_id, rule_type, operator, threshold_min, threshold_max, expected_interval_seconds, stale_timeout_seconds, grace_period_seconds, pass_text, fail_text) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL)", (check_id, str(check.get('rule_type') or 'FreshnessOnly'), check.get('operator'), check.get('threshold_min'), check.get('threshold_max'), int(check.get('expected_interval_seconds') or 60), int(check.get('stale_timeout_seconds') or 300)))
            imported_checks += 1

        self._conn.execute("INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ConfigImport', 'Configuration', 'Checks/Groups', ?)", (now, self._user.user_id, f'Imported {imported_groups} groups and {imported_checks} checks from {path}'))
        self._conn.commit()
        self._load_configuration_data()
        self._load_audit_data()
        self._load_data()
        QMessageBox.information(self, "Import complete", f"Imported {imported_groups} groups and {imported_checks} checks from:\n{path}")

    def _import_events_csv(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Events", "", "CSV Files (*.csv)")
        if not path:
            return
        imported = 0
        with open(path, 'r', newline='', encoding='utf-8-sig') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                event_utc = row.get('event_utc') or datetime.utcnow().isoformat()
                event_type = row.get('event_type') or 'ImportedEvent'
                check_id = self._check_id_for_label(row.get('check_label'))
                site_id = self._site_id_for_name(row.get('site_name'))
                user_id = self._user_id_for_name(row.get('user_name'))
                self._conn.execute("INSERT INTO events(event_utc, event_type, check_id, alert_instance_id, site_id, mode_name, user_id, message, detail) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)", (event_utc, event_type, check_id, site_id, None, user_id, row.get('message') or event_type, row.get('detail')))
                imported += 1
        self._conn.execute("INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'EventsImport', 'Import', 'Events', ?)", (datetime.utcnow().isoformat(), self._user.user_id, f'Imported {imported} events from {path}'))
        self._conn.commit()
        self._load_audit_data()
        self._load_data()
        QMessageBox.information(self, "Import complete", f"Imported {imported} events from:\n{path}")

    def _import_active_alerts_csv(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Active Alerts", "", "CSV Files (*.csv)")
        if not path:
            return
        imported = 0
        now = datetime.utcnow().isoformat()
        with open(path, 'r', newline='', encoding='utf-8-sig') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                check_id = self._check_id_for_label(row.get('check_label'))
                site_id = self._site_id_for_name(row.get('site_name'))
                if check_id is None or site_id is None:
                    continue
                existing = self._conn.execute("SELECT alert_instance_id FROM alert_instances WHERE check_id = ? AND site_id = ? AND is_active = 1", (check_id, site_id)).fetchone()
                if existing:
                    continue
                ack_user_id = self._user_id_for_name(row.get('acknowledged_by'))
                is_ack = 1 if str(row.get('is_acknowledged') or '').strip().lower() in {'1', 'true', 'yes', 'y'} else 0
                start_utc = row.get('start_utc') or now
                mode_name = row.get('mode_name') or self._settings_repo.get_setting('active_mode', 'Test')
                alert_type = row.get('alert_type') or 'ImportedActiveAlert'
                self._conn.execute("INSERT INTO alert_instances(check_id, site_id, mode_name, alert_type, start_utc, is_active, is_acknowledged, acknowledged_by_user_id, acknowledged_utc, acknowledgment_note, start_message) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)", (check_id, site_id, mode_name, alert_type, start_utc, is_ack, ack_user_id, row.get('acknowledged_utc') if is_ack else None, 'Imported from CSV', row.get('start_message') or f'Imported active alert for {row.get("check_label") or check_id}'))
                alert_id = int(self._conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                prior = self._conn.execute("SELECT * FROM current_check_status WHERE check_id = ?", (check_id,)).fetchone()
                operational_state = 'Healthy'
                if str(alert_type).lower() not in {'none', 'healthy', 'cleared'}:
                    operational_state = 'Unhealthy' if 'stale' not in str(alert_type).lower() else 'Stale'
                alert_state = 'ActiveAcknowledged' if is_ack else 'ActiveUnacknowledged'
                if prior:
                    self._conn.execute("UPDATE current_check_status SET site_id = ?, mode_name = ?, operational_state = ?, alert_state = ?, is_acknowledged = ?, active_alert_instance_id = ?, updated_utc = ?, last_detail_message = ? WHERE check_id = ?", (site_id, mode_name, operational_state, alert_state, is_ack, alert_id, now, row.get('start_message') or 'Imported active alert', check_id))
                else:
                    self._conn.execute("INSERT INTO current_check_status(check_id, site_id, mode_name, operational_state, alert_state, is_acknowledged, active_alert_instance_id, last_result_utc, last_source_modified_utc, last_source_age_seconds, last_parse_status, last_value_text, last_value_numeric, last_detail_message, updated_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'Imported', NULL, NULL, ?, ?)", (check_id, site_id, mode_name, operational_state, alert_state, is_ack, alert_id, start_utc, row.get('start_message') or 'Imported active alert', now))
                self._conn.execute("INSERT INTO events(event_utc, event_type, check_id, alert_instance_id, site_id, mode_name, user_id, message, detail) VALUES (?, 'AlertImported', ?, ?, ?, ?, ?, ?, ?)", (now, check_id, alert_id, site_id, mode_name, self._user.user_id, f'Active alert imported for {row.get("check_label") or check_id}', row.get('start_message')))
                imported += 1
        self._conn.execute("INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ActiveAlertsImport', 'Import', 'ActiveAlerts', ?)", (now, self._user.user_id, f'Imported {imported} active alerts from {path}'))
        self._conn.commit()
        self._load_audit_data()
        self._load_data()
        QMessageBox.information(self, "Import complete", f"Imported {imported} active alerts from:\n{path}")

    def _export_configuration_json(self) -> None:
        payload = {
            "exported_utc": datetime.utcnow().isoformat(),
            "groups": [dict(r) for r in self._config_repo.list_groups()],
            "checks": [dict(r) for r in self._config_repo.list_checks()],
        }
        default_name = f"opsmonitor_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        path, _ = QFileDialog.getSaveFileName(self, "Export Configuration", default_name, "JSON Files (*.json)")
        if not path:
            return
        with open(path, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, indent=2, default=str)
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ConfigExport', 'Configuration', 'Checks/Groups', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Configuration exported to {path}')
        )
        self._conn.commit()
        QMessageBox.information(self, "Export complete", f"Configuration exported to:\n{path}")
        self._load_audit_data()

    def _export_events_csv(self) -> None:
        default_name = f"opsmonitor_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path, _ = QFileDialog.getSaveFileName(self, "Export Events", default_name, "CSV Files (*.csv)")
        if not path:
            return
        rows = self._repo.get_recent_events(500)
        with open(path, 'w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(["event_utc", "event_type", "check_label", "site_name", "user_name", "message", "detail"])
            for row in rows:
                writer.writerow([row['event_utc'], row['event_type'], row['check_label'], row['site_name'], row['user_name'], row['message'], row['detail']])
        QMessageBox.information(self, "Export complete", f"Events exported to:\n{path}")

    def _scope_options(self) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
        groups = [(int(r["group_id"]), str(r["display_label"])) for r in self._config_repo.list_groups()]
        checks = [(int(r["check_id"]), str(r["display_label"])) for r in self._config_repo.list_checks()]
        return groups, checks

    def _manage_notification_rules(self) -> None:
        groups, checks = self._scope_options()
        rows = self._monitoring_repo.list_notification_rules()
        dialog = QDialog(self)
        dialog.setWindowTitle("Notification Rules")
        dialog.resize(900, 420)
        layout = QVBoxLayout(dialog)
        buttons = QHBoxLayout()
        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete")
        table = QTableWidget(0, 7)
        table.setHorizontalHeaderLabels(["Name", "Scope", "Scope Value", "Trigger", "Channel", "Destination", "Enabled"])
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        for btn in [add_btn, edit_btn, delete_btn]:
            buttons.addWidget(btn)
        buttons.addStretch()
        layout.addLayout(buttons)
        layout.addWidget(table, 1)

        label_map = {gid: name for gid, name in groups}
        label_map.update({cid: name for cid, name in checks})

        def load_table() -> None:
            table.setRowCount(0)
            for row in self._monitoring_repo.list_notification_rules():
                ridx = table.rowCount()
                table.insertRow(ridx)
                table.setVerticalHeaderItem(ridx, QTableWidgetItem(str(row['notification_rule_id'])))
                values = [
                    str(row['rule_name']),
                    str(row['scope_type']),
                    label_map.get(row['scope_value_id'], ''),
                    str(row['trigger_event']),
                    str(row['channel_type']),
                    str(row['destination'] or ''),
                    'Yes' if int(row['is_enabled'] or 0) else 'No',
                ]
                for col, value in enumerate(values):
                    table.setItem(ridx, col, QTableWidgetItem(value))
            table.resizeColumnsToContents()

        def selected_rule_id() -> int | None:
            row = table.currentRow()
            if row < 0:
                return None
            header = table.verticalHeaderItem(row)
            return int(header.text()) if header else None

        def add_rule() -> None:
            dlg = NotificationRuleDialog(dialog, groups=groups, checks=checks)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._monitoring_repo.create_notification_rule(self._user.user_id, dlg.get_data())
                load_table()

        def edit_rule() -> None:
            rule_id = selected_rule_id()
            if rule_id is None:
                return
            row = next((r for r in self._monitoring_repo.list_notification_rules() if int(r['notification_rule_id']) == rule_id), None)
            if row is None:
                return
            dlg = NotificationRuleDialog(dialog, dict(row), groups=groups, checks=checks)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._monitoring_repo.update_notification_rule(rule_id, self._user.user_id, dlg.get_data())
                load_table()

        def delete_rule() -> None:
            rule_id = selected_rule_id()
            if rule_id is None:
                return
            self._monitoring_repo.delete_notification_rule(rule_id)
            load_table()

        add_btn.clicked.connect(add_rule)
        edit_btn.clicked.connect(edit_rule)
        delete_btn.clicked.connect(delete_rule)
        load_table()
        dialog.exec()

    def _manage_maintenance_windows(self) -> None:
        groups, checks = self._scope_options()
        dialog = QDialog(self)
        dialog.setWindowTitle("Maintenance Windows")
        dialog.resize(980, 440)
        layout = QVBoxLayout(dialog)
        buttons = QHBoxLayout()
        add_btn = QPushButton("Add")
        edit_btn = QPushButton("Edit")
        delete_btn = QPushButton("Delete")
        table = QTableWidget(0, 7)
        table.setHorizontalHeaderLabels(["Name", "Scope", "Scope Value", "Start", "End", "Reason", "Enabled"])
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        for btn in [add_btn, edit_btn, delete_btn]:
            buttons.addWidget(btn)
        buttons.addStretch()
        layout.addLayout(buttons)
        layout.addWidget(table, 1)
        label_map = {gid: name for gid, name in groups}
        label_map.update({cid: name for cid, name in checks})

        def load_table() -> None:
            table.setRowCount(0)
            for row in self._monitoring_repo.list_maintenance_windows():
                ridx = table.rowCount()
                table.insertRow(ridx)
                table.setVerticalHeaderItem(ridx, QTableWidgetItem(str(row['maintenance_window_id'])))
                values = [
                    str(row['window_name']),
                    str(row['scope_type']),
                    label_map.get(row['scope_value_id'], ''),
                    str(row['start_utc']),
                    str(row['end_utc']),
                    str(row['reason'] or ''),
                    'Yes' if int(row['is_enabled'] or 0) else 'No',
                ]
                for col, value in enumerate(values):
                    table.setItem(ridx, col, QTableWidgetItem(value))
            table.resizeColumnsToContents()

        def selected_window_id() -> int | None:
            row = table.currentRow()
            if row < 0:
                return None
            header = table.verticalHeaderItem(row)
            return int(header.text()) if header else None

        def add_window() -> None:
            dlg = MaintenanceWindowDialog(dialog, groups=groups, checks=checks)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._monitoring_repo.create_maintenance_window(self._user.user_id, dlg.get_data())
                load_table()

        def edit_window() -> None:
            window_id = selected_window_id()
            if window_id is None:
                return
            row = next((r for r in self._monitoring_repo.list_maintenance_windows() if int(r['maintenance_window_id']) == window_id), None)
            if row is None:
                return
            dlg = MaintenanceWindowDialog(dialog, dict(row), groups=groups, checks=checks)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                self._monitoring_repo.update_maintenance_window(window_id, self._user.user_id, dlg.get_data())
                load_table()

        def delete_window() -> None:
            window_id = selected_window_id()
            if window_id is None:
                return
            self._monitoring_repo.delete_maintenance_window(window_id)
            load_table()

        add_btn.clicked.connect(add_window)
        edit_btn.clicked.connect(edit_window)
        delete_btn.clicked.connect(delete_window)
        load_table()
        dialog.exec()

    def _open_notifications_log(self) -> None:
        path = self._settings_repo.get_setting('notification_log_path', 'Logs/notifications.log')
        target = resolve_app_path(path)
        if target.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))
        else:
            QMessageBox.information(self, 'Notifications Log', f'Log file not found yet:\n{target}')

    def _open_logs_folder(self) -> None:
        logs_dir = self._resolve_runtime_path('logs')
        logs_dir.mkdir(parents=True, exist_ok=True)
        try:
            import os
            os.startfile(str(logs_dir))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to open logs folder")
            QMessageBox.warning(self, "Open Logs Folder", f"Could not open logs folder:\n{exc}")

    def _export_active_alerts_csv(self) -> None:
        default_name = f"opsmonitor_active_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path, _ = QFileDialog.getSaveFileName(self, "Export Active Alerts", default_name, "CSV Files (*.csv)")
        if not path:
            return
        rows = self._conn.execute(
            """
            SELECT c.display_label AS check_label,
                   s.site_name,
                   ai.mode_name,
                   ai.alert_type,
                   ai.start_utc,
                   ai.is_acknowledged,
                   u.display_name AS acknowledged_by,
                   ai.acknowledged_utc,
                   ai.start_message
            FROM alert_instances ai
            JOIN checks c ON c.check_id = ai.check_id
            JOIN sites s ON s.site_id = ai.site_id
            LEFT JOIN users u ON u.user_id = ai.acknowledged_by_user_id
            WHERE ai.is_active = 1
            ORDER BY ai.start_utc DESC
            """
        ).fetchall()
        with open(path, 'w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "check_label",
                "site_name",
                "mode_name",
                "alert_type",
                "start_utc",
                "is_acknowledged",
                "acknowledged_by",
                "acknowledged_utc",
                "start_message",
            ])
            for row in rows:
                writer.writerow([
                    row["check_label"],
                    row["site_name"],
                    row["mode_name"],
                    row["alert_type"],
                    row["start_utc"],
                    "Yes" if int(row["is_acknowledged"] or 0) else "No",
                    row["acknowledged_by"] or "",
                    row["acknowledged_utc"] or "",
                    row["start_message"],
                ])
        self._conn.execute(
            "INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_name, message) VALUES (?, ?, 'ActiveAlertsExport', 'Export', 'ActiveAlerts', ?)",
            (datetime.utcnow().isoformat(), self._user.user_id, f'Active alerts exported to {path}')
        )
        self._conn.commit()
        QMessageBox.information(self, "Export complete", f"Active alerts exported to:\n{path}")
        self._load_audit_data()


    def _delete_selected_check(self) -> None:
        row = self._checks_table.currentRow()
        if row < 0 and self._selected_config_check_id is None:
            QMessageBox.information(self, "Delete Check", "Select a check first.")
            return
        item = self._checks_table.item(row, 0) if row >= 0 else None
        data = item.data(Qt.ItemDataRole.UserRole) if item is not None else None
        check_id = None
        if isinstance(data, dict):
            check_id = data.get("check_id")
        elif data is not None:
            check_id = data
        elif self._selected_config_check_id is not None:
            check_id = self._selected_config_check_id
        label = item.text() if item is not None and item.text() else "selected check"
        if check_id is None:
            QMessageBox.warning(self, "Delete Check", "Unable to determine which check to delete.")
            return
        answer = QMessageBox.question(
            self,
            "Delete Check",
            f"Move '{label}' to the recycle bin? Historical data will remain untouched.",
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            self._config_repo.delete_check(int(check_id), self._user.user_id)
            if self._selected_check_id == int(check_id):
                self._selected_check_id = None
            self._load_configuration_data()
            self._load_data()
            self._load_audit_data()
            QMessageBox.information(self, "Delete Check", f"Moved '{label}' to the recycle bin.")
        except Exception as exc:
            logger.exception("Failed to delete check")
            QMessageBox.critical(self, "Delete Check", f"Failed to delete check.\n\n{exc}")

    def _duplicate_selected_check(self) -> None:
        if not self._can_manage_config() or self._selected_config_check_id is None:
            return
        try:
            new_id = self._config_repo.duplicate_check(self._selected_config_check_id, self._user.user_id)
            self._selected_config_check_id = new_id
            self._load_configuration_data()
            self._run_cycle_and_refresh()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Duplicate check", str(exc))

    def _new_group(self) -> None:
        if not self._can_manage_config():
            return
        dialog = GroupDialog(self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        try:
            self._config_repo.create_group(self._user.user_id, dialog.get_data())
            self._load_configuration_data()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Save group", str(exc))

    def _edit_selected_group(self) -> None:
        if not self._can_manage_config() or self._selected_group_id is None:
            return
        row = next((dict(r) for r in self._config_repo.list_groups() if int(r["group_id"]) == self._selected_group_id), None)
        if row is None:
            return
        dialog = GroupDialog(self, row)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        try:
            self._config_repo.update_group(self._selected_group_id, self._user.user_id, dialog.get_data())
            self._load_configuration_data()
            self._load_data()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Update group", str(exc))

    def _new_check(self) -> None:
        if not self._can_manage_config():
            return
        dialog = CheckDialog(self._config_repo.list_group_options(), self._settings_repo.get_setting("test_data_root_path", "TestData"), self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        try:
            self._config_repo.create_check(self._user.user_id, dialog.get_data())
            self._run_cycle_and_refresh()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Create check", str(exc))

    def _edit_selected_check(self) -> None:
        if not self._can_manage_config() or self._selected_config_check_id is None:
            return
        check_id = int(self._selected_config_check_id)
        row = self._config_repo.get_check(check_id)
        if row is None:
            return
        dialog = CheckDialog(self._config_repo.list_group_options(), self._settings_repo.get_setting("test_data_root_path", "TestData"), self, dict(row))
        if dialog.exec() != dialog.DialogCode.Accepted:
            return
        try:
            self._config_repo.update_check(check_id, self._user.user_id, dialog.get_data())
            self._selected_config_check_id = check_id
            self._run_cycle_and_refresh()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Update check", str(exc))

    def _toggle_selected_check(self) -> None:
        if not self._can_manage_config() or self._selected_config_check_id is None:
            return
        try:
            self._config_repo.toggle_check_enabled(self._selected_config_check_id, self._user.user_id)
            self._run_cycle_and_refresh()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Toggle check", str(exc))

    def _on_config_check_selection_changed(self) -> None:
        row = self._checks_table.currentRow()
        if row < 0:
            self._selected_config_check_id = None
            return
        item = self._checks_table.item(row, 0)
        data = item.data(Qt.ItemDataRole.UserRole) if item is not None else None
        self._selected_config_check_id = int(data) if data is not None else None

    def _on_group_selection_changed(self) -> None:
        items = self._groups_table.selectedItems()
        self._selected_group_id = int(items[0].data(Qt.ItemDataRole.UserRole)) if items else None

    def _on_deleted_check_selection_changed(self) -> None:
        items = self._deleted_checks_table.selectedItems() if hasattr(self, "_deleted_checks_table") else []
        self._selected_deleted_check_id = int(items[0].data(Qt.ItemDataRole.UserRole)) if items else None

    def _on_deleted_group_selection_changed(self) -> None:
        items = self._deleted_groups_table.selectedItems() if hasattr(self, "_deleted_groups_table") else []
        self._selected_deleted_group_id = int(items[0].data(Qt.ItemDataRole.UserRole)) if items else None

    def _delete_selected_group(self) -> None:
        if not self._can_manage_config() or self._selected_group_id is None:
            QMessageBox.information(self, "Delete Group", "Select a group first.")
            return
        active_groups = [r for r in self._config_repo.list_groups() if int(r["group_id"]) != int(self._selected_group_id)]
        if active_groups:
            move_answer = QMessageBox.question(self, "Delete Group", "Move checks to the first available active group and delete this group?\n\nChoose No to move the group and all of its checks to the recycle bin.")
            if move_answer == QMessageBox.StandardButton.Yes:
                destination_group_id = int(active_groups[0]["group_id"])
                self._config_repo.delete_group(self._selected_group_id, self._user.user_id, "move", destination_group_id)
            else:
                confirm = QMessageBox.question(self, "Delete Group", "Delete this group and move all checks in it to the recycle bin?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if confirm != QMessageBox.StandardButton.Yes:
                    return
                self._config_repo.delete_group(self._selected_group_id, self._user.user_id, "delete_all")
        else:
            confirm = QMessageBox.question(self, "Delete Group", "No alternate active group exists. Delete this group and move all checks in it to the recycle bin?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if confirm != QMessageBox.StandardButton.Yes:
                return
            self._config_repo.delete_group(self._selected_group_id, self._user.user_id, "delete_all")
        self._selected_group_id = None
        self._load_configuration_data()
        self._load_data()
        self._load_audit_data()

    def _restore_selected_check(self) -> None:
        if self._selected_deleted_check_id is None:
            QMessageBox.information(self, "Restore Check", "Select a deleted check first.")
            return
        self._config_repo.restore_check(self._selected_deleted_check_id, self._user.user_id)
        self._load_configuration_data()
        self._load_data()
        self._load_audit_data()

    def _retire_selected_check(self) -> None:
        if self._selected_deleted_check_id is None:
            QMessageBox.information(self, "Permanent Delete", "Select a deleted check first.")
            return
        confirm = QMessageBox.question(self, "Permanent Delete", "Permanently remove this check from the recycle bin? Historical records will be preserved.")
        if confirm != QMessageBox.StandardButton.Yes:
            return
        self._config_repo.retire_check(self._selected_deleted_check_id, self._user.user_id)
        self._selected_deleted_check_id = None
        self._load_configuration_data()
        self._load_data()
        self._load_audit_data()

    def _restore_selected_group(self) -> None:
        if self._selected_deleted_group_id is None:
            QMessageBox.information(self, "Restore Group", "Select a deleted group first.")
            return
        self._config_repo.restore_group(self._selected_deleted_group_id, self._user.user_id)
        self._load_configuration_data()
        self._load_data()
        self._load_audit_data()

    def _retire_selected_group(self) -> None:
        if self._selected_deleted_group_id is None:
            QMessageBox.information(self, "Permanent Delete", "Select a deleted group first.")
            return
        confirm = QMessageBox.question(self, "Permanent Delete", "Permanently remove this group from the recycle bin? Historical records will be preserved.")
        if confirm != QMessageBox.StandardButton.Yes:
            return
        self._config_repo.retire_group(self._selected_deleted_group_id, self._user.user_id)
        self._selected_deleted_group_id = None
        self._load_configuration_data()
        self._load_data()
        self._load_audit_data()

    def _on_audit_selection_changed(self) -> None:
        if not hasattr(self, "_audit_table"):
            return
        row = self._audit_table.currentRow()
        if row < 0:
            self._selected_audit_key = None
            return
        item = self._audit_table.item(row, 0)
        data = item.data(Qt.ItemDataRole.UserRole) if item is not None else None
        self._selected_audit_key = data if isinstance(data, tuple) and len(data) == 4 else None

    def _select_check(self, check_id: int) -> None:
        self._selected_check_id = check_id
        self._apply_tile_focus_state()
        self._update_incident_action_state()

    def _open_check_details(self, check_id: int) -> None:
        self._selected_check_id = check_id
        self._apply_tile_focus_state()
        self._render_monitoring_pane("details", check_id)
        self._update_incident_action_state()

    def _open_check_history(self, check_id: int) -> None:
        self._selected_check_id = check_id
        self._apply_tile_focus_state()
        self._render_monitoring_pane("history", check_id)
        self._update_incident_action_state()

    def _open_check_graph(self, check_id: int) -> None:
        self._selected_check_id = check_id
        self._apply_tile_focus_state()
        self._render_monitoring_pane("graph", check_id)
        self._update_incident_action_state()

    def _switch_monitoring_pane_mode(self, mode: str) -> None:
        if self._selected_check_id is None:
            return
        self._render_monitoring_pane(mode, self._selected_check_id)

    def _render_monitoring_pane(self, mode: str, check_id: int | None, make_visible: bool = True) -> None:
        self._detail_mode = mode
        if check_id is not None:
            self._selected_check_id = check_id
        self._detail_action_active = self._selected_check_id is not None
        if make_visible and self._detail_action_active:
            self._expand_details_panel()
        self._selected_header.setVisible(False)
        self._detail_mode_bar.setVisible(self._details_panel_expanded and self._detail_action_active)
        for key, button in getattr(self, '_detail_mode_buttons', {}).items():
            button.blockSignals(True)
            button.setChecked(key == mode and self._detail_action_active)
            button.blockSignals(False)
        if self._selected_check_id is None or not self._detail_action_active:
            self._status_card.setVisible(False)
            self._info_card.setVisible(False)
            self._detail_tabs.setVisible(False)
            if hasattr(self, '_recent_activity_title'):
                self._recent_activity_title.setVisible(False)
            return
        if mode == 'details':
            self._status_card.setVisible(True)
            self._info_card.setVisible(True)
            self._detail_tabs.setVisible(False)
            if hasattr(self, '_recent_activity_title'):
                self._recent_activity_title.setVisible(False)
            self._load_selected_check_details()
        elif mode == 'history':
            self._status_card.setVisible(False)
            self._info_card.setVisible(False)
            if hasattr(self, '_recent_activity_title'):
                self._recent_activity_title.setVisible(False)
            self._detail_tabs.setVisible(True)
            self._detail_tabs.setCurrentWidget(self._events_tab)
            self._load_history_activity_panel()
        else:
            self._status_card.setVisible(False)
            self._info_card.setVisible(False)
            if hasattr(self, '_recent_activity_title'):
                self._recent_activity_title.setVisible(False)
            self._detail_tabs.setVisible(True)
            self._detail_tabs.setCurrentWidget(self._history_tab)
            self._load_history_tab()
            self._graph_type_combo.setFocus()

    def _select_incident_check(self, check_id: int, _event=None) -> None:
        self._select_check(check_id)
        self._update_incident_action_state()

    def _update_incident_action_state(self) -> None:
        if not hasattr(self, "_incident_selected_check_label"):
            return
        row = self._repo.get_check_details(self._selected_check_id) if self._selected_check_id is not None else None
        if row is None:
            self._incident_selected_check_label.setText("No impacted check selected")
            self._incident_mode_escalate_button.setEnabled(False)
            self._incident_mode_salesforce_button.setEnabled(False)
            return
        label = str(row["display_label"] or "Selected check")
        state = self._format_combined_state(row)
        self._incident_selected_check_label.setText(f"Selected: {label} — {state}")
        alert_state = str(row["alert_state"] or "")
        is_escalated = int(row["is_escalated"] or 0) == 1
        can_escalate = alert_state in {"ActiveUnacknowledged", "ActiveAcknowledged"} and self._can_ack() and not is_escalated
        show_salesforce = alert_state in {"ActiveUnacknowledged", "ActiveAcknowledged"} or self._selected_check_has_recent_alert_context()
        self._incident_mode_escalate_button.setEnabled(can_escalate)
        self._incident_mode_salesforce_button.setEnabled(show_salesforce)

    def _ack_check(self, check_id: int) -> None:
        if not self._can_ack():
            QMessageBox.warning(self, "Not authorized", "Your role cannot acknowledge alerts.")
            return
        dialog = AcknowledgeDialog(self)
        note = None
        if dialog.exec() == QDialog.DialogCode.Accepted:
            note = dialog.note() or None
        else:
            return
        success = self._monitoring_repo.acknowledge_alert(check_id, self._user.user_id, note)
        if not success:
            QMessageBox.information(self, "Acknowledge", "This alert is already acknowledged or no active alert exists.")
        self._selected_check_id = check_id
        self._load_data()

    def _ack_selected_alert(self) -> None:
        if self._selected_check_id is not None:
            self._ack_check(self._selected_check_id)

    def _build_event_card(self, row) -> QFrame:
        frame = QFrame()
        frame.setObjectName("eventCard")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(2)
        top = QLabel(f"{self._format_time(str(row['event_utc']))} • {row['event_type']}")
        top.setObjectName("tileMeta")
        body_parts = [str(row["message"])]
        if row["check_label"]:
            body_parts.insert(0, str(row["check_label"]))
        if row["user_name"]:
            body_parts.append(f"By: {row['user_name']}")
        if row["detail"]:
            body_parts.append(str(row["detail"]))
        body = QLabel("\n".join(body_parts))
        body.setWordWrap(True)
        layout.addWidget(top)
        layout.addWidget(body)
        return frame

    def _export_history_csv(self) -> None:
        if self._selected_check_id is None:
            QMessageBox.information(self, "Export", "Select a check first.")
            return
        details = self._repo.get_check_details(self._selected_check_id)
        history_rows = self._repo.get_check_history_for_range(self._selected_check_id, self._history_range)
        safe_name = str(details["display_label"]).replace(" ", "_")
        default_name = f"{safe_name}_{self._history_range}.csv"
        path, _ = QFileDialog.getSaveFileName(self, "Export History CSV", default_name, "CSV Files (*.csv)")
        if not path:
            return
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["evaluated_utc", "operational_state", "value_text", "value_numeric", "detail_message"])
            for row in history_rows:
                writer.writerow([row["evaluated_utc"], row["operational_state"], row["value_text"], row["value_numeric"], row["detail_message"]])
        QMessageBox.information(self, "Export complete", f"History exported to:\n{path}")

    def _export_history_png(self) -> None:
        if self._selected_check_id is None:
            QMessageBox.information(self, "Export", "Select a check first.")
            return
        details = self._repo.get_check_details(self._selected_check_id)
        safe_name = str(details["display_label"]).replace(" ", "_")
        default_name = f"{safe_name}_{self._history_range}.png"
        path, _ = QFileDialog.getSaveFileName(self, "Export History PNG", default_name, "PNG Files (*.png)")
        if not path:
            return
        pixmap = self._history_chart.grab()
        pixmap.save(path, "PNG")
        QMessageBox.information(self, "Export complete", f"Graph exported to:\n{path}")

    def _reset_history_zoom(self) -> None:
        self._history_chart.reset_zoom()
        self._reset_zoom_button.setEnabled(self._history_chart.can_zoom())

    def _on_history_zoom_state_changed(self, is_zoomed: bool) -> None:
        self._reset_zoom_button.setEnabled(is_zoomed)

    def _on_detail_tab_changed(self, index: int) -> None:
        return

    def _open_large_graph(self) -> None:
        if self._selected_check_id is None or not self._history_rows_cache:
            QMessageBox.information(self, "Large Graph", "Select a check with history first.")
            return
        details = self._repo.get_check_details(self._selected_check_id)
        threshold_min = float(details['threshold_min']) if details['threshold_min'] is not None else None
        threshold_max = float(details['threshold_max']) if details['threshold_max'] is not None else None
        theme_name = self._settings_repo.get_setting("current_theme", "Charcoal Blue")
        theme = self._theme_service.get_theme_tokens(theme_name)
        dialog = GraphDialog(
            str(details['display_label']),
            self._history_rows_cache,
            threshold_min,
            threshold_max,
            self._graph_type_combo.currentText(),
            theme,
            self,
        )
        dialog.exec()

    def _escalate_selected_alert(self) -> None:
        if self._selected_check_id is None:
            return
        if self._monitoring_repo.escalate_alert(self._selected_check_id, self._user.user_id):
            self._run_cycle_and_refresh()
            QMessageBox.information(self, "Escalated", "Alert escalated to support engineer and auto-acknowledged.")
        else:
            QMessageBox.information(self, "Escalate", "No active alert is available to escalate.")

    def _prepare_salesforce_ticket(self) -> None:
        try:
            if self._selected_check_id is None:
                return
            row = self._repo.get_check_details(self._selected_check_id)
            if row is None:
                QMessageBox.information(self, "Prepare Salesforce Ticket", "No check details are available for the selected alert.")
                return
            site_id = self._settings_repo.get_setting("active_site_id", "1")
            site_name = "Site 1" if site_id == "1" else "Site 2"
            env_name = self._settings_repo.get_setting("active_mode", "Test")
            current_value = row["last_value_numeric"] if row["last_value_numeric"] is not None else (row["last_value_text"] or "(none)")
            events = self._repo.get_check_recent_events(self._selected_check_id, 5)
            recent_events = self._repo.get_check_recent_events(self._selected_check_id, 12)
            recent_alert_start = next((event for event in recent_events if str(event['event_type']) == 'AlertStarted'), None)
            recent_alert_cleared = next((event for event in recent_events if str(event['event_type']) == 'AlertCleared'), None)
            recent_ack = next((event for event in recent_events if str(event['event_type']) == 'AlertAcknowledged'), None)
            lines = [
                f"{row['display_label']} alert on {site_name}",
                "",
                "Priority: High",
                f"Environment: {env_name}",
                "",
                f"Check: {row['display_label']}",
                f"Current State: {row['operational_state']}",
                f"Current Value: {current_value}",
                f"Alert Start: {self._format_time(str(row['alert_start_utc'])) if row['alert_start_utc'] else (self._format_time(str(recent_alert_start['event_utc'])) if recent_alert_start else '(unknown)')}",
                f"Acknowledged: {'Yes' if int(row['is_acknowledged'] or 0) == 1 or recent_ack else 'No'}",
            ]
            if recent_alert_cleared is not None and str(row['alert_state'] or '') not in {'ActiveUnacknowledged', 'ActiveAcknowledged'}:
                lines.append(f"Resolution Time: {self._format_time(str(recent_alert_cleared['event_utc']))}")
                lines.append("Current Incident Status: Resolved / monitoring")
            lines.extend([
                "",
                f"Summary: {row['last_detail_message'] or row['description'] or ''}",
                "",
                "Recent Events:",
            ])
            events = recent_events[:5]
            for event in events:
                who = f" by {event['user_name']}" if event['user_name'] else ""
                lines.append(f"{self._format_time(str(event['event_utc']))}  {event['event_type']}{who}")
            dialog = TicketPreviewDialog("Prepare Salesforce Ticket", "\n".join(lines), self)
            dialog.exec()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Prepare Salesforce Ticket", f"Could not generate Salesforce ticket preview:\n{exc}")


    def _on_graph_type_changed(self, _value: str) -> None:
        self._load_history_tab()

    def _open_troubleshooting_doc(self, tier: int) -> None:
        if self._selected_check_id is None:
            return
        row = self._repo.get_check_details(self._selected_check_id)
        if row is None:
            return
        url = row["troubleshooting_tier1_url"] if tier == 1 else row["troubleshooting_tier2_url"]
        if url:
            QDesktopServices.openUrl(QUrl(str(url)))

    def _can_ack(self) -> bool:
        return self._effective_role_name() in {"Operator", "ConfigAdmin", "SystemAdmin"}

    def _can_manage_config(self) -> bool:
        return self._effective_role_name() in {"ConfigAdmin", "SystemAdmin"}

    def _can_switch_site(self) -> bool:
        return self._effective_role_name() in {"Operator", "ConfigAdmin", "SystemAdmin"}

    def _can_manage_environment(self) -> bool:
        return self._effective_role_name() in {"ConfigAdmin", "SystemAdmin"}

    @staticmethod
    def _clear_layout(layout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            child_layout = item.layout()
            if widget is not None:
                widget.deleteLater()
            elif child_layout is not None:
                MainWindow._clear_layout(child_layout)

    @staticmethod
    def _format_time(value: str) -> str:
        try:
            normalized = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            try:
                eastern = ZoneInfo("America/New_York")
                return dt.astimezone(eastern).strftime("%Y-%m-%d %H:%M:%S ET")
            except ZoneInfoNotFoundError:
                # Fallback when tzdata is not bundled in the packaged app.
                # Use a fixed UTC-5 offset so the app still opens instead of crashing.
                fallback_eastern = timezone(timedelta(hours=-5), name="ET")
                return dt.astimezone(fallback_eastern).strftime("%Y-%m-%d %H:%M:%S ET")
        except ValueError:
            return value

    @staticmethod
    def _table_item(text: str, user_data: int | None = None) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        if user_data is not None:
            item.setData(Qt.ItemDataRole.UserRole, user_data)
        return item
