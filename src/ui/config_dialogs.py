from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from pathlib import Path

from src.data.config_repository import GroupOption
from src.data.monitoring_repository import MonitoringCheckConfig
from src.engine.monitoring_engine import MonitoringEngine
from src.engine.parser_engine import ParserEngine



PARSER_HELP: dict[str, dict[str, str]] = {
    "RawText": {
        "summary": "Reads the file as plain text and lets rules search the content directly.",
        "example": "Example:\nFile line: ERROR connection lost\nUse with TextExists or TextNotExists.",
    },
    "AnsiFormattedText": {
        "summary": "Looks for shell-style ANSI formatted text, such as color-coded output like ^[[1;;37m 1^[[0m.",
        "example": "Example:\nMatch full sequence ^[[1;;37m 1^[[0m\nor match color code 1;;37.",
    },
    "IntegerFromPattern": {
        "summary": "Finds a target pattern and extracts the next whole number after it.",
        "example": "Example:\nPattern: Count:\nLine: Count: 17\nResult: 17",
    },
    "DecimalFromPattern": {
        "summary": "Finds a target pattern and extracts the next decimal number after it.",
        "example": "Example:\nPattern: Value:\nLine: Value: 13.42\nResult: 13.42",
    },
    "FreshnessOnly": {
        "summary": "Ignores file contents and only checks whether the file exists and updates within the allowed stale timeout.",
        "example": "Example:\nUse for heartbeat files or transferred logs where freshness matters more than content.",
    },
}

RULE_HELP: dict[str, dict[str, str]] = {
    "TextExists": {
        "summary": "Passes when the target text is found in the parser output or raw text.",
        "example": "Example:\nTarget: ERROR\nHealthy/alert result depends on your rule meaning.",
    },
    "TextNotExists": {
        "summary": "Passes when the target text is not found.",
        "example": "Example:\nTarget: CRITICAL\nUseful when a word should never appear.",
    },
    "NumericCompare": {
        "summary": "Compares one extracted number to a threshold using an operator such as >, >=, <, <=, or ==.",
        "example": "Example:\nValue 12 > 8 => alert",
    },
    "NumericRange": {
        "summary": "Checks whether an extracted number stays between a minimum and maximum.",
        "example": "Example:\nAllowed range 0 to 8\nValue 12 => out of range",
    },
    "FreshnessOnly": {
        "summary": "Passes when the file is updating within the configured stale timeout.",
        "example": "Example:\nIf file update age exceeds stale timeout, the check becomes stale.",
    },
}

class GroupDialog(QDialog):
    def __init__(self, parent: QWidget | None = None, initial: dict[str, Any] | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Group")
        self.resize(420, 240)
        initial = initial or {}

        root = QVBoxLayout(self)
        form = QFormLayout()
        root.addLayout(form)

        self.group_name = QLineEdit(str(initial.get("group_name", "")))
        self.display_label = QLineEdit(str(initial.get("display_label", "")))
        self.display_order = QSpinBox()
        self.display_order.setMaximum(9999)
        self.display_order.setValue(int(initial.get("display_order", 0) or 0))
        self.description = QPlainTextEdit(str(initial.get("description", "")))
        self.description.setFixedHeight(80)
        self.is_enabled = QCheckBox("Enabled")
        self.is_enabled.setChecked(bool(int(initial.get("is_enabled", 1) or 1)))

        form.addRow("Internal group name", self.group_name)
        form.addRow("Display label", self.display_label)
        form.addRow("Display order", self.display_order)
        form.addRow("Description", self.description)
        form.addRow("", self.is_enabled)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def get_data(self) -> dict[str, Any]:
        return {
            "group_name": self.group_name.text().strip(),
            "display_label": self.display_label.text().strip(),
            "display_order": self.display_order.value(),
            "description": self.description.toPlainText().strip() or None,
            "is_enabled": self.is_enabled.isChecked(),
        }



class CheckDialog(QDialog):
    def __init__(
        self,
        group_options: list[GroupOption],
        test_data_root: str,
        parent: QWidget | None = None,
        initial: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Check")
        self.resize(700, 760)
        initial = initial or {}
        self._group_options = group_options
        self._test_data_root = Path(test_data_root)
        self._parser_engine = ParserEngine()
        self._monitoring_engine_for_test = MonitoringEngine.__new__(MonitoringEngine)

        root = QVBoxLayout(self)
        title = QLabel("Create or edit a monitoring check")
        title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        root.addWidget(title)

        info = QLabel("Rule controls update based on the selected rule type. Use Parser Test to confirm the file, pattern, and extracted value before saving.")
        info.setWordWrap(True)
        root.addWidget(info)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root.addWidget(scroll, 1)

        content = QWidget()
        scroll.setWidget(content)
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(10)

        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        content_layout.addLayout(form)

        self.internal_name = QLineEdit(str(initial.get("internal_name", "")))
        self._is_edit_mode = bool(initial.get("check_id"))
        self.internal_name.setReadOnly(self._is_edit_mode)
        self.display_label = QLineEdit(str(initial.get("display_label", "")))
        self.description = QPlainTextEdit(str(initial.get("description", "")))
        self.description.setFixedHeight(72)

        self.group_combo = QComboBox()
        for option in group_options:
            self.group_combo.addItem(option.display_label, option.group_id)
        if initial.get("group_id"):
            idx = self.group_combo.findData(int(initial["group_id"]))
            if idx >= 0:
                self.group_combo.setCurrentIndex(idx)

        self.display_order = QSpinBox()
        self.display_order.setMaximum(9999)
        self.display_order.setValue(int(initial.get("display_order", 0) or 0))

        self.site1 = QCheckBox("Site 1")
        self.site1.setChecked(bool(int(initial.get("applies_to_site1", 1) or 1)))
        self.site2 = QCheckBox("Site 2")
        self.site2.setChecked(bool(int(initial.get("applies_to_site2", 1) or 1)))
        site_box = QWidget()
        site_layout = QHBoxLayout(site_box)
        site_layout.setContentsMargins(0, 0, 0, 0)
        site_layout.addWidget(self.site1)
        site_layout.addWidget(self.site2)
        site_layout.addStretch()

        self.path1 = QLineEdit(str(initial.get("relative_path_site1", "")))
        self.path2 = QLineEdit(str(initial.get("relative_path_site2", "")))

        self.parser_type = QComboBox()
        self.parser_type.addItems(["RawText", "AnsiFormattedText", "IntegerFromPattern", "DecimalFromPattern", "FreshnessOnly", "FileMustBeBlank"])
        self.parser_type.setCurrentText(str(initial.get("parser_type", "RawText")))
        self.parser_type.currentTextChanged.connect(self._sync_parser_defaults)
        self.parser_type.currentTextChanged.connect(self._refresh_context_help)

        self.rule_type = QComboBox()
        self.rule_type.addItems(["TextExists", "TextNotExists", "NumericCompare", "NumericRange", "FreshnessOnly", "FileMustBeBlank"])
        self.rule_type.setCurrentText(str(initial.get("rule_type", "TextExists")))
        self.rule_type.currentTextChanged.connect(self._sync_rule_controls)
        self.rule_type.currentTextChanged.connect(self._refresh_context_help)

        self.target_pattern = QLineEdit(str(initial.get("target_pattern", "")))

        self.operator = QComboBox()
        self.operator.addItems([">", ">=", "<", "<=", "=="])
        op_value = str(initial.get("operator", ">"))
        idx = self.operator.findText(op_value)
        self.operator.setCurrentIndex(max(idx, 0))

        self.threshold_min = QDoubleSpinBox()
        self.threshold_min.setMaximum(999999999)
        self.threshold_min.setMinimum(-999999999)
        self.threshold_min.setDecimals(3)
        self.threshold_min.setSpecialValueText("")
        if initial.get("threshold_min") not in (None, ""):
            self.threshold_min.setValue(float(initial.get("threshold_min")))

        self.threshold_max = QDoubleSpinBox()
        self.threshold_max.setMaximum(999999999)
        self.threshold_max.setMinimum(-999999999)
        self.threshold_max.setDecimals(3)
        self.threshold_max.setSpecialValueText("")
        if initial.get("threshold_max") not in (None, ""):
            self.threshold_max.setValue(float(initial.get("threshold_max")))

        self.expected_interval = QSpinBox()
        self.expected_interval.setMaximum(86400)
        self.expected_interval.setValue(int(initial.get("expected_interval_seconds", 60) or 60))
        self.stale_timeout = QSpinBox()
        self.stale_timeout.setMaximum(86400)
        self.stale_timeout.setValue(int(initial.get("stale_timeout_seconds", 300) or 300))
        self.pass_text = QLineEdit(str(initial.get("pass_text", "")))
        self.fail_text = QLineEdit(str(initial.get("fail_text", "")))
        self.graph_type = QComboBox()
        self.graph_type.addItems(["Line", "Area", "Step Line", "State Timeline"])
        self.graph_type.setCurrentText(str(initial.get("graph_type", "Line") or "Line"))
        self.tier1_url = QLineEdit(str(initial.get("troubleshooting_tier1_url", "")))
        self.tier2_url = QLineEdit(str(initial.get("troubleshooting_tier2_url", "")))
        self.is_enabled = QCheckBox("Enabled")
        self.is_enabled.setChecked(bool(int(initial.get("is_enabled", 1) or 1)))

        self.rule_help = QLabel("")
        self.rule_help.setWordWrap(True)
        self.rule_help.setObjectName("detailCard")


        self.test_site_combo = QComboBox()
        self.test_site_combo.addItems(["Site 1", "Site 2"])
        if self.site2.isChecked() and not self.site1.isChecked():
            self.test_site_combo.setCurrentIndex(1)
        self._parser_test_button = QPushButton("Run Parser Test")
        self._parser_test_button.clicked.connect(self._run_parser_test)
        self._parser_test_result = QLabel("Parser test has not been run yet.")
        self._parser_test_result.setWordWrap(True)
        self._parser_test_result.setObjectName("detailCard")

        parser_test_row = QWidget()
        parser_test_layout = QHBoxLayout(parser_test_row)
        parser_test_layout.setContentsMargins(0, 0, 0, 0)
        parser_test_layout.addWidget(QLabel("Test against"))
        parser_test_layout.addWidget(self.test_site_combo)
        parser_test_layout.addWidget(self._parser_test_button)
        parser_test_layout.addStretch()

        form.addRow("Internal name", self.internal_name)
        form.addRow("Display label", self.display_label)
        form.addRow("Description", self.description)
        form.addRow("Group", self.group_combo)
        form.addRow("Display order", self.display_order)
        form.addRow("Sites", site_box)
        form.addRow("Site 1 path", self.path1)
        form.addRow("Site 2 path", self.path2)
        self.parser_type.setToolTip("Choose how the application reads or extracts data from the file.")
        self.rule_type.setToolTip("Choose how the extracted value or text is evaluated.")
        self._apply_combo_item_tooltips()
        form.addRow("Parser", self.parser_type)
        form.addRow("Rule", self.rule_type)
        form.addRow("Target pattern", self.target_pattern)
        form.addRow("Operator", self.operator)
        form.addRow("Threshold min", self.threshold_min)
        form.addRow("Threshold max", self.threshold_max)
        form.addRow("Expected interval (sec)", self.expected_interval)
        form.addRow("Stale timeout (sec)", self.stale_timeout)
        form.addRow("Pass text", self.pass_text)
        form.addRow("Fail text", self.fail_text)
        form.addRow("Graph type", self.graph_type)
        form.addRow("Tier 1 troubleshooting URL", self.tier1_url)
        form.addRow("Tier 2 troubleshooting URL", self.tier2_url)
        form.addRow("", self.is_enabled)

        rule_box = QFrame()
        rule_box.setObjectName("panel")
        rule_box_layout = QVBoxLayout(rule_box)
        rule_box_layout.setContentsMargins(10, 10, 10, 10)
        rule_box_layout.setSpacing(8)
        rule_box_layout.addWidget(QLabel("Rule Guidance"))
        rule_box_layout.addWidget(self.rule_help)
        rule_box_layout.addWidget(parser_test_row)
        rule_box_layout.addWidget(self._parser_test_result)
        content_layout.addWidget(rule_box)

        self._sync_parser_defaults(self.parser_type.currentText())
        self._sync_rule_controls(self.rule_type.currentText())
        self._refresh_context_help()

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)


    def _sync_parser_defaults(self, parser_type: str) -> None:
        if parser_type == "FreshnessOnly":
            self.rule_type.setCurrentText("FreshnessOnly")
            self.target_pattern.setPlaceholderText("No pattern required for freshness checks")
        elif parser_type == "FileMustBeBlank":
            self.rule_type.setCurrentText("FileMustBeBlank")
            self.target_pattern.setPlaceholderText("No pattern required for blank-file checks")
        elif parser_type == "RawText":
            self.target_pattern.setPlaceholderText('Example: ERROR or READY')
        elif parser_type == "AnsiFormattedText":
            self.target_pattern.setPlaceholderText(r'Example: ^[[1;;37m 1^[[0m or 1;;37')
        else:
            self.target_pattern.setPlaceholderText('Example: Count:')

    def _sync_rule_controls(self, rule_type: str) -> None:
        is_text_rule = rule_type in {"TextExists", "TextNotExists"}
        is_numeric_compare = rule_type == "NumericCompare"
        is_numeric_range = rule_type == "NumericRange"
        is_freshness = rule_type == "FreshnessOnly"
        is_blank_file = rule_type == "FileMustBeBlank"

        self.target_pattern.setEnabled(not is_freshness and not is_blank_file)
        self.operator.setEnabled(is_numeric_compare)

        self.threshold_min.setEnabled(is_numeric_range)
        self.threshold_max.setEnabled(is_numeric_compare or is_numeric_range)

        if is_text_rule:
            parser_type = self.parser_type.currentText()
            if parser_type == "AnsiFormattedText":
                self.rule_help.setText("ANSI text rules can search for a full shell color sequence, a color code, or an ANSI-wrapped value. Example target: ^[[1;;37m 1^[[0m or 1;;37.")
            else:
                self.rule_help.setText("Text rules use the parser output or raw text. Enter the pattern to search for. Threshold fields are not used.")
        elif is_numeric_compare:
            self.rule_help.setText("Numeric Compare uses the extracted number and compares it against the threshold max value using the selected operator.")
        elif is_numeric_range:
            self.rule_help.setText("Numeric Range checks that the extracted number is between threshold min and threshold max.")
        elif is_freshness:
            self.rule_help.setText("Freshness Only ignores file content and checks whether the file exists and updates within the stale timeout.")
            self.target_pattern.clear()
        elif is_blank_file:
            self.rule_help.setText("File Must Be Blank treats an empty or whitespace-only file as healthy. Any real content makes the check unhealthy.")
            self.target_pattern.clear()
        else:
            self.rule_help.setText("Configure parser and rule settings for this check.")

    def _apply_combo_item_tooltips(self) -> None:
        for combo, meta in ((self.parser_type, PARSER_HELP), (self.rule_type, RULE_HELP)):
            for idx in range(combo.count()):
                name = combo.itemText(idx)
                item = meta.get(name, {"summary": "", "example": ""})
                combo.setItemData(idx, item["summary"], Qt.ItemDataRole.ToolTipRole)
                combo.setItemData(idx, f"{item['summary']}\n\n{item['example']}".strip(), Qt.ItemDataRole.WhatsThisRole)

    def _refresh_context_help(self) -> None:
        parser_name = self.parser_type.currentText()
        rule_name = self.rule_type.currentText()
        parser_meta = PARSER_HELP.get(parser_name, {"summary": "No parser help available.", "example": ""})
        rule_meta = RULE_HELP.get(rule_name, {"summary": "No rule help available.", "example": ""})
        self.parser_type.setToolTip(f"{parser_name}\n\n{parser_meta['summary']}\n\n{parser_meta['example']}".strip())
        self.parser_type.setWhatsThis(f"{parser_name}\n\n{parser_meta['summary']}\n\n{parser_meta['example']}".strip())
        self.rule_type.setToolTip(f"{rule_name}\n\n{rule_meta['summary']}\n\n{rule_meta['example']}".strip())
        self.rule_type.setWhatsThis(f"{rule_name}\n\n{rule_meta['summary']}\n\n{rule_meta['example']}".strip())

    def _run_parser_test(self) -> None:
        try:
            data = self.get_data()
            site_id = 1 if self.test_site_combo.currentIndex() == 0 else 2
            file_name = data.get("relative_path_site1") if site_id == 1 else data.get("relative_path_site2")
            file_path = self._test_data_root / ("Site1" if site_id == 1 else "Site2") / str(file_name or "")
            temp_check = MonitoringCheckConfig(
                check_id=0,
                internal_name=str(data["internal_name"] or "temp_check"),
                display_label=str(data["display_label"] or "Temp Check"),
                group_id=int(data["group_id"]),
                description=data.get("description"),
                source_type="File",
                relative_path_site1=data.get("relative_path_site1"),
                relative_path_site2=data.get("relative_path_site2"),
                parser_type=str(data["parser_type"]),
                match_strategy="FirstMatch",
                case_sensitive=False,
                target_pattern=data.get("target_pattern"),
                secondary_pattern=None,
                rule_type=str(data["rule_type"]),
                operator=data.get("operator"),
                threshold_min=float(data["threshold_min"]) if data.get("threshold_min") not in (None, "") else None,
                threshold_max=float(data["threshold_max"]) if data.get("threshold_max") not in (None, "") else None,
                expected_interval_seconds=int(data["expected_interval_seconds"]),
                stale_timeout_seconds=int(data["stale_timeout_seconds"]),
                grace_period_seconds=0,
                pass_text=data.get("pass_text"),
                fail_text=data.get("fail_text"),
            )
            parsed = self._parser_engine.parse_file(file_path, temp_check)
            evaluation = self._monitoring_engine_for_test._apply_rule(temp_check, parsed)
            value_display = parsed.value_numeric if parsed.value_numeric is not None else (parsed.value_text or "(none)")
            extra = ""
            if data.get("parser_type") == "AnsiFormattedText":
                extra = f"\nANSI Detail: {parsed.detail_message}"
                if parsed.technical_detail:
                    extra += f"\nMatched Sequence: {parsed.technical_detail}"
            self._parser_test_result.setText(
                f"File: {file_path}\n"
                f"Parse Status: {parsed.parse_status}\n"
                f"Extracted Value: {value_display}\n"
                f"Rule Result: {evaluation.rule_outcome}\n"
                f"State: {evaluation.operational_state}\n"
                f"Message: {evaluation.detail_message}"
                f"{extra}"
            )
        except Exception as exc:  # noqa: BLE001
            self._parser_test_result.setText(f"Parser test failed: {exc}")

    def _validate_and_accept(self) -> None:
        if not self.internal_name.text().strip():
            self._warn("Internal name is required.")
            return
        if not self.display_label.text().strip():
            self._warn("Display label is required.")
            return
        if not self.site1.isChecked() and not self.site2.isChecked():
            self._warn("At least one site must be enabled.")
            return
        if self.stale_timeout.value() <= self.expected_interval.value():
            self._warn("Stale timeout must be greater than expected interval.")
            return
        rule_type = self.rule_type.currentText()
        parser_type = self.parser_type.currentText()
        if rule_type in {"TextExists", "TextNotExists"} and not self.target_pattern.text().strip():
            self._warn("Target pattern is required for text rules.")
            return
        if parser_type in {"IntegerFromPattern", "DecimalFromPattern"} and not self.target_pattern.text().strip():
            self._warn("Target pattern is required for numeric parsers.")
            return
        if rule_type == "NumericCompare" and self.operator.currentText() not in {">", ">=", "<", "<=", "=="}:
            self._warn("Select a numeric compare operator.")
            return
        self.accept()

    def _warn(self, message: str) -> None:
        QMessageBox.warning(self, "Validation", message)

    def get_data(self) -> dict[str, Any]:
        return {
            "internal_name": self.internal_name.text().strip(),
            "display_label": self.display_label.text().strip(),
            "description": self.description.toPlainText().strip() or None,
            "group_id": int(self.group_combo.currentData()),
            "display_order": self.display_order.value(),
            "applies_to_site1": self.site1.isChecked(),
            "applies_to_site2": self.site2.isChecked(),
            "relative_path_site1": self.path1.text().strip() or None,
            "relative_path_site2": self.path2.text().strip() or None,
            "parser_type": self.parser_type.currentText(),
            "match_strategy": "FirstMatch",
            "case_sensitive": False,
            "target_pattern": self.target_pattern.text().strip() or None,
            "rule_type": self.rule_type.currentText(),
            "operator": self.operator.currentText() or None,
            "threshold_min": None if not self.threshold_min.isEnabled() else self.threshold_min.value(),
            "threshold_max": None if not self.threshold_max.isEnabled() else self.threshold_max.value(),
            "expected_interval_seconds": self.expected_interval.value(),
            "stale_timeout_seconds": self.stale_timeout.value(),
            "pass_text": self.pass_text.text().strip() or None,
            "fail_text": self.fail_text.text().strip() or None,
            "graph_type": self.graph_type.currentText(),
            "troubleshooting_tier1_url": self.tier1_url.text().strip() or None,
            "troubleshooting_tier2_url": self.tier2_url.text().strip() or None,
            "is_enabled": self.is_enabled.isChecked(),
        }
