from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re

from src.data.monitoring_repository import MonitoringCheckConfig


@dataclass(slots=True)
class ParseResult:
    file_path: str
    source_modified_utc: str | None
    source_age_seconds: int | None
    parse_status: str
    value_type: str | None
    value_text: str | None
    value_numeric: float | None
    detail_message: str
    technical_detail: str | None
    raw_content: str | None


class ParserEngine:
    def parse_file(self, file_path: Path, check: MonitoringCheckConfig) -> ParseResult:
        now = datetime.now(timezone.utc)
        if not file_path.exists():
            return ParseResult(str(file_path), None, None, "NoInput", None, None, None, "File not found", None, None)

        stat = file_path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        age_seconds = max(0, int((now - modified).total_seconds()))

        try:
            raw = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Failed", None, None, None, "File locked or unreadable", str(exc), None)

        if check.parser_type == "FreshnessOnly":
            return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Success", None, None, None, "File exists and freshness metadata captured", None, None)

        if check.parser_type == "FileMustBeBlank":
            stripped = raw.strip()
            if stripped == "":
                return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Success", "text", "", None, "File is blank", None, raw)
            return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Success", "text", stripped, None, f"File contains {len(stripped)} non-whitespace characters", None, raw)

        if raw == "":
            return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Failed", None, None, None, "File exists but is empty", None, "")

        if check.parser_type == "AnsiFormattedText":
            ansi_matches = list(re.finditer(r"\^\[\[([0-9;]+)m(.*?)\^\[\[0m", raw, re.DOTALL))
            if not ansi_matches:
                return ParseResult(
                    str(file_path),
                    modified.isoformat(),
                    age_seconds,
                    "Success",
                    "text",
                    raw.strip(),
                    None,
                    "Loaded ANSI-formatted text (no formatted sequences detected)",
                    None,
                    raw,
                )
            first = ansi_matches[0]
            ansi_code = first.group(1)
            wrapped_value = first.group(2).strip()
            return ParseResult(
                str(file_path),
                modified.isoformat(),
                age_seconds,
                "Success",
                "ansi_text",
                raw.strip(),
                None,
                f'ANSI match detected: code={ansi_code} value={wrapped_value or "(blank)"}',
                first.group(0),
                raw,
            )

        if check.parser_type == "RawText":
            return ParseResult(
                str(file_path),
                modified.isoformat(),
                age_seconds,
                "Success",
                "text",
                raw.strip(),
                None,
                f"Loaded text content ({len(raw)} chars)",
                None,
                raw,
            )

        if check.parser_type in {"IntegerFromPattern", "DecimalFromPattern"}:
            pattern = check.target_pattern or ""
            idx = raw.lower().find(pattern.lower()) if not check.case_sensitive else raw.find(pattern)
            if idx < 0:
                return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Failed", None, None, None, f'Pattern "{pattern}" not found', None, raw)
            segment = raw[idx + len(pattern):]
            regex = r"[-+]?\d+" if check.parser_type == "IntegerFromPattern" else r"[-+]?\d+(?:\.\d+)?"
            match = re.search(regex, segment)
            if match is None:
                return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Failed", None, None, None, f'Pattern "{pattern}" found but numeric conversion failed', None, raw)
            value = float(match.group(0))
            if check.parser_type == "IntegerFromPattern":
                return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Success", "number", None, int(value), f'Extracted integer {int(value)} after pattern "{pattern}"', None, raw)
            return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Success", "number", None, value, f'Extracted decimal {value} after pattern "{pattern}"', None, raw)

        return ParseResult(str(file_path), modified.isoformat(), age_seconds, "Failed", None, None, None, f"Unsupported parser type: {check.parser_type}", None, raw)
