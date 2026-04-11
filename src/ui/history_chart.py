
from __future__ import annotations

from datetime import datetime
from typing import Sequence

from PySide6.QtCore import Qt, QPoint, Signal, QDateTime
from PySide6.QtGui import QPainter, QMouseEvent, QWheelEvent, QColor, QFont
from PySide6.QtWidgets import QToolTip
from PySide6.QtCharts import (
    QAreaSeries,
    QChart,
    QChartView,
    QLineSeries,
    QScatterSeries,
    QDateTimeAxis,
    QValueAxis,
)


class HistoryChartWidget(QChartView):
    zoom_state_changed = Signal(bool)

    def __init__(self) -> None:
        chart = QChart()
        super().__init__(chart)
        self._title = "History"
        self._rows: list[dict] = []
        self._threshold_min: float | None = None
        self._threshold_max: float | None = None
        self._is_zoomed = False
        self._pan_active = False
        self._last_pan_pos = QPoint()
        self._base_x_range: tuple[float, float] | None = None
        self._base_y_range: tuple[float, float] | None = None
        self._view_x_range: tuple[float, float] | None = None
        self._view_y_range: tuple[float, float] | None = None
        self._point_lookup: dict[int, dict] = {}
        self._event_point_lookup: dict[tuple[int, int], dict] = {}
        self._graph_type = "Line"
        self._theme = {
            "bg": "#14171C",
            "text": "#E5E7EB",
            "grid": "#43484F",
            "accent": "#60A5FA",
            "threshold": "#F59E0B",
        }

        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setRubberBand(QChartView.RubberBand.RectangleRubberBand)
        self.setMouseTracking(True)
        self.setMinimumHeight(260)
        self.apply_theme(self._theme)

    def apply_theme(self, theme: dict) -> None:
        self._theme = dict(theme)
        chart = self.chart()
        chart.setBackgroundBrush(QColor(self._theme.get("bg", "#14171C")))
        chart.setTitleBrush(QColor(self._theme.get("text", "#E5E7EB")))
        chart.setPlotAreaBackgroundVisible(False)
        chart.legend().hide()

        title_font = QFont()
        title_font.setPixelSize(12)
        title_font.setBold(True)
        chart.setTitleFont(title_font)

        axis_font = QFont()
        axis_font.setPixelSize(10)

        for axis in chart.axes():
            axis.setLabelsFont(axis_font)
            axis.setLabelsColor(QColor(self._theme.get("text", "#E5E7EB")))
            axis.setGridLineColor(QColor(self._theme.get("grid", "#43484F")))
            axis.setLinePenColor(QColor(self._theme.get("grid", "#43484F")))

    def set_graph_type(self, graph_type: str) -> None:
        self._graph_type = graph_type or "Line"

    def set_data(self, title: str, rows: Sequence[dict], threshold_min: float | None = None, threshold_max: float | None = None) -> None:
        current_x = None
        current_y = None
        axis_x = self._axis_x()
        axis_y = self._axis_y()
        if axis_x is not None and axis_y is not None and self._is_zoomed:
            current_x = (axis_x.min().toMSecsSinceEpoch(), axis_x.max().toMSecsSinceEpoch())
            current_y = (axis_y.min(), axis_y.max())

        self._title = title
        self._rows = list(rows)
        self._threshold_min = threshold_min
        self._threshold_max = threshold_max
        self._point_lookup = {}
        self._event_point_lookup = {}

        chart = self.chart()
        chart.removeAllSeries()
        for axis in list(chart.axes()):
            chart.removeAxis(axis)
        chart.setTitle(title)

        if not self._rows:
            self._base_x_range = None
            self._base_y_range = None
            self._is_zoomed = False
            self.zoom_state_changed.emit(False)
            return

        numeric_rows = [r for r in self._rows if r.get("value_numeric") is not None]
        if numeric_rows and self._graph_type != "State Timeline":
            self._build_numeric_chart(numeric_rows)
        else:
            self._build_state_chart(self._rows)

        self.apply_theme(self._theme)
        if current_x and current_y:
            self._restore_view(current_x, current_y)
        else:
            self.reset_zoom()

        chart.layout().invalidate()
        chart.update()
        self.viewport().update()
        self.update()

    def _build_numeric_chart(self, rows: list[dict]) -> None:
        chart = self.chart()
        line = QLineSeries()
        line.setName("Value")
        line.setPointsVisible(True)
        line.hovered.connect(self._on_series_hovered)

        point_markers = QScatterSeries()
        point_markers.setName("Samples")
        point_markers.setMarkerSize(8.0)
        point_markers.hovered.connect(self._on_series_hovered)

        xs, ys = [], []
        for row in rows:
            x = self._to_ms(str(row.get("evaluated_utc", "")))
            y = float(row["value_numeric"])
            xs.append(x); ys.append(y)
            line.append(x, y)
            point_markers.append(x, y)
            self._point_lookup[int(round(x))] = row

        if self._graph_type == "Step Line":
            step = QLineSeries()
            step.setPointsVisible(True)
            step.hovered.connect(self._on_series_hovered)
            last_x = None
            last_y = None
            for row in rows:
                x = self._to_ms(str(row.get("evaluated_utc", "")))
                y = float(row["value_numeric"])
                if last_x is not None:
                    step.append(x, last_y)
                step.append(x, y)
                last_x, last_y = x, y
            chart.addSeries(step)
        elif self._graph_type == "Area":
            lower = QLineSeries()
            upper = line
            area = QAreaSeries(upper, lower)
            chart.addSeries(area)
            chart.addSeries(point_markers)
        else:
            chart.addSeries(line)
            chart.addSeries(point_markers)

        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(6)
        axis_y = QValueAxis()
        axis_y.setLabelFormat("%.0f")

        vals = ys[:]
        if self._threshold_min is not None:
            vals.append(float(self._threshold_min))
        if self._threshold_max is not None:
            vals.append(float(self._threshold_max))
        low, high = min(vals), max(vals)
        if low == high:
            low -= 1; high += 1
        padding = max((high - low) * 0.08, 1.0)
        low -= padding; high += padding

        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        for s in chart.series():
            s.attachAxis(axis_x)
            s.attachAxis(axis_y)

        if self._threshold_max is not None:
            threshold = QLineSeries()
            threshold.append(xs[0], float(self._threshold_max))
            threshold.append(xs[-1], float(self._threshold_max))
            chart.addSeries(threshold)
            threshold.attachAxis(axis_x)
            threshold.attachAxis(axis_y)

        if self._threshold_min is not None and self._threshold_min != self._threshold_max:
            threshold2 = QLineSeries()
            threshold2.append(xs[0], float(self._threshold_min))
            threshold2.append(xs[-1], float(self._threshold_min))
            chart.addSeries(threshold2)
            threshold2.attachAxis(axis_x)
            threshold2.attachAxis(axis_y)

        self._add_event_markers(rows, axis_x, axis_y, numeric=True)

        self._base_x_range = (xs[0], xs[-1] if xs[-1] > xs[0] else xs[0] + 1)
        self._base_y_range = (low, high)
        self._style_series()

    def _build_state_chart(self, rows: list[dict]) -> None:
        chart = self.chart()
        series = QLineSeries()
        series.hovered.connect(self._on_series_hovered)
        state_map = {"Healthy": 2.0, "Stale": 1.0, "Unhealthy": 0.0}
        xs = []
        for row in rows:
            x = self._to_ms(str(row.get("evaluated_utc", "")))
            y = state_map.get(str(row.get("operational_state") or "Unknown"), 0.0)
            xs.append(x)
            series.append(x, y)
            self._point_lookup[int(round(x))] = row

        chart.addSeries(series)
        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(6)
        axis_y = QValueAxis()
        axis_y.setRange(-0.25, 2.25)
        axis_y.setTickCount(3)
        axis_y.setLabelFormat("%.0f")
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)
        self._add_event_markers(rows, axis_x, axis_y, numeric=False)
        self._base_x_range = (xs[0], xs[-1] if xs[-1] > xs[0] else xs[0] + 1)
        self._base_y_range = (-0.25, 2.25)
        self._style_series()

    def _add_event_markers(self, rows: list[dict], axis_x: QDateTimeAxis, axis_y: QValueAxis, *, numeric: bool) -> None:
        starts = [row for row in rows if row.get("event_marker") == "AlertStart"]
        clears = [row for row in rows if row.get("event_marker") == "AlertEnd"]
        if not starts and not clears:
            return

        for marker_rows, marker_name in ((starts, "AlertStart"), (clears, "AlertEnd")):
            if not marker_rows:
                continue
            marker_series = QScatterSeries()
            marker_series.setMarkerSize(11.0)
            marker_series.hovered.connect(self._on_event_marker_hovered)
            marker_series.setName(marker_name)
            for row in marker_rows:
                x = self._to_ms(str(row.get("evaluated_utc", "")))
                if numeric:
                    if row.get("value_numeric") is None:
                        continue
                    y = float(row["value_numeric"])
                else:
                    state_map = {"Healthy": 2.0, "Stale": 1.0, "Unhealthy": 0.0}
                    y = state_map.get(str(row.get("operational_state") or "Unknown"), 0.0)
                marker_series.append(x, y)
                self._event_point_lookup[(int(round(x)), int(round(y * 1000)))] = row
            if marker_series.count() == 0:
                continue
            self.chart().addSeries(marker_series)
            marker_series.attachAxis(axis_x)
            marker_series.attachAxis(axis_y)

    def _style_series(self) -> None:
        accent = QColor(self._theme.get("accent", "#60A5FA"))
        threshold = QColor(self._theme.get("threshold", "#F59E0B"))
        alert_start = QColor("#EF4444")
        alert_end = QColor("#22C55E")
        threshold_index = 0
        for series in self.chart().series():
            if isinstance(series, QScatterSeries):
                if series.name() == "Samples":
                    color = accent
                else:
                    color = alert_start if series.name() == "AlertStart" else alert_end
                pen = series.pen()
                pen.setColor(color)
                pen.setWidth(2)
                series.setPen(pen)
                series.setColor(color)
                series.setBorderColor(color)
                continue

            pen = series.pen()
            pen.setWidth(2)
            if threshold_index == 0:
                pen.setColor(accent)
            else:
                pen.setColor(threshold)
            series.setPen(pen)
            if isinstance(series, QAreaSeries):
                series.setColor(accent)
            threshold_index += 1

    def _restore_view(self, x_range: tuple[float, float], y_range: tuple[float, float]) -> None:
        axis_x = self._axis_x()
        axis_y = self._axis_y()
        if axis_x and axis_y:
            axis_x.setRange(QDateTime.fromMSecsSinceEpoch(int(x_range[0])), QDateTime.fromMSecsSinceEpoch(int(x_range[1])))
            axis_y.setRange(y_range[0], y_range[1])
            self._is_zoomed = True
            self.zoom_state_changed.emit(True)

    def reset_zoom(self) -> None:
        self.chart().zoomReset()
        if self._base_x_range:
            axis_x = self._axis_x()
            if axis_x:
                axis_x.setRange(QDateTime.fromMSecsSinceEpoch(int(self._base_x_range[0])), QDateTime.fromMSecsSinceEpoch(int(self._base_x_range[1])))
        if self._base_y_range:
            axis_y = self._axis_y()
            if axis_y:
                axis_y.setRange(*self._base_y_range)
        self._is_zoomed = False
        self.zoom_state_changed.emit(False)

    def can_zoom(self) -> bool:
        return self._is_zoomed

    def wheelEvent(self, event: QWheelEvent) -> None:
        if not self._rows:
            return
        self.chart().zoom(1.15 if event.angleDelta().y() > 0 else 0.87)
        self._is_zoomed = True
        self.zoom_state_changed.emit(True)
        event.accept()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        if event.button() == Qt.MouseButton.RightButton:
            self._pan_active = True
            self._last_pan_pos = event.pos()
            self.setCursor(Qt.CursorShape.ClosedHandCursor)
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        if self._pan_active:
            delta = event.pos() - self._last_pan_pos
            self.chart().scroll(-delta.x(), delta.y())
            self._last_pan_pos = event.pos()
            self._is_zoomed = True
            self.zoom_state_changed.emit(True)
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        if self._pan_active and event.button() == Qt.MouseButton.RightButton:
            self._pan_active = False
            self.setCursor(Qt.CursorShape.ArrowCursor)
            self._is_zoomed = True
            self.zoom_state_changed.emit(True)
            event.accept()
            return
        super().mouseReleaseEvent(event)
        axis_x = self._axis_x()
        if axis_x and self._base_x_range:
            current_min = axis_x.min().toMSecsSinceEpoch()
            current_max = axis_x.max().toMSecsSinceEpoch()
            self._is_zoomed = (abs(current_min - self._base_x_range[0]) > 1 or abs(current_max - self._base_x_range[1]) > 1)
            self.zoom_state_changed.emit(self._is_zoomed)

    def _on_series_hovered(self, point, state: bool) -> None:
        if not state:
            QToolTip.hideText()
            return
        row = self._point_lookup.get(int(round(point.x())))
        if not row:
            return
        value = row.get("value_numeric")
        detail = f"Time: {self._format_tooltip_ts(str(row.get('evaluated_utc', '')))}\n"
        detail += f"Value: {value if value is not None else row.get('value_text', '')}"
        if self._threshold_max is not None:
            detail += f"\nThreshold: {self._threshold_max:g}"
        detail += f"\nStatus: {row.get('operational_state', '')}"
        QToolTip.showText(self.mapToGlobal(self.rect().center()), detail, self)

    def _on_event_marker_hovered(self, point, state: bool) -> None:
        if not state:
            QToolTip.hideText()
            return
        row = self._event_point_lookup.get((int(round(point.x())), int(round(point.y() * 1000))))
        if not row:
            return
        marker = "Alert started" if row.get("event_marker") == "AlertStart" else "Alert cleared"
        value = row.get("value_numeric")
        detail = f"{marker}\nTime: {self._format_tooltip_ts(str(row.get('evaluated_utc', '')))}"
        if value is not None:
            detail += f"\nValue: {value}"
        elif row.get("value_text"):
            detail += f"\nValue: {row.get('value_text', '')}"
        detail += f"\nStatus: {row.get('operational_state', '')}"
        QToolTip.showText(self.mapToGlobal(self.rect().center()), detail, self)

    def _axis_x(self):
        for axis in self.chart().axes(Qt.Orientation.Horizontal):
            if isinstance(axis, QDateTimeAxis):
                return axis
        return None

    def _axis_y(self):
        for axis in self.chart().axes(Qt.Orientation.Vertical):
            if isinstance(axis, QValueAxis):
                return axis
        return None

    @staticmethod
    def _to_ms(value: str) -> float:
        try:
            return float(datetime.fromisoformat(value).timestamp() * 1000)
        except ValueError:
            return 0.0

    @staticmethod
    def _format_tooltip_ts(value: str) -> str:
        try:
            return datetime.fromisoformat(value).strftime('%H:%M:%S')
        except ValueError:
            return value
