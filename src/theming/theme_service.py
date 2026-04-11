from __future__ import annotations

import sqlite3

from PySide6.QtWidgets import QApplication


class ThemeService:

    def get_theme_tokens(self, theme_name: str) -> dict:
        row = self._conn.execute("SELECT * FROM themes WHERE theme_name = ?", (theme_name,)).fetchone()
        if row is None:
            return {"bg": "#14171C", "text": "#E5E7EB", "grid": "#43484F", "accent": "#60A5FA", "threshold": "#F59E0B"}
        return {
            "bg": row["panel_color"] if "panel_color" in row.keys() else row["background_color"],
            "text": row["text_color"],
            "grid": row["border_color"],
            "accent": row["accent_color"],
            "threshold": "#F59E0B",
        }

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def apply_theme(self, app: QApplication, theme_name: str, zoom_percent: int = 100) -> None:
        row = self._conn.execute(
            "SELECT * FROM themes WHERE theme_name = ?", (theme_name,)
        ).fetchone()
        if row is None:
            return

        panel_alt = "#242833" if row["is_dark_mode"] else "#F3F5F7"
        subtle = "#B8C0CC" if row["is_dark_mode"] else "#4B5563"
        card_bg = "#1B2029" if row["is_dark_mode"] else "#FFFFFF"
        raised_bg = "#202633" if row["is_dark_mode"] else "#FAFBFD"
        base_font = max(11, int(round(12 * zoom_percent / 100)))
        small_font = max(10, base_font - 1)
        section_font = base_font + 2
        status_font = base_font + 8

        app.setStyleSheet(
            f"""
            QWidget {{
                background-color: {row['background_color']};
                color: {row['text_color']};
                font-size: {base_font}px;
            }}
            QMainWindow, QDialog {{
                background-color: {row['background_color']};
            }}
            QLabel {{
                background: transparent;
            }}
            QToolTip {{
                background-color: rgba(17, 24, 39, 235);
                color: #F9FAFB;
                border: 1px solid rgba(96, 165, 250, 0.85);
                border-radius: 10px;
                padding: 10px 12px;
                font-size: {base_font}px;
            }}
            QRubberBand {{
                border: 1px dashed rgba(59, 130, 246, 0.95);
                background-color: rgba(59, 130, 246, 0.28);
            }}
            QLabel#headerTitle {{
                font-size: {section_font + 4}px;
                font-weight: 800;
                color: white;
                letter-spacing: 0.5px;
            }}
            QLabel#headerMeta {{
                color: rgba(255,255,255,0.86);
                font-size: {base_font}px;
                font-weight: 700;
                padding: 0 4px;
            }}
            QLabel#headerUser {{
                color: white;
                font-size: {base_font}px;
                font-weight: 700;
                padding: 0 2px;
            }}
            QLabel#headerIcon {{
                color: white;
                font-size: {section_font + 2}px;
                font-weight: 800;
                padding-right: 4px;
            }}
            QLabel#headerPill, QLabel#headerPillAlt {{
                color: white;
                font-size: {base_font}px;
                font-weight: 800;
                padding: 7px 12px;
                border-radius: 12px;
                background-color: rgba(255,255,255,0.10);
                border: 1px solid rgba(255,255,255,0.14);
            }}
            QLabel#headerPillAlt {{
                background-color: rgba(17,24,39,0.26);
            }}
            QLabel#sectionTitle {{
                font-size: {section_font}px;
                font-weight: 700;
            }}
            QLabel#groupTitle {{
                font-size: {base_font + 1}px;
                font-weight: 700;
            }}
            QLabel#selectedCheckName {{
                font-size: {section_font + 1}px;
                font-weight: 700;
            }}
            QLabel#statusBanner {{
                background-color: #7F1D1D;
                color: white;
                border: 1px solid #DC2626;
                border-radius: 8px;
                padding: 8px 10px;
                font-weight: 700;
            }}
            QLabel#tileTitle {{
                font-size: 13px;
                font-weight: 700;
            }}
            QLabel#tileMeta {{
                color: {subtle};
                font-size: {small_font}px;
            }}
            QFrame#panel, QFrame#groupPanel, QFrame#selectedHeader, QFrame#detailCard {{
                background-color: {card_bg};
                border: 1px solid {row['border_color']};
                border-radius: 14px;
            }}
            QFrame#header {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {row['panel_color']}, stop:0.52 {row['header_color']}, stop:1 {row['panel_color']});
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 14px;
            }}
            QPushButton {{
                background-color: {row['accent_color']};
                color: white;
                border: 1px solid rgba(255,255,255,0.10);
                padding: 8px 12px;
                border-radius: 10px;
                font-weight: 700;
            }}
            QPushButton#headerIconButton {{
                background-color: rgba(255,255,255,0.08);
                color: white;
                min-width: 28px;
                max-width: 28px;
                padding: 6px;
                border-radius: 10px;
                border: 1px solid rgba(255,255,255,0.10);
            }}
            QPushButton#headerIconButton:hover {{
                background-color: rgba(255,255,255,0.16);
            }}
            QFrame#headerSegment {{
                background-color: rgba(255,255,255,0.08);
                border: 1px solid rgba(255,255,255,0.10);
                border-radius: 12px;
            }}
            QPushButton#headerSegmentButton {{
                background-color: transparent;
                color: rgba(255,255,255,0.84);
                border: none;
                padding: 7px 12px;
                min-width: 64px;
                border-radius: 9px;
                font-weight: 800;
            }}
            QPushButton#headerSegmentButton:hover {{
                background-color: rgba(255,255,255,0.10);
                color: white;
            }}
            QPushButton#headerSegmentButton:checked {{
                background-color: {row['accent_color']};
                color: white;
                border: 1px solid rgba(255,255,255,0.14);
            }}
            QPushButton:hover {{
                background-color: {row['header_color']};
            }}
            QPushButton:disabled {{
                background-color: #666666;
                color: #DDDDDD;
            }}

            QPushButton#ackAction {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #2563EB, stop:1 #1D4ED8);
            }}
            QPushButton#ackAction:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #3B82F6, stop:1 #1E40AF);
            }}
            QPushButton#escalateAction {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #EF4444, stop:1 #991B1B);
            }}
            QPushButton#escalateAction:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #F87171, stop:1 #B91C1C);
            }}
            QPushButton#salesforceAction {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #64748B, stop:1 #475569);
            }}
            QPushButton#salesforceAction:hover {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #94A3B8, stop:1 #475569);
            }}
            QLabel#escalationBanner {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 rgba(245, 158, 11, 0.28), stop:1 rgba(217, 119, 6, 0.18));
                border: 1px solid #D97706;
                border-radius: 10px;
                padding: 8px 10px;
                font-weight: 700;
            }}
            QPushButton[activeRange="true"] {{
                border: 2px solid white;
            }}
            QLineEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox, QComboBox, QScrollArea {{
                padding: 6px;
                border: 1px solid {row['border_color']};
                border-radius: 10px;
                background: {raised_bg};
                selection-background-color: {row['accent_color']};
            }}
            QTableWidget {{
                background: {card_bg};
                color: {row['text_color']};
                alternate-background-color: {panel_alt};
                gridline-color: {row['border_color']};
                border: 1px solid {row['border_color']};
                border-radius: 6px;
                selection-background-color: {row['accent_color']};
                selection-color: white;
            }}
            QTableWidget::item {{
                padding: 6px;
                color: {row['text_color']};
            }}
            QTableWidget::item:selected {{
                background: {row['accent_color']};
                color: white;
            }}
            QHeaderView::section {{
                background-color: {row['header_color']};
                color: white;
                font-weight: 700;
                padding: 6px;
                border: none;
            }}
            QTabWidget::pane {{
                border-left: 1px solid {row['border_color']};
                border-right: 1px solid {row['border_color']};
                border-bottom: 1px solid {row['border_color']};
                border-top: none;
                border-bottom-left-radius: 8px;
                border-bottom-right-radius: 8px;
                top: -1px;
                background: {row['panel_color']};
            }}
            QTabBar::tab {{
                background: {panel_alt};
                color: {row['text_color']};
                padding: 10px 16px;
                margin-right: 4px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                border: 1px solid {row['border_color']};
                font-weight: 700;
                min-width: 88px;
            }}
            QTabBar::tab:selected {{
                background: {row['header_color']};
                color: white;
            }}
            QTabBar::tab:hover {{
                background: {row['accent_color']};
                color: white;
            }}
            QFrame#checkTile {{
                border-left: 6px solid {row['accent_color']};
                border-radius: 8px;
                border-top: 1px solid {row['border_color']};
                border-right: 1px solid {row['border_color']};
                border-bottom: 1px solid {row['border_color']};
                background-color: {row['panel_color']};
            }}
            QFrame#checkTile[state="Healthy"] {{
                border-left-color: #2E7D32;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(34, 197, 94, 0.16),
                    stop:1 rgba(21, 128, 61, 0.08));
            }}
            QFrame#checkTile[state="Healthy"] QLabel#tileTitle,
            QFrame#checkTile[state="Healthy"] QLabel#tileMeta {{
                color: #F0FDF4;
            }}
            QFrame#checkTile[state="Unhealthy"] {{
                border-left-color: #D32F2F;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(239, 68, 68, 0.18),
                    stop:0.55 rgba(248, 113, 113, 0.10),
                    stop:1 rgba(251, 191, 36, 0.08));
            }}
            QFrame#checkTile[state="Stale"] {{
                border-left-color: #F9A825;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(245, 158, 11, 0.16),
                    stop:1 rgba(251, 191, 36, 0.08));
            }}
            QFrame#checkTile[state="Unhealthy"] QLabel#tileTitle,
            QFrame#checkTile[state="Unhealthy"] QLabel#tileMeta,
            QFrame#checkTile[state="Unhealthy"] QLabel {{
                color: #FFF7F7;
            }}
            QFrame#checkTile[state="Stale"] QLabel#tileTitle,
            QFrame#checkTile[state="Stale"] QLabel#tileMeta {{
                color: #FFF7ED;
            }}
            QLabel#ackBadge {{
                background-color: #455A64;
                color: white;
                border-radius: 8px;
                padding: 4px 8px;
                font-weight: 700;
            }}
            QLabel#escBadge {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #FB923C, stop:1 #C2410C);
                color: white;
                border-radius: 8px;
                padding: 4px 8px;
                font-weight: 700;
            }}

            QLabel#selectedStatusPill, QLabel#detailStatusLabel {{
                font-size: {status_font}px;
                font-weight: 800;
                letter-spacing: 1px;
            }}
            QLabel#selectedStatusPill[state="Healthy"], QLabel#detailStatusLabel[state="Healthy"] {{
                color: #22C55E;
            }}
            QLabel#selectedStatusPill[state="Unhealthy"], QLabel#detailStatusLabel[state="Unhealthy"] {{
                color: #EF4444;
            }}
            QLabel#selectedStatusPill[state="Stale"], QLabel#detailStatusLabel[state="Stale"] {{
                color: #F59E0B;
            }}
            QFrame#checkTile[hoverPersistent="true"] {{
                background-color: rgba(255,255,255,0.08);
                border-top: 1px solid {row['accent_color']};
                border-right: 1px solid {row['accent_color']};
                border-bottom: 1px solid {row['accent_color']};
            }}
            QFrame#checkTile[state="Healthy"][selected="true"] {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(22, 163, 74, 0.82),
                    stop:1 rgba(34, 197, 94, 0.24));
                border-top: 1px solid #86EFAC;
                border-right: 1px solid #86EFAC;
                border-bottom: 1px solid #86EFAC;
            }}
            QFrame#checkTile[state="Unhealthy"][selected="true"] {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(220, 38, 38, 0.88),
                    stop:0.55 rgba(239, 68, 68, 0.72),
                    stop:1 rgba(251, 191, 36, 0.25));
                border-top: 1px solid #FCA5A5;
                border-right: 1px solid #FCA5A5;
                border-bottom: 1px solid #FCA5A5;
            }}
            QFrame#checkTile[state="Stale"][selected="true"] {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 rgba(217, 119, 6, 0.80),
                    stop:1 rgba(251, 191, 36, 0.24));
                border-top: 1px solid #FCD34D;
                border-right: 1px solid #FCD34D;
                border-bottom: 1px solid #FCD34D;
            }}
            QFrame#checkTile[selected="true"] {{
                background-color: rgba(255,255,255,0.12);
                border-top: 1px solid #FFFFFF;
                border-right: 1px solid #FFFFFF;
                border-bottom: 1px solid #FFFFFF;
            }}
            QFrame#checkTile[state="Healthy"][selected="true"] QLabel,
            QFrame#checkTile[state="Unhealthy"][selected="true"] QLabel,
            QFrame#checkTile[state="Stale"][selected="true"] QLabel {{
                color: white;
            }}
            QLabel#detailCard {{
                background-color: rgba(255,255,255,0.03);
                border: 1px solid {row['border_color']};
                border-radius: 6px;
                padding: 8px;
            }}
            QFrame#eventCard {{
                border: 1px solid {row['border_color']};
                border-radius: 6px;
                background-color: rgba(255,255,255,0.03);
            }}
            """
        )