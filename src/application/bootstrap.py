from __future__ import annotations

import logging
import sys

from PySide6.QtWidgets import QApplication, QMessageBox

from src.application.config_loader import ConfigError, load_bootstrap_config, write_runtime_info_to_db
from src.application.logging_setup import configure_logging
from src.data.auth_repository import AuthRepository
from src.data.database import create_connection, initialize_database
from src.data.monitoring_repository import MonitoringRepository
from src.data.seed import seed_database
from src.engine.monitoring_engine import MonitoringEngine
from src.theming.theme_service import ThemeService
from src.ui.login_window import LoginWindow
from src.ui.main_window import MainWindow

logger = logging.getLogger(__name__)


def bootstrap_and_run() -> None:
    try:
        config = load_bootstrap_config()
    except ConfigError as exc:
        _show_fatal_error(f"Configuration error: {exc}")
        return

    configure_logging(config.app_log_path)
    logger.info("Bootstrap config loaded successfully")
    logger.info("App path: %s", config.app_base_path)
    logger.info("Data path: %s", config.data_root_path)
    logger.info("Migration status: %s", config.migration_status)

    try:
        conn = create_connection(config.database_path)
        initialize_database(conn)
        seed_database(conn, config.default_theme)
        conn.close()
        write_runtime_info_to_db(config.database_path, config)
        conn = create_connection(config.database_path)
        logger.info("Database initialized and seeded successfully")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed during DB startup")
        _show_fatal_error(f"Database startup failure: {exc}")
        return

    app = QApplication(sys.argv)
    theme_service = ThemeService(conn)
    theme_service.apply_theme(app, config.default_theme, 100)

    auth_repo = AuthRepository(conn)
    login = LoginWindow(auth_repo)
    if login.exec() != LoginWindow.DialogCode.Accepted or login.authenticated_user is None:
        logger.info("Application closed before login completion")
        return

    logger.info("User '%s' logged in", login.authenticated_user.username)
    monitoring_repo = MonitoringRepository(conn)
    monitoring_engine = MonitoringEngine(monitoring_repo, config.data_root_path)
    window = MainWindow(login.authenticated_user, conn, monitoring_engine)
    window.show()
    sys.exit(app.exec())


def _show_fatal_error(message: str) -> None:
    app = QApplication.instance() or QApplication(sys.argv)
    QMessageBox.critical(None, "Ops Monitor Startup Error", message)
    if not QApplication.instance():
        app.quit()
