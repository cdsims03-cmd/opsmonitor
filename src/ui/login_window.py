from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from src.data.auth_repository import AuthRepository, AuthenticationError
from src.shared.constants import APP_NAME
from src.shared.models import AuthenticatedUser


class LoginWindow(QDialog):
    def __init__(self, auth_repository: AuthRepository) -> None:
        super().__init__()
        self._auth_repository = auth_repository
        self.authenticated_user: AuthenticatedUser | None = None
        self.setWindowTitle(f"{APP_NAME} - Login")
        self.setModal(True)
        self.setMinimumWidth(380)

        title = QLabel("Ops Monitor Login")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setStyleSheet("font-size: 18px; font-weight: bold;")

        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        self.username_edit.setText("admin")

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_edit.setText("admin123")

        form = QFormLayout()
        form.addRow("Username", self.username_edit)
        form.addRow("Password", self.password_edit)

        login_button = QPushButton("Login")
        login_button.clicked.connect(self._attempt_login)

        layout = QVBoxLayout()
        layout.addWidget(title)
        layout.addLayout(form)
        layout.addWidget(login_button)
        self.setLayout(layout)

    def _attempt_login(self) -> None:
        try:
            self.authenticated_user = self._auth_repository.authenticate(
                self.username_edit.text().strip(),
                self.password_edit.text(),
            )
        except AuthenticationError as exc:
            QMessageBox.warning(self, "Login Failed", str(exc))
            return

        self.accept()
