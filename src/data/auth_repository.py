from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from src.data.seed import verify_password
from src.shared.models import AuthenticatedUser


class AuthenticationError(RuntimeError):
    pass


class AuthRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def authenticate(self, username: str, password: str) -> AuthenticatedUser:
        row = self._conn.execute(
            """
            SELECT u.user_id, u.username, u.display_name, u.password_hash, r.role_name, u.is_active
            FROM users u
            JOIN roles r ON r.role_id = u.role_id
            WHERE u.username = ?
            """,
            (username,),
        ).fetchone()

        if row is None or int(row["is_active"]) != 1:
            raise AuthenticationError("Invalid username or password")

        if not verify_password(password, str(row["password_hash"])):
            raise AuthenticationError("Invalid username or password")

        self._conn.execute(
            "UPDATE users SET last_login_utc = ? WHERE user_id = ?",
            (datetime.now(timezone.utc).isoformat(), int(row["user_id"])),
        )
        self._conn.commit()

        return AuthenticatedUser(
            user_id=int(row["user_id"]),
            username=str(row["username"]),
            display_name=str(row["display_name"]),
            role_name=str(row["role_name"]),
        )
