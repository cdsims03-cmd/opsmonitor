from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from src.data.seed import hash_password


class UserRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def list_users(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                """
                SELECT u.user_id, u.username, u.display_name, u.is_active, u.created_utc, u.last_login_utc,
                       r.role_name
                FROM users u
                JOIN roles r ON r.role_id = u.role_id
                ORDER BY lower(u.username)
                """
            )
        )

    def get_user(self, user_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT u.user_id, u.username, u.display_name, u.is_active, u.created_utc, u.last_login_utc,
                   r.role_name, u.role_id
            FROM users u
            JOIN roles r ON r.role_id = u.role_id
            WHERE u.user_id = ?
            """,
            (user_id,),
        ).fetchone()

    def list_role_names(self) -> list[str]:
        return [str(r['role_name']) for r in self._conn.execute("SELECT role_name FROM roles ORDER BY role_id")]

    def create_user(self, acting_user_id: int, username: str, display_name: str, password: str, role_name: str, is_active: bool = True) -> int:
        now = self._utc_now()
        role_id = self._role_id(role_name)
        cur = self._conn.execute(
            """
            INSERT INTO users(username, display_name, password_hash, role_id, is_active, created_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (username.strip(), display_name.strip(), hash_password(password), role_id, 1 if is_active else 0, now),
        )
        user_id = int(cur.lastrowid)
        self._audit(acting_user_id, 'UserCreated', 'User', str(user_id), display_name.strip(), None, json.dumps({
            'username': username.strip(), 'display_name': display_name.strip(), 'role_name': role_name, 'is_active': bool(is_active)
        }))
        self._conn.commit()
        return user_id

    def update_user(self, target_user_id: int, acting_user_id: int, username: str, display_name: str, role_name: str, is_active: bool) -> None:
        existing = self.get_user(target_user_id)
        if existing is None:
            raise ValueError('User not found')
        if not is_active and self._would_disable_last_system_admin(target_user_id, role_name):
            raise ValueError('You cannot disable the last active SystemAdmin.')
        role_id = self._role_id(role_name)
        self._conn.execute(
            """
            UPDATE users
            SET username = ?, display_name = ?, role_id = ?, is_active = ?
            WHERE user_id = ?
            """,
            (username.strip(), display_name.strip(), role_id, 1 if is_active else 0, target_user_id),
        )
        self._audit(acting_user_id, 'UserUpdated', 'User', str(target_user_id), display_name.strip(), json.dumps(dict(existing)), json.dumps({
            'username': username.strip(), 'display_name': display_name.strip(), 'role_name': role_name, 'is_active': bool(is_active)
        }))
        self._conn.commit()

    def reset_password(self, target_user_id: int, acting_user_id: int, new_password: str) -> None:
        existing = self.get_user(target_user_id)
        if existing is None:
            raise ValueError('User not found')
        self._conn.execute(
            "UPDATE users SET password_hash = ? WHERE user_id = ?",
            (hash_password(new_password), target_user_id),
        )
        self._audit(acting_user_id, 'UserPasswordReset', 'User', str(target_user_id), str(existing['display_name']), None, json.dumps({'password_reset': True}))
        self._conn.commit()

    def toggle_user_active(self, target_user_id: int, acting_user_id: int) -> None:
        existing = self.get_user(target_user_id)
        if existing is None:
            raise ValueError('User not found')
        new_active = 0 if int(existing['is_active']) == 1 else 1
        if new_active == 0 and str(existing['role_name']) == 'SystemAdmin' and self._would_disable_last_system_admin(target_user_id, str(existing['role_name'])):
            raise ValueError('You cannot disable the last active SystemAdmin.')
        self._conn.execute('UPDATE users SET is_active = ? WHERE user_id = ?', (new_active, target_user_id))
        self._audit(acting_user_id, 'UserToggled', 'User', str(target_user_id), str(existing['display_name']), json.dumps(dict(existing)), json.dumps({'is_active': bool(new_active)}))
        self._conn.commit()

    def _role_id(self, role_name: str) -> int:
        row = self._conn.execute('SELECT role_id FROM roles WHERE role_name = ?', (role_name,)).fetchone()
        if row is None:
            raise ValueError(f'Role not found: {role_name}')
        return int(row['role_id'])

    def _would_disable_last_system_admin(self, target_user_id: int, role_name_after: str) -> bool:
        if role_name_after != 'SystemAdmin':
            count = self._conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM users u JOIN roles r ON r.role_id = u.role_id
                WHERE r.role_name = 'SystemAdmin' AND u.is_active = 1 AND u.user_id != ?
                """,
                (target_user_id,),
            ).fetchone()
            return int(count['c']) == 0
        count = self._conn.execute(
            """
            SELECT COUNT(1) AS c
            FROM users u JOIN roles r ON r.role_id = u.role_id
            WHERE r.role_name = 'SystemAdmin' AND u.is_active = 1 AND u.user_id != ?
            """,
            (target_user_id,),
        ).fetchone()
        return int(count['c']) == 0

    def _audit(self, user_id: int, action_type: str, entity_type: str, entity_id: str | None, entity_name: str | None, old_value_json: str | None, new_value_json: str | None) -> None:
        self._conn.execute(
            """
            INSERT INTO audit_log(audit_utc, user_id, action_type, entity_type, entity_id, entity_name, old_value_json, new_value_json, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (self._utc_now(), user_id, action_type, entity_type, entity_id, entity_name, old_value_json, new_value_json, f'{action_type}: {entity_name or entity_id or entity_type}'),
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()
