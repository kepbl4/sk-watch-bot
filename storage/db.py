"""SQLite storage helpers for SK Watch Bot."""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_DB_LOCK = threading.RLock()
_CONNECTION: Optional[sqlite3.Connection] = None
_INITIALISED = False

DEFAULT_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    ("UTOCISKO_REG", "Уточиско — регистрация"),
    ("UTOCISKO_DOKLAD", "Уточиско — документы"),
    ("PRECHODNY", "ВНЖ (временный)"),
    ("TRVALY_5Y", "ПМЖ на 5 лет"),
    ("TRVALY_UNLIM", "ПМЖ без срока"),
)

DEFAULT_CITIES: Tuple[Tuple[str, str], ...] = (
    ("banska_bystrica", "Банска-Бистрица"),
    ("bratislava", "Братислава"),
    ("dunajska_streda", "Дунайска-Стреда"),
    ("kosice", "Кошице"),
    ("michalovce", "Михаловце"),
    ("nitra", "Нитра"),
    ("nove_zamky", "Нове-Замки"),
    ("presov", "Прешов"),
    ("rimavska_sobota", "Римавска-Собота"),
    ("ruzomberok", "Ружомберок"),
    ("trencin", "Тренчин"),
    ("trnava", "Трнава"),
    ("zilina", "Жилина"),
)


def _resolve_db_path() -> str:
    url = os.getenv("DB_URL", "sqlite:///./bot.db")
    if not url.startswith("sqlite://"):
        raise ValueError("DB_URL must use sqlite:// scheme")

    # sqlite:////absolute/path or sqlite:///relative/path
    if url.startswith("sqlite:////"):
        path = url.replace("sqlite:////", "/", 1)
    else:
        path = url.replace("sqlite:///", "", 1)

    path = os.path.expanduser(path)
    path_obj = Path(path)
    if path_obj.parent and not path_obj.parent.exists():
        path_obj.parent.mkdir(parents=True, exist_ok=True)
    return str(path_obj)


def _get_connection() -> sqlite3.Connection:
    global _CONNECTION
    if _CONNECTION is None:
        db_path = _resolve_db_path()
        _CONNECTION = sqlite3.connect(db_path, check_same_thread=False)
        _CONNECTION.row_factory = sqlite3.Row
    return _CONNECTION


@contextmanager
def _cursor() -> Iterable[sqlite3.Cursor]:
    with _DB_LOCK:
        conn = _get_connection()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        finally:
            cur.close()


def init_db() -> None:
    """Initialise the SQLite database and seed defaults."""

    global _INITIALISED
    with _DB_LOCK:
        conn = _get_connection()
        if _INITIALISED:
            return

        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                title TEXT,
                url TEXT,
                enabled INTEGER DEFAULT 0,
                status TEXT,
                last_check_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS cities (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                title TEXT,
                ord INTEGER
            );

            CREATE TABLE IF NOT EXISTS watches (
                id INTEGER PRIMARY KEY,
                category_id INTEGER,
                city_id INTEGER,
                enabled INTEGER DEFAULT 0,
                status TEXT,
                last_seen_value TEXT,
                last_seen_at TEXT,
                last_check_at TEXT,
                error_msg TEXT,
                UNIQUE(category_id, city_id),
                FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE,
                FOREIGN KEY(city_id) REFERENCES cities(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS findings (
                id INTEGER PRIMARY KEY,
                watch_id INTEGER,
                found_value TEXT,
                found_at TEXT,
                notified_at TEXT,
                FOREIGN KEY(watch_id) REFERENCES watches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                ok INTEGER,
                errors INTEGER,
                findings INTEGER,
                scope TEXT
            );

            CREATE TABLE IF NOT EXISTS messages (
                key TEXT PRIMARY KEY,
                chat_id INTEGER,
                message_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS diagnostics (
                id INTEGER PRIMARY KEY,
                recorded_at TEXT,
                category_code TEXT,
                city_key TEXT,
                url TEXT,
                status TEXT,
                http_status INTEGER,
                content_len INTEGER,
                anchor_hash TEXT,
                diff_len INTEGER,
                diff_anchor TEXT,
                comment TEXT
            );

            CREATE TABLE IF NOT EXISTS portal_pulses (
                id INTEGER PRIMARY KEY,
                recorded_at TEXT,
                status TEXT,
                latency_ms INTEGER,
                http_status INTEGER,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS screenshots (
                id INTEGER PRIMARY KEY,
                name TEXT,
                path TEXT,
                description TEXT,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS pulses (
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                kind TEXT,
                status TEXT,
                note TEXT
            );
            """
        )

        _seed(conn)
        _INITIALISED = True


def _seed(conn: sqlite3.Connection) -> None:
    category_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    if category_count == 0:
        for order, (key, title) in enumerate(DEFAULT_CATEGORIES, start=1):
            conn.execute(
                "INSERT INTO categories(key, title, url, enabled, status, last_check_at, last_error) "
                "VALUES (?, ?, '', 0, 'PAUSED', NULL, NULL)",
                (key, title),
            )

    city_count = conn.execute("SELECT COUNT(*) FROM cities").fetchone()[0]
    if city_count == 0:
        for order, (key, title) in enumerate(DEFAULT_CITIES, start=1):
            conn.execute(
                "INSERT INTO cities(key, title, ord) VALUES (?, ?, ?)",
                (key, title, order),
            )

    # Ensure watches exist for each combination.
    category_rows = conn.execute("SELECT id FROM categories").fetchall()
    city_rows = conn.execute("SELECT id FROM cities").fetchall()
    for cat in category_rows:
        for city in city_rows:
            conn.execute(
                "INSERT OR IGNORE INTO watches("
                "category_id, city_id, enabled, status, last_seen_value, last_seen_at, last_check_at, error_msg) "
                "VALUES (?, ?, 0, 'PAUSED', NULL, NULL, NULL, NULL)",
                (cat["id"], city["id"]),
            )

    conn.commit()


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return dict(row)


def get_categories() -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute("SELECT * FROM categories ORDER BY id").fetchall()
        return [dict(row) for row in rows]


def get_category(key: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        row = cur.execute("SELECT * FROM categories WHERE key = ?", (key,)).fetchone()
        return _row_to_dict(row)


def update_category_url(key: str, url: str) -> None:
    with _cursor() as cur:
        cur.execute("UPDATE categories SET url = ? WHERE key = ?", (url, key))


def set_category_enabled(key: str, on: bool) -> None:
    snapshot_key = f"category_snapshot:{key}"
    with _cursor() as cur:
        cur.execute("UPDATE categories SET enabled = ? WHERE key = ?", (1 if on else 0, key))

    if on:
        snapshot = settings_get(snapshot_key)
        if snapshot:
            ids = [int(part) for part in snapshot.split(",") if part]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                with _cursor() as cur:
                    cur.execute(
                        f"UPDATE watches SET enabled = 1 WHERE id IN ({placeholders})",
                        ids,
                    )
            settings_delete(snapshot_key)
    else:
        watches = get_watches_by_category(key)
        enabled_ids = [str(w["id"]) for w in watches if w["enabled"]]
        if enabled_ids:
            settings_set(snapshot_key, ",".join(enabled_ids))
        with _cursor() as cur:
            cur.execute(
                "UPDATE watches SET enabled = 0 WHERE category_id = (SELECT id FROM categories WHERE key = ?)",
                (key,),
            )


def set_category_status(
    key: str,
    status: str,
    *,
    last_check_at: Optional[str] = None,
    last_error: Optional[str] = None,
) -> None:
    timestamp = last_check_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            "UPDATE categories SET status = ?, last_check_at = ?, last_error = ? WHERE key = ?",
            (status, timestamp, last_error, key),
        )


def get_cities() -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute("SELECT * FROM cities ORDER BY ord, id").fetchall()
        return [dict(row) for row in rows]


def get_city(key: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        row = cur.execute("SELECT * FROM cities WHERE key = ?", (key,)).fetchone()
        return _row_to_dict(row)


def get_watches_by_category(cat_key: str) -> List[Dict[str, Any]]:
    query = (
        "SELECT w.*, c.key AS city_key, c.title AS city_title, c.ord, "
        "cat.key AS category_key, cat.title AS category_title, cat.enabled AS category_enabled, "
        "cat.status AS category_status, cat.last_check_at AS category_last_check_at, cat.last_error AS category_last_error, "
        "cat.url AS category_url "
        "FROM watches w "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "WHERE cat.key = ? "
        "ORDER BY c.ord"
    )
    with _cursor() as cur:
        rows = cur.execute(query, (cat_key,)).fetchall()
        return [dict(row) for row in rows]


def get_watch(cat_key: str, city_key: str) -> Optional[Dict[str, Any]]:
    query = (
        "SELECT w.*, cat.key AS category_key, cat.enabled AS category_enabled, c.key AS city_key, c.title AS city_title "
        "FROM watches w "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "WHERE cat.key = ? AND c.key = ?"
    )
    with _cursor() as cur:
        row = cur.execute(query, (cat_key, city_key)).fetchone()
        return _row_to_dict(row)


def enable_watch(cat_key: str, city_key: str, on: bool) -> None:
    watch = get_watch(cat_key, city_key)
    if not watch:
        return
    with _cursor() as cur:
        cur.execute("UPDATE watches SET enabled = ? WHERE id = ?", (1 if on else 0, watch["id"]))
    flag_key = f"watch_manual_off:{watch['id']}"
    if on:
        settings_delete(flag_key)
    else:
        settings_set(flag_key, "1")


def enable_all_watches(cat_key: str, on: bool) -> None:
    with _cursor() as cur:
        cur.execute(
            "UPDATE watches SET enabled = ? WHERE category_id = (SELECT id FROM categories WHERE key = ?)",
            (1 if on else 0, cat_key),
        )
    if on:
        settings_delete(f"category_snapshot:{cat_key}")
        watches = get_watches_by_category(cat_key)
        for watch in watches:
            settings_delete(f"watch_manual_off:{watch['id']}")


def get_enabled_categories() -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute("SELECT * FROM categories WHERE enabled = 1 ORDER BY id").fetchall()
        return [dict(row) for row in rows]


def get_enabled_watches() -> List[Dict[str, Any]]:
    query = (
        "SELECT w.*, cat.key AS category_key, cat.title AS category_title, cat.enabled AS category_enabled, "
        "c.key AS city_key, c.title AS city_title, c.ord "
        "FROM watches w "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "WHERE w.enabled = 1 AND cat.enabled = 1 "
        "ORDER BY cat.id, c.ord"
    )
    with _cursor() as cur:
        rows = cur.execute(query).fetchall()
        return [dict(row) for row in rows]


def update_watch_result(
    watch_id: int,
    status: str,
    *,
    last_seen_value: Optional[str] = None,
    last_seen_at: Optional[str] = None,
    error_msg: Optional[str] = None,
    last_check_at: Optional[str] = None,
) -> None:
    fields: List[str] = ["status = ?"]
    params: List[Any] = [status]
    if last_seen_value is not None:
        fields.append("last_seen_value = ?")
        params.append(last_seen_value)
    if last_seen_at is not None:
        fields.append("last_seen_at = ?")
        params.append(last_seen_at)
    if error_msg is not None:
        fields.append("error_msg = ?")
        params.append(error_msg)
    timestamp = last_check_at or datetime.utcnow().isoformat()
    fields.append("last_check_at = ?")
    params.append(timestamp)
    params.append(watch_id)
    with _cursor() as cur:
        cur.execute(f"UPDATE watches SET {', '.join(fields)} WHERE id = ?", params)


def reset_watches_for_category(cat_key: str, status: str, error: Optional[str] = None) -> None:
    with _cursor() as cur:
        cur.execute(
            "UPDATE watches SET status = ?, error_msg = ?, last_check_at = ? "
            "WHERE category_id = (SELECT id FROM categories WHERE key = ?)",
            (status, error, datetime.utcnow().isoformat(), cat_key),
        )


def record_finding(watch_id: int, value: str, when_iso: Optional[str] = None) -> Optional[int]:
    timestamp = when_iso or datetime.utcnow().isoformat()
    with _cursor() as cur:
        last = cur.execute(
            "SELECT found_value FROM findings WHERE watch_id = ? ORDER BY found_at DESC LIMIT 1",
            (watch_id,),
        ).fetchone()
        if last and last["found_value"] == value:
            return None
        cur.execute(
            "INSERT INTO findings(watch_id, found_value, found_at, notified_at) VALUES (?, ?, ?, NULL)",
            (watch_id, value, timestamp),
        )
        return cur.lastrowid


def get_pending_findings() -> List[Dict[str, Any]]:
    query = (
        "SELECT f.*, w.category_id, w.city_id, w.status, w.last_seen_value, w.last_seen_at, "
        "cat.key AS category_key, cat.title AS category_title, "
        "c.key AS city_key, c.title AS city_title "
        "FROM findings f "
        "JOIN watches w ON w.id = f.watch_id "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "WHERE f.notified_at IS NULL "
        "ORDER BY f.found_at"
    )
    with _cursor() as cur:
        rows = cur.execute(query).fetchall()
        return [dict(row) for row in rows]


def get_recent_findings(limit: int = 5) -> List[Dict[str, Any]]:
    query = (
        "SELECT f.*, w.category_id, w.city_id, cat.key AS category_key, cat.title AS category_title, "
        "c.key AS city_key, c.title AS city_title "
        "FROM findings f "
        "JOIN watches w ON w.id = f.watch_id "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "ORDER BY f.found_at DESC LIMIT ?"
    )
    with _cursor() as cur:
        rows = cur.execute(query, (limit,)).fetchall()
        return [dict(row) for row in rows]


def mark_finding_notified(finding_id: int, when_iso: Optional[str] = None) -> None:
    timestamp = when_iso or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            "UPDATE findings SET notified_at = ? WHERE id = ?",
            (timestamp, finding_id),
        )


def record_diagnostic(
    *,
    recorded_at: Optional[str],
    category_code: str,
    city_key: str,
    url: str,
    status: str,
    http_status: Optional[int],
    content_len: int,
    anchor_hash: str,
    diff_len: int,
    diff_anchor: str,
    comment: str,
) -> None:
    timestamp = recorded_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO diagnostics(
                recorded_at, category_code, city_key, url, status, http_status,
                content_len, anchor_hash, diff_len, diff_anchor, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                category_code,
                city_key,
                url,
                status,
                http_status,
                content_len,
                anchor_hash,
                diff_len,
                diff_anchor,
                comment,
            ),
        )


def get_last_diagnostic(category_code: str, city_key: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        row = cur.execute(
            """
            SELECT * FROM diagnostics
            WHERE category_code = ? AND city_key = ?
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
            """,
            (category_code, city_key),
        ).fetchone()
    return _row_to_dict(row)


def get_latest_diagnostics(limit: int = 100) -> List[Dict[str, Any]]:
    query = """
        SELECT d.*
        FROM diagnostics d
        INNER JOIN (
            SELECT category_code, city_key, MAX(recorded_at) AS recorded_at
            FROM diagnostics
            GROUP BY category_code, city_key
        ) latest
        ON latest.category_code = d.category_code
        AND latest.city_key = d.city_key
        AND latest.recorded_at = d.recorded_at
        ORDER BY d.recorded_at DESC
        LIMIT ?
    """
    with _cursor() as cur:
        rows = cur.execute(query, (limit,)).fetchall()
        return [dict(row) for row in rows]


def record_portal_pulse(
    *,
    recorded_at: Optional[str],
    status: str,
    latency_ms: Optional[int],
    http_status: Optional[int],
    error: Optional[str],
) -> None:
    timestamp = recorded_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO portal_pulses(recorded_at, status, latency_ms, http_status, error)
            VALUES (?, ?, ?, ?, ?)
            """,
            (timestamp, status, latency_ms, http_status, error),
        )


def get_recent_portal_pulses(limit: int = 10) -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute(
            "SELECT * FROM portal_pulses ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def record_screenshot(name: str, path: str, description: str, *, created_at: Optional[str] = None) -> None:
    timestamp = created_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO screenshots(name, path, description, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (name, path, description, timestamp),
        )


def get_recent_screenshots(limit: int = 5) -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute(
            "SELECT * FROM screenshots ORDER BY created_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_screenshot(name: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        row = cur.execute(
            "SELECT * FROM screenshots WHERE name = ? ORDER BY created_at DESC LIMIT 1",
            (name,),
        ).fetchone()
    return _row_to_dict(row)


def record_pulse(kind: str, status: str, note: str, *, created_at: Optional[str] = None) -> None:
    timestamp = created_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO pulses(created_at, kind, status, note) VALUES (?, ?, ?, ?)",
            (timestamp, kind, status, note),
        )


def get_recent_pulses(limit: int = 10) -> List[Dict[str, Any]]:
    with _cursor() as cur:
        rows = cur.execute(
            "SELECT * FROM pulses ORDER BY created_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def save_anchor(name: str, chat_id: int, message_id: int) -> None:
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO messages(key, chat_id, message_id) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET chat_id = excluded.chat_id, message_id = excluded.message_id",
            (name, chat_id, message_id),
        )


def get_anchor(name: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        row = cur.execute("SELECT chat_id, message_id FROM messages WHERE key = ?", (name,)).fetchone()
    if not row:
        return None
    return {"chat_id": row["chat_id"], "message_id": row["message_id"]}


def settings_get(key: str, default: Optional[str] = None) -> Optional[str]:
    with _cursor() as cur:
        row = cur.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return row["value"]


def settings_set(key: str, value: str) -> None:
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def settings_delete(key: str) -> None:
    with _cursor() as cur:
        cur.execute("DELETE FROM settings WHERE key = ?", (key,))


def count_watches() -> Dict[str, int]:
    query = (
        "SELECT "
        "COUNT(*) AS total, "
        "SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled, "
        "SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors "
        "FROM watches"
    )
    with _cursor() as cur:
        row = cur.execute(query).fetchone()
    return {
        "total": int(row["total"] or 0),
        "enabled": int(row["enabled"] or 0),
        "errors": int(row["errors"] or 0),
    }


def list_tracked_watches() -> List[Dict[str, Any]]:
    query = (
        "SELECT w.*, cat.key AS category_key, cat.title AS category_title, cat.enabled AS category_enabled, "
        "c.key AS city_key, c.title AS city_title, c.ord "
        "FROM watches w "
        "JOIN categories cat ON cat.id = w.category_id "
        "JOIN cities c ON c.id = w.city_id "
        "WHERE w.enabled = 1 "
        "ORDER BY cat.id, c.ord"
    )
    with _cursor() as cur:
        rows = cur.execute(query).fetchall()
        return [dict(row) for row in rows]


def create_run(started_at: Optional[str], scope: str) -> int:
    ts = started_at or datetime.utcnow().isoformat()
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO runs(started_at, finished_at, ok, errors, findings, scope) VALUES (?, NULL, 0, 0, 0, ?)",
            (ts, scope),
        )
        return cur.lastrowid


def finish_run(run_id: int, *, ok: int, errors: int, findings: int) -> None:
    with _cursor() as cur:
        cur.execute(
            "UPDATE runs SET finished_at = ?, ok = ?, errors = ?, findings = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), ok, errors, findings, run_id),
        )


__all__ = [
    "init_db",
    "get_categories",
    "get_category",
    "update_category_url",
    "set_category_enabled",
    "set_category_status",
    "get_cities",
    "get_city",
    "get_watches_by_category",
    "get_watch",
    "enable_watch",
    "enable_all_watches",
    "get_enabled_categories",
    "get_enabled_watches",
    "update_watch_result",
    "reset_watches_for_category",
    "record_finding",
    "get_pending_findings",
    "get_recent_findings",
    "mark_finding_notified",
    "save_anchor",
    "get_anchor",
    "settings_get",
    "settings_set",
    "settings_delete",
    "count_watches",
    "list_tracked_watches",
    "create_run",
    "finish_run",
    "record_diagnostic",
    "get_last_diagnostic",
    "get_latest_diagnostics",
    "record_portal_pulse",
    "get_recent_portal_pulses",
    "record_screenshot",
    "get_recent_screenshots",
    "get_screenshot",
    "record_pulse",
    "get_recent_pulses",
]
