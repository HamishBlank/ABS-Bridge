"""
db.py — SQLite persistence layer for ABS Cast Bridge.

Tables:
  libraries   — cached ABS library list
  series      — cached series per library, with cover item id
  devices     — discovered Chromecast devices
  sessions    — all cast sessions (active + historical)

Cache TTLs are configurable. A background refresh thread keeps data fresh
so the UI never needs to trigger a manual load.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "/data/abscast.db")
CACHE_TTL_LIBRARIES = int(os.getenv("CACHE_TTL_LIBRARIES", "600"))   # 10 min
CACHE_TTL_SERIES    = int(os.getenv("CACHE_TTL_SERIES",    "600"))   # 10 min
CACHE_TTL_DEVICES   = int(os.getenv("CACHE_TTL_DEVICES",   "300"))   # 5 min

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Return a thread-local connection, creating it if needed."""
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


@contextmanager
def tx():
    """Context manager for a committed transaction."""
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    """Create tables if they don't exist."""
    with tx() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS libraries (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                media_type  TEXT,
                refreshed_at REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS series (
                id              TEXT PRIMARY KEY,
                library_id      TEXT NOT NULL,
                name            TEXT NOT NULL,
                book_count      INTEGER DEFAULT 0,
                cover_item_id   TEXT,
                cover_url       TEXT,
                qr_url          TEXT,
                refreshed_at    REAL NOT NULL DEFAULT 0,
                FOREIGN KEY (library_id) REFERENCES libraries(id)
            );

            CREATE INDEX IF NOT EXISTS idx_series_library ON series(library_id);

            CREATE TABLE IF NOT EXISTS devices (
                name        TEXT PRIMARY KEY,
                host        TEXT,
                port        INTEGER,
                model       TEXT,
                is_default  INTEGER DEFAULT 0,
                refreshed_at REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id      TEXT PRIMARY KEY,
                device_name     TEXT,
                book_id         TEXT,
                book_title      TEXT,
                book_author     TEXT,
                series_name     TEXT,
                stream_url      TEXT,
                start_time      REAL DEFAULT 0,
                duration        REAL DEFAULT 0,
                current_time    REAL DEFAULT 0,
                state           TEXT DEFAULT 'IDLE',
                error           TEXT DEFAULT '',
                series_restart  INTEGER DEFAULT 0,
                created_at      REAL NOT NULL DEFAULT 0,
                updated_at      REAL NOT NULL DEFAULT 0,
                stopped_at      REAL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_state ON sessions(state);
            CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at DESC);
        """)
    log.info(f"Database ready at {DB_PATH}")


# ---------------------------------------------------------------------------
# Libraries
# ---------------------------------------------------------------------------

def get_libraries_cached() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM libraries ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def upsert_libraries(libs: list[dict]):
    now = time.time()
    with tx() as conn:
        conn.execute("DELETE FROM libraries")
        conn.executemany(
            "INSERT OR REPLACE INTO libraries (id, name, media_type, refreshed_at) VALUES (?,?,?,?)",
            [(l["id"], l["name"], l.get("mediaType", ""), now) for l in libs]
        )
    log.info(f"Cached {len(libs)} libraries")


def libraries_stale() -> bool:
    conn = get_conn()
    row = conn.execute("SELECT MIN(refreshed_at) as oldest FROM libraries").fetchone()
    if not row or not row["oldest"]:
        return True
    return (time.time() - row["oldest"]) > CACHE_TTL_LIBRARIES


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------

def get_series_cached(library_id: Optional[str] = None) -> list[dict]:
    conn = get_conn()
    if library_id:
        rows = conn.execute(
            "SELECT s.*, l.name as library_name FROM series s "
            "JOIN libraries l ON s.library_id = l.id "
            "WHERE s.library_id = ? ORDER BY s.name",
            (library_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT s.*, l.name as library_name FROM series s "
            "JOIN libraries l ON s.library_id = l.id ORDER BY l.name, s.name"
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_series(library_id: str, series_list: list[dict]):
    now = time.time()
    with tx() as conn:
        conn.execute("DELETE FROM series WHERE library_id = ?", (library_id,))
        conn.executemany(
            """INSERT OR REPLACE INTO series
               (id, library_id, name, book_count, cover_item_id, cover_url, qr_url, refreshed_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            [(
                s["series_id"],
                library_id,
                s["series_name"],
                s.get("book_count", 0),
                s.get("cover_item_id"),
                s.get("cover_url"),
                s.get("qr_url"),
                now,
            ) for s in series_list]
        )
    log.info(f"Cached {len(series_list)} series for library {library_id}")


def series_stale(library_id: Optional[str] = None) -> bool:
    conn = get_conn()
    if library_id:
        row = conn.execute(
            "SELECT MIN(refreshed_at) as oldest FROM series WHERE library_id = ?",
            (library_id,)
        ).fetchone()
    else:
        row = conn.execute("SELECT MIN(refreshed_at) as oldest FROM series").fetchone()
    if not row or not row["oldest"]:
        return True
    return (time.time() - row["oldest"]) > CACHE_TTL_SERIES


# ---------------------------------------------------------------------------
# Devices
# ---------------------------------------------------------------------------

def get_devices_cached() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM devices ORDER BY is_default DESC, name").fetchall()
    return [dict(r) for r in rows]


def upsert_devices(devices: list[dict]):
    now = time.time()
    with tx() as conn:
        conn.execute("DELETE FROM devices")
        conn.executemany(
            """INSERT OR REPLACE INTO devices (name, host, port, model, is_default, refreshed_at)
               VALUES (?,?,?,?,?,?)""",
            [(d["name"], d["host"], d["port"], d.get("model",""), 1 if d.get("is_default") else 0, now)
             for d in devices]
        )
    log.info(f"Cached {len(devices)} devices")


def devices_stale() -> bool:
    conn = get_conn()
    row = conn.execute("SELECT MIN(refreshed_at) as oldest FROM devices").fetchone()
    if not row or not row["oldest"]:
        return True
    return (time.time() - row["oldest"]) > CACHE_TTL_DEVICES


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def upsert_session(s: dict):
    now = time.time()
    with tx() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO sessions
               (session_id, device_name, book_id, book_title, book_author, series_name,
                stream_url, start_time, duration, current_time, state, error,
                series_restart, created_at, updated_at, stopped_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,
                       COALESCE((SELECT created_at FROM sessions WHERE session_id=?), ?),
                       ?,?)""",
            (
                s["session_id"], s.get("device_name",""), s.get("book_id",""),
                s.get("book_title",""), s.get("book_author",""), s.get("series_name",""),
                s.get("stream_url",""), s.get("start_time",0), s.get("duration",0),
                s.get("current_time",0), s.get("state","IDLE"), s.get("error",""),
                1 if s.get("series_restart") else 0,
                s["session_id"], now,   # for COALESCE created_at
                now,                    # updated_at
                s.get("stopped_at", 0),
            )
        )


def get_active_sessions() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM sessions WHERE state NOT IN ('DONE','STOPPED','ERROR') ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def get_session_history(limit: int = 50) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_session(session_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
    return dict(row) if row else None