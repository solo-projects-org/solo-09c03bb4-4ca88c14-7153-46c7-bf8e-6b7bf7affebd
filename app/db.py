"""SQLite storage layer.

Tables:
  trades      -- current state per (symbol, trade_no), keyed by revision
  minute_bars -- current materialized minute bar per (symbol, minute)
  journal     -- append-only change log; its seq is the cursor risk control uses
  meta        -- one-off upgrade markers (e.g. sort-key backfill done)

WAL + synchronous=FULL: a committed transaction survives a process restart or
power loss, so a cursor a client has already seen can never be "uncommitted".
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    symbol            TEXT    NOT NULL,
    trade_no          TEXT    NOT NULL,
    revision          INTEGER NOT NULL,
    op                TEXT    NOT NULL CHECK (op IN ('trade', 'correction', 'cancel')),
    trade_time_utc_ms INTEGER,
    price_cents       INTEGER,
    qty               INTEGER,
    cancelled         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, trade_no)
);

CREATE TABLE IF NOT EXISTS minute_bars (
    symbol              TEXT    NOT NULL,
    minute_start_utc_ms INTEGER NOT NULL,
    open_cents          INTEGER NOT NULL,
    high_cents          INTEGER NOT NULL,
    low_cents           INTEGER NOT NULL,
    close_cents         INTEGER NOT NULL,
    volume              INTEGER NOT NULL,
    trade_count         INTEGER NOT NULL,
    PRIMARY KEY (symbol, minute_start_utc_ms)
);

CREATE TABLE IF NOT EXISTS journal (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    minute_start_utc_ms INTEGER NOT NULL,
    change_type         TEXT    NOT NULL CHECK (change_type IN ('upsert', 'delete')),
    bar_json            TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_journal_symbol_seq ON journal (symbol, seq);
CREATE INDEX IF NOT EXISTS idx_trades_minute ON trades (symbol, cancelled, trade_time_utc_ms);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None -> autocommit; transactions are opened explicitly
    # with BEGIN / BEGIN IMMEDIATE in logic.py so commit boundaries are exact.
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: str) -> None:
    path = Path(db_path)
    if path.parent and str(path.parent) not in ("", "."):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(SCHEMA)
        # One-off repair of bars ordered by the legacy int64-only sort key.
        # Idempotent: a meta marker makes later restarts a no-op.
        from . import logic

        logic.upgrade_sort_key(conn)
    finally:
        conn.close()
