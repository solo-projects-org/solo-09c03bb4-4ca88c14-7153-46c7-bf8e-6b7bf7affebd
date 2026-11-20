"""Core feed logic: revision-checked ingest, minute-bar recompute, cursor pulls.

Consistency rules:
  * Every ingest runs in ONE immediate transaction: trade-state upsert +
    minute-bar recompute + journal insert commit or roll back together.
    A failed write or a crash therefore never leaves a half-applied update,
    and a committed journal seq (cursor) can never be skipped afterwards.
  * Reads (snapshot / pull) run in a single read transaction, so clients only
    ever see fully committed changes.
"""
from __future__ import annotations

import json
import re
import sqlite3
from typing import Any, Optional

MINUTE_MS = 60_000
INT64_MAX = 9_223_372_036_854_775_807
_INT64_MAX_STR = str(INT64_MAX)

# One-off data-repair marker: recompute bars affected by the old sorting bug.
SORT_KEY_UPGRADE_KEY = "sort_key_upgrade_v1"

# Only ASCII digits count as numeric trade numbers; anything else sorts as text.
_DIGITS_RE = re.compile(r"[0-9]+")

BAR_KEYS = (
    "minute_start_utc_ms",
    "open_cents",
    "high_cents",
    "low_cents",
    "close_cents",
    "volume",
    "trade_count",
)


class ConflictError(Exception):
    """Same revision already stored with different content -> HTTP 409."""


class CursorAheadError(Exception):
    """Client cursor is ahead of the committed journal -> HTTP 409."""

    def __init__(self, cursor: int, max_seq: int):
        self.cursor = cursor
        self.max_seq = max_seq
        super().__init__(
            f"cursor {cursor} 超过当前已提交的最大序号 {max_seq}，请重新快照"
        )


def minute_floor(ts_ms: int) -> int:
    return ts_ms - (ts_ms % MINUTE_MS)


def _numeric_digits(trade_no: str) -> Optional[str]:
    """Canonical digit string without leading zeros, or None if not all digits."""
    if _DIGITS_RE.fullmatch(trade_no) is None:
        return None
    return trade_no.lstrip("0") or "0"


def _trade_no_key(trade_no: str) -> tuple:
    """Total order on trade numbers: all-digit numbers compare numerically
    (leading zeros irrelevant), anything else compares as text and sorts after
    all numeric ones. Numeric order is expressed as (length, digits), which is
    exact for any number of digits -- no int conversion, so no 64-bit limit and
    no Python large-int digit cap. The key is never stored in SQLite.
    """
    digits = _numeric_digits(trade_no)
    if digits is not None:
        # Raw value only breaks exact numeric ties ("123" vs "000123"), so the
        # order stays deterministic without making leading zeros significant.
        return (0, len(digits), digits, trade_no)
    return (1, 0, trade_no, trade_no)


def _is_overflow_trade_no(trade_no: str) -> bool:
    """True for all-ASCII-digit trade numbers that do not fit in signed int64.

    The old code cached the numeric key in an INTEGER column and fell back to
    text comparison when the value did not fit, which is exactly the set whose
    minute bars may have been ordered incorrectly.
    """
    digits = _numeric_digits(trade_no)
    if digits is None:
        return False
    return len(digits) > len(_INT64_MAX_STR) or (
        len(digits) == len(_INT64_MAX_STR) and digits > _INT64_MAX_STR
    )


def _incoming_content(msg) -> tuple:
    if msg.op == "cancel":
        return ("cancel", None, None, None)
    return (msg.op, msg.trade_time_utc_ms, msg.price_cents, msg.qty)


def _stored_content(row: sqlite3.Row) -> tuple:
    return (row["op"], row["trade_time_utc_ms"], row["price_cents"], row["qty"])


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

def apply_message(conn: sqlite3.Connection, msg) -> dict[str, Any]:
    """Apply one message. Returns {status, current_revision, affected_minutes}.

    Revision rules per (symbol, trade_no):
      revision <  stored : stale_ignored -- late message, never rolls back state
      revision == stored : identical content -> duplicate (idempotent);
                           different content  -> ConflictError (409)
      revision >  stored : applied (gaps allowed)
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM trades WHERE symbol = ? AND trade_no = ?",
            (msg.symbol, msg.trade_no),
        ).fetchone()

        if row is not None and msg.revision < row["revision"]:
            conn.execute("COMMIT")
            return {
                "status": "stale_ignored",
                "current_revision": row["revision"],
                "affected_minutes": [],
            }

        if row is not None and msg.revision == row["revision"]:
            if _stored_content(row) == _incoming_content(msg):
                conn.execute("COMMIT")
                return {
                    "status": "duplicate",
                    "current_revision": row["revision"],
                    "affected_minutes": [],
                }
            raise ConflictError(
                f"{msg.symbol}/{msg.trade_no} 修订号 {msg.revision} "
                "已存在且内容不同，拒绝写入"
            )

        old_minute: Optional[int] = None
        if row is not None and not row["cancelled"]:
            old_minute = minute_floor(row["trade_time_utc_ms"])

        cancelled = msg.op == "cancel"
        conn.execute(
            """
            INSERT INTO trades (symbol, trade_no, revision, op,
                                trade_time_utc_ms, price_cents, qty,
                                cancelled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_no) DO UPDATE SET
                revision          = excluded.revision,
                op                = excluded.op,
                trade_time_utc_ms = excluded.trade_time_utc_ms,
                price_cents       = excluded.price_cents,
                qty               = excluded.qty,
                cancelled         = excluded.cancelled
            """,
            (
                msg.symbol,
                msg.trade_no,
                msg.revision,
                msg.op,
                None if cancelled else msg.trade_time_utc_ms,
                None if cancelled else msg.price_cents,
                None if cancelled else msg.qty,
                1 if cancelled else 0,
            ),
        )

        new_minute = None if cancelled else minute_floor(msg.trade_time_utc_ms)

        # Cross-minute corrections touch both the old and the new minute;
        # both recomputes (and their journal rows) commit in this transaction.
        affected = []
        for minute in sorted({m for m in (old_minute, new_minute) if m is not None}):
            if recompute_minute(conn, msg.symbol, minute):
                affected.append(minute)

        conn.execute("COMMIT")
        return {
            "status": "applied",
            "current_revision": msg.revision,
            "affected_minutes": affected,
        }
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# minute bars
# ---------------------------------------------------------------------------

def _trade_order_key(row: sqlite3.Row) -> tuple:
    # Open/close are determined by (trade_time_utc_ms, trade_no); all-digit
    # trade numbers compare numerically, others lexicographically.
    return (row["trade_time_utc_ms"], _trade_no_key(row["trade_no"]))


def _build_bar(conn: sqlite3.Connection, symbol: str, minute_ms: int) -> Optional[dict]:
    """Materialize the bar for one minute from active trades, or None if empty."""
    rows = conn.execute(
        """
        SELECT trade_time_utc_ms, trade_no, price_cents, qty
        FROM trades
        WHERE symbol = ? AND cancelled = 0
          AND trade_time_utc_ms >= ? AND trade_time_utc_ms < ?
        """,
        (symbol, minute_ms, minute_ms + MINUTE_MS),
    ).fetchall()
    if not rows:
        return None

    ordered = sorted(rows, key=_trade_order_key)
    return {
        "minute_start_utc_ms": minute_ms,
        "open_cents": ordered[0]["price_cents"],
        "high_cents": max(r["price_cents"] for r in ordered),
        "low_cents": min(r["price_cents"] for r in ordered),
        "close_cents": ordered[-1]["price_cents"],
        "volume": sum(r["qty"] for r in ordered),
        "trade_count": len(ordered),
    }


def _write_bar(conn: sqlite3.Connection, symbol: str, bar: dict) -> None:
    conn.execute(
        """
        INSERT INTO minute_bars (symbol, minute_start_utc_ms, open_cents, high_cents,
                                 low_cents, close_cents, volume, trade_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, minute_start_utc_ms) DO UPDATE SET
            open_cents  = excluded.open_cents,
            high_cents  = excluded.high_cents,
            low_cents   = excluded.low_cents,
            close_cents = excluded.close_cents,
            volume      = excluded.volume,
            trade_count = excluded.trade_count
        """,
        (
            symbol,
            bar["minute_start_utc_ms"],
            bar["open_cents"],
            bar["high_cents"],
            bar["low_cents"],
            bar["close_cents"],
            bar["volume"],
            bar["trade_count"],
        ),
    )


def recompute_minute(conn: sqlite3.Connection, symbol: str, minute_ms: int) -> bool:
    """Rebuild one minute bar from active trades and journal the visible change.

    Returns True if a journal row was written (bar created/updated/deleted).
    Must be called inside the ingest transaction.
    """
    existing = conn.execute(
        "SELECT * FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
        (symbol, minute_ms),
    ).fetchone()
    bar = _build_bar(conn, symbol, minute_ms)

    if bar is None:
        # Last trade of the minute disappeared -> delete marker, but only if a
        # bar was ever published for this minute.
        if existing is None:
            return False
        conn.execute(
            "DELETE FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
            (symbol, minute_ms),
        )
        _journal(conn, symbol, minute_ms, "delete", None)
        return True

    if existing is not None and all(existing[k] == v for k, v in bar.items()):
        return False  # visible state unchanged -> no journal noise

    _write_bar(conn, symbol, bar)
    _journal(conn, symbol, minute_ms, "upsert", bar)
    return True


def _journal(
    conn: sqlite3.Connection,
    symbol: str,
    minute_ms: int,
    change_type: str,
    bar: Optional[dict],
) -> None:
    conn.execute(
        """
        INSERT INTO journal (symbol, minute_start_utc_ms, change_type, bar_json)
        VALUES (?, ?, ?, ?)
        """,
        (symbol, minute_ms, change_type, None if bar is None else json.dumps(bar, sort_keys=True)),
    )


# ---------------------------------------------------------------------------
# one-off upgrade of databases written by the old (int64-only) sort key
# ---------------------------------------------------------------------------

def upgrade_sort_key(conn: sqlite3.Connection) -> int:
    """Repair minute bars whose open/close may be swapped by the legacy order.

    The old version cached the numeric trade-number key in a SQLite INTEGER
    column. Numbers beyond int64 did not fit, so they fell back to text order.
    This recomputes ONLY the minutes that contain such a not-cancelled trade
    and publishes a correction only where the visible open or close price
    actually changes. The recompute, its journal row and the upgrade marker
    commit in one transaction:

      * bars and journal commit or roll back together (no half-updated feed);
      * the marker commits with them, so a restart never re-applies the repair;
      * if the transaction fails, everything rolls back and the next start
        safely retries (recompute is idempotent);
      * journal rows are appended after all existing seqs, so a cursor saved
        before the upgrade will receive the corrected bars.

    Returns the number of journal rows written (0 on repeat runs).
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        done = conn.execute(
            "SELECT 1 FROM meta WHERE key = ?", (SORT_KEY_UPGRADE_KEY,)
        ).fetchone()
        if done is not None:
            conn.execute("COMMIT")
            return 0

        # Candidate minutes: every not-cancelled trade whose numeric number does
        # not fit int64 (the set the legacy sort key could order wrongly).
        # Filtering in Python keeps the comparison exact for arbitrarily long
        # digit strings (SQLite INTEGER would overflow).
        candidates = conn.execute(
            """
            SELECT symbol, trade_no, trade_time_utc_ms
            FROM trades
            WHERE cancelled = 0 AND trade_time_utc_ms IS NOT NULL
            """
        ).fetchall()
        minutes = sorted({
            (row["symbol"], minute_floor(row["trade_time_utc_ms"]))
            for row in candidates
            if _is_overflow_trade_no(row["trade_no"])
        })

        repaired = 0
        for symbol, minute_ms in minutes:
            existing = conn.execute(
                "SELECT * FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
                (symbol, minute_ms),
            ).fetchone()
            if existing is None:
                continue  # no published bar -> nothing a cursor could have seen
            bar = _build_bar(conn, symbol, minute_ms)
            if bar is None:
                continue
            if (existing["open_cents"] == bar["open_cents"]
                    and existing["close_cents"] == bar["close_cents"]):
                continue  # order fix changes only open/close; nothing to publish
            _write_bar(conn, symbol, bar)
            _journal(conn, symbol, minute_ms, "upsert", bar)
            repaired += 1

        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (SORT_KEY_UPGRADE_KEY, "done"),
        )
        conn.execute("COMMIT")
        return repaired
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def snapshot_bars(conn: sqlite3.Connection, symbol: str) -> dict[str, Any]:
    """First query: all current minute bars plus a cursor consistent with them."""
    conn.execute("BEGIN")
    try:
        cursor = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM journal").fetchone()[0]
        rows = conn.execute(
            "SELECT * FROM minute_bars WHERE symbol = ? ORDER BY minute_start_utc_ms",
            (symbol,),
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {
        "symbol": symbol,
        "cursor": cursor,
        "bars": [{k: row[k] for k in BAR_KEYS} for row in rows],
    }


def pull_changes(
    conn: sqlite3.Connection, symbol: str, cursor: int, limit: int
) -> dict[str, Any]:
    """Incremental pull: committed changes for one symbol with seq > cursor."""
    conn.execute("BEGIN")
    try:
        max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM journal").fetchone()[0]
        if cursor > max_seq:
            raise CursorAheadError(cursor, max_seq)
        rows = conn.execute(
            "SELECT * FROM journal WHERE symbol = ? AND seq > ? ORDER BY seq LIMIT ?",
            (symbol, cursor, limit),
        ).fetchall()
        has_more = len(rows) == limit
        # Fewer rows than the limit => we saw everything committed up to max_seq.
        next_cursor = rows[-1]["seq"] if has_more else max_seq
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    changes = [
        {
            "seq": r["seq"],
            "minute_start_utc_ms": r["minute_start_utc_ms"],
            "type": r["change_type"],
            "bar": None if r["bar_json"] is None else json.loads(r["bar_json"]),
        }
        for r in rows
    ]
    return {
        "symbol": symbol,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "changes": changes,
    }


def get_trade(conn: sqlite3.Connection, symbol: str, trade_no: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM trades WHERE symbol = ? AND trade_no = ?", (symbol, trade_no)
    ).fetchone()
    if row is None:
        return None
    return {
        "symbol": row["symbol"],
        "trade_no": row["trade_no"],
        "revision": row["revision"],
        "op": row["op"],
        "trade_time_utc_ms": row["trade_time_utc_ms"],
        "price_cents": row["price_cents"],
        "qty": row["qty"],
        "cancelled": bool(row["cancelled"]),
    }
