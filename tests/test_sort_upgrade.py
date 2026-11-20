"""Trade-number ordering fix and one-off upgrade of legacy databases.

The pre-upgrade version cached an int64 sort key in `trades.trade_no_num`.
Numbers larger than int64 did not fit and fell back to TEXT order, so within
one millisecond the open/close could be swapped. These tests cover the new
ordering and the repair of a database written by the old version.
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import logic
from app.main import create_app

T0 = 1_699_999_980_000          # minute start
T_IN_0 = T0 + 20_000
M0 = T0
INT64_MAX = 9_223_372_036_854_775_807
BIG_A = str(INT64_MAX + 1)      # 9223372036854775808  (19 digits)
BIG_B = "18446744073709551616"  # 2**64                (20 digits)
BIG_C = "99999999999999999999"  # 20 digits


def msg(symbol="600519", trade_no="1", revision=1, op="trade",
        t=T_IN_0, price=10000, qty=100):
    m = {"symbol": symbol, "trade_no": trade_no, "revision": revision, "op": op}
    if op != "cancel":
        m.update(trade_time_utc_ms=t, price_cents=price, qty=qty)
    return m


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def post(client, m):
    return client.post("/v1/trades", json=m)


def bars(client, symbol="600519"):
    return client.get(f"/v1/symbols/{symbol}/bars").json()


# ---------------------------------------------------------------------------
# ordering
# ---------------------------------------------------------------------------

def test_overflow_trade_numbers_compare_numerically(client):
    # Text order says BIG_B < BIG_A ("1..." < "9..."), numeric order says the
    # opposite. Old code (which cached int64) produced the swapped open/close.
    post(client, msg(trade_no=BIG_B, t=T0 + 5_000, price=10000, qty=10))
    post(client, msg(trade_no=BIG_A, t=T0 + 5_000, price=10100, qty=10))

    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10100   # BIG_A (9223...5808) is smaller
    assert bar["close_cents"] == 10000  # BIG_B (1.84e19) is larger


def test_overflow_vs_normal_number_same_millisecond(client):
    # A normal in-range number must still be numerically smaller than BIG_A.
    post(client, msg(trade_no=BIG_A, t=T0 + 5_000, price=10100, qty=10))
    post(client, msg(trade_no="999999999999999999", t=T0 + 5_000, price=10000, qty=10))

    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10000
    assert bar["close_cents"] == 10100


def test_leading_zeros_do_not_change_numeric_order(client):
    # "007" is 7 numerically and must sort before "10" (text order would not).
    post(client, msg(trade_no="10", t=T0 + 5_000, price=10100, qty=10))
    post(client, msg(trade_no="007", t=T0 + 5_000, price=10000, qty=10))

    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10000
    assert bar["close_cents"] == 10100


def test_leading_zeros_large_values(client):
    # Leading zeros on an out-of-int64 number still compare numerically.
    post(client, msg(trade_no="0" + BIG_B, t=T0 + 5_000, price=10000, qty=10))
    post(client, msg(trade_no=BIG_A, t=T0 + 5_000, price=10100, qty=10))
    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10100
    assert bar["close_cents"] == 10000


def test_non_numeric_trade_numbers_still_compare_as_text(client):
    post(client, msg(trade_no="B-2", t=T0 + 5_000, price=10100, qty=10))
    post(client, msg(trade_no="A-9", t=T0 + 5_000, price=10000, qty=10))
    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10000  # "A-9" < "B-2"
    assert bar["close_cents"] == 10100


def test_very_long_digit_trade_numbers_do_not_overflow(client):
    # 64 digits: far beyond int64 and beyond Python's int<->str digit cap.
    long_a = "1" + "0" * 63
    long_b = "9" * 64
    post(client, msg(trade_no=long_b, t=T0 + 5_000, price=10100, qty=10))
    post(client, msg(trade_no=long_a, t=T0 + 5_000, price=10000, qty=10))
    bar = bars(client)["bars"][0]
    assert bar["open_cents"] == 10000
    assert bar["close_cents"] == 10100


# ---------------------------------------------------------------------------
# legacy database + upgrade
# ---------------------------------------------------------------------------

LEGACY_SCHEMA = """
CREATE TABLE trades (
    symbol            TEXT    NOT NULL,
    trade_no          TEXT    NOT NULL,
    revision          INTEGER NOT NULL,
    op                TEXT    NOT NULL CHECK (op IN ('trade', 'correction', 'cancel')),
    trade_time_utc_ms INTEGER,
    price_cents       INTEGER,
    qty               INTEGER,
    cancelled         INTEGER NOT NULL DEFAULT 0,
    trade_no_num      INTEGER,
    PRIMARY KEY (symbol, trade_no)
);
CREATE TABLE minute_bars (
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
CREATE TABLE journal (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    minute_start_utc_ms INTEGER NOT NULL,
    change_type         TEXT    NOT NULL CHECK (change_type IN ('upsert', 'delete')),
    bar_json            TEXT
);
"""


def _legacy_key(row):
    """Reproduce the pre-upgrade ordering exactly (None => text fallback)."""
    trade_time, trade_no, num = row
    if num is not None:
        return (trade_time, (0, num, ""))
    return (trade_time, (1, 0, trade_no))


def _legacy_num(trade_no: str):
    if trade_no.isdigit():
        value = int(trade_no)
        if value <= INT64_MAX:
            return value
    return None


def build_legacy_db(path, trades, with_bars=True):
    """trades: list of dicts {symbol, trade_no, revision, op, t, price, qty, cancelled}.

    Bars and journal rows are materialized with the OLD ordering, as a legacy
    process would have left them.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(LEGACY_SCHEMA)
        for tr in trades:
            cancelled = tr.get("cancelled", False)
            conn.execute(
                """
                INSERT INTO trades (symbol, trade_no, revision, op, trade_time_utc_ms,
                                    price_cents, qty, cancelled, trade_no_num)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tr["symbol"], tr["trade_no"], tr.get("revision", 1), tr.get("op", "trade"),
                    None if cancelled else tr["t"],
                    None if cancelled else tr["price"],
                    None if cancelled else tr["qty"],
                    1 if cancelled else 0,
                    _legacy_num(tr["trade_no"]),
                ),
            )
        if with_bars:
            active = [t for t in trades if not t.get("cancelled", False)]
            minutes = sorted({(t["symbol"], logic.minute_floor(t["t"])) for t in active})
            for symbol, minute in minutes:
                rows = [t for t in active
                        if t["symbol"] == symbol and logic.minute_floor(t["t"]) == minute]
                ordered = sorted(
                    rows,
                    key=lambda t: _legacy_key(
                        (t["t"], t["trade_no"], _legacy_num(t["trade_no"]))
                    ),
                )
                bar = {
                    "minute_start_utc_ms": minute,
                    "open_cents": ordered[0]["price"],
                    "high_cents": max(t["price"] for t in ordered),
                    "low_cents": min(t["price"] for t in ordered),
                    "close_cents": ordered[-1]["price"],
                    "volume": sum(t["qty"] for t in ordered),
                    "trade_count": len(ordered),
                }
                conn.execute(
                    """
                    INSERT INTO minute_bars (symbol, minute_start_utc_ms, open_cents,
                        high_cents, low_cents, close_cents, volume, trade_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (symbol, minute, bar["open_cents"], bar["high_cents"],
                     bar["low_cents"], bar["close_cents"], bar["volume"], bar["trade_count"]),
                )
                conn.execute(
                    "INSERT INTO journal (symbol, minute_start_utc_ms, change_type, bar_json)"
                    " VALUES (?, ?, 'upsert', ?)",
                    (symbol, minute, json.dumps(bar, sort_keys=True)),
                )
        conn.commit()
    finally:
        conn.close()


def _bar_at(path, symbol, minute):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
            (symbol, minute),
        ).fetchone()
        return None if row is None else dict(row)
    finally:
        conn.close()


def _journal(path):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT * FROM journal ORDER BY seq").fetchall()
    finally:
        conn.close()


def _legacy_trades_buggy():
    """Two same-millisecond trades; legacy text order swaps open/close."""
    return [
        # An ordinary trade in another minute must not be touched.
        {"symbol": "600519", "trade_no": "1", "t": T0 - 60_000, "price": 9000, "qty": 10},
        {"symbol": "600519", "trade_no": BIG_B, "t": T0 + 5_000, "price": 10000, "qty": 10},
        {"symbol": "600519", "trade_no": BIG_A, "t": T0 + 5_000, "price": 10100, "qty": 10},
    ]


def test_upgrade_recomputes_only_affected_minute_and_delivers_by_old_cursor(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, _legacy_trades_buggy())

    # The legacy bar really is wrong (text order put BIG_B first).
    legacy_bar = _bar_at(db_file, "600519", M0)
    assert legacy_bar["open_cents"] == 10000
    assert legacy_bar["close_cents"] == 10100
    legacy_cursor = _journal(db_file)[-1][0]

    with TestClient(create_app(db_file)) as c:
        # Corrected bar is visible in a fresh snapshot.
        snap = bars(c)
        fixed = [b for b in snap["bars"] if b["minute_start_utc_ms"] == M0][0]
        assert fixed["open_cents"] == 10100
        assert fixed["close_cents"] == 10000

        # Risk control holding the pre-upgrade cursor receives the correction,
        # appended after all old journal rows.
        page = c.get("/v1/symbols/600519/changes",
                     params={"cursor": legacy_cursor}).json()
        assert len(page["changes"]) == 1
        change = page["changes"][0]
        assert change["seq"] > legacy_cursor
        assert change["minute_start_utc_ms"] == M0
        assert change["type"] == "upsert"
        assert change["bar"]["open_cents"] == 10100
        assert change["bar"]["close_cents"] == 10000

        # The untouched minute got no journal row.
        all_changes = c.get("/v1/symbols/600519/changes", params={"cursor": 0}).json()
        minutes = [ch["minute_start_utc_ms"] for ch in all_changes["changes"]]
        assert minutes.count(T0 - 60_000) == 1  # only the original legacy row


def test_upgrade_skips_minutes_whose_open_close_did_not_change(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    # Overflow numbers are in the middle of the minute: open/close unchanged.
    build_legacy_db(db_file, [
        {"symbol": "600519", "trade_no": "1", "t": T0 + 1_000, "price": 9900, "qty": 10},
        {"symbol": "600519", "trade_no": BIG_B, "t": T0 + 5_000, "price": 10000, "qty": 10},
        {"symbol": "600519", "trade_no": BIG_A, "t": T0 + 5_000, "price": 10100, "qty": 10},
        {"symbol": "600519", "trade_no": "2", "t": T0 + 9_000, "price": 10200, "qty": 10},
    ])
    legacy_bar = _bar_at(db_file, "600519", M0)
    assert (legacy_bar["open_cents"], legacy_bar["close_cents"]) == (9900, 10200)
    journal_before = _journal(db_file)

    with TestClient(create_app(db_file)) as c:
        current = _bar_at(db_file, "600519", M0)
        assert (current["open_cents"], current["close_cents"]) == (9900, 10200)
        assert _journal(db_file) == journal_before  # no noise
        assert c.get("/v1/symbols/600519/changes",
                     params={"cursor": journal_before[-1][0]}).json()["changes"] == []


def test_upgrade_ignores_cancelled_trades(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, [
        # A cancelled out-of-range trade must not keep a minute in scope.
        {"symbol": "600519", "trade_no": "1", "t": T0 + 1_000, "price": 9900, "qty": 10},
        {"symbol": "600519", "trade_no": BIG_A, "t": T0 + 5_000, "price": 10100, "qty": 10,
         "cancelled": True},
    ])
    journal_before = _journal(db_file)
    with TestClient(create_app(db_file)) as c:
        assert bars(c)["bars"][0]["open_cents"] == 9900
        assert _journal(db_file) == journal_before


def test_upgrade_is_not_reapplied_on_restart(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, _legacy_trades_buggy())

    with TestClient(create_app(db_file)) as c:
        first_cursor = bars(c)["cursor"]
    journal_after_first = _journal(db_file)
    assert len(journal_after_first) == 3  # two legacy rows + one correction

    # Restart on the same file: no extra journal rows, cursor unchanged.
    with TestClient(create_app(db_file)) as c2:
        assert bars(c2)["cursor"] == first_cursor
        assert c2.get("/v1/symbols/600519/changes",
                      params={"cursor": first_cursor}).json()["changes"] == []
    assert _journal(db_file) == journal_after_first


def test_upgrade_failure_rolls_back_and_retry_succeeds(tmp_path, monkeypatch):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, _legacy_trades_buggy())
    bars_before = [_bar_at(db_file, "600519", m) for m in (T0 - 60_000, M0)]
    journal_before = _journal(db_file)

    def boom(conn, symbol, bar):
        raise RuntimeError("simulated failure during upgrade")

    monkeypatch.setattr(logic, "_write_bar", boom)
    with pytest.raises(RuntimeError):
        create_app(db_file)
    monkeypatch.undo()

    # Nothing half-applied: bars, journal and marker all rolled back.
    assert [_bar_at(db_file, "600519", m) for m in (T0 - 60_000, M0)] == bars_before
    assert _journal(db_file) == journal_before
    conn = sqlite3.connect(db_file)
    try:
        assert conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0] == 0
    finally:
        conn.close()

    # Retry on the next start repairs exactly once.
    with TestClient(create_app(db_file)) as c:
        fixed = [b for b in bars(c)["bars"] if b["minute_start_utc_ms"] == M0][0]
        assert (fixed["open_cents"], fixed["close_cents"]) == (10100, 10000)
    assert len(_journal(db_file)) == len(journal_before) + 1


def test_upgrade_failure_midway_does_not_partially_update(tmp_path, monkeypatch):
    db_file = str(tmp_path / "legacy.db")
    # Two affected minutes in different symbols so a failure can occur after
    # the first one was already written inside the transaction.
    trades = _legacy_trades_buggy() + [
        {"symbol": "000001", "trade_no": BIG_B, "t": T0 + 5_000, "price": 2000, "qty": 10},
        {"symbol": "000001", "trade_no": BIG_A, "t": T0 + 5_000, "price": 2100, "qty": 10},
    ]
    build_legacy_db(db_file, trades)
    before = {s: _bar_at(db_file, s, M0) for s in ("600519", "000001")}
    journal_before = _journal(db_file)

    original = logic._write_bar
    calls = {"n": 0}

    def flaky(conn, symbol, bar):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("fail after first write in the transaction")
        original(conn, symbol, bar)

    monkeypatch.setattr(logic, "_write_bar", flaky)
    with pytest.raises(RuntimeError):
        create_app(db_file)

    assert calls["n"] == 2  # the first write happened and must be rolled back
    assert {s: _bar_at(db_file, s, M0) for s in ("600519", "000001")} == before
    assert _journal(db_file) == journal_before


def test_upgrade_adds_meta_table_to_legacy_db(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, [], with_bars=False)
    create_app(db_file)
    conn = sqlite3.connect(db_file)
    try:
        value = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (logic.SORT_KEY_UPGRADE_KEY,)
        ).fetchone()
        assert value == ("done",)
    finally:
        conn.close()


def test_ingest_continues_correctly_on_upgraded_legacy_db(tmp_path):
    db_file = str(tmp_path / "legacy.db")
    build_legacy_db(db_file, _legacy_trades_buggy())

    with TestClient(create_app(db_file)) as c:
        cursor = bars(c)["cursor"]
        # The legacy `trade_no_num` column is gone from the code path: a brand
        # new out-of-range trade still sorts numerically against existing ones.
        r = post(c, msg(trade_no="99999999999999999999", t=T0 + 5_000,
                        price=10200, qty=10))
        assert r.json()["status"] == "applied"

        fixed = [b for b in bars(c)["bars"] if b["minute_start_utc_ms"] == M0][0]
        assert fixed["open_cents"] == 10100   # BIG_A
        assert fixed["close_cents"] == 10200  # 20-digit 9... is largest

        page = c.get("/v1/symbols/600519/changes", params={"cursor": cursor}).json()
        assert page["changes"][-1]["bar"]["close_cents"] == 10200

