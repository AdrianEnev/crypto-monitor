"""Persist processed candles, sweep hits, and period-aware reference snapshots.

Use each store instance serially from one thread. commit_candle(event, hits)
saves candles, hits and new email outbox entries in one transaction;
the first successful commit for (symbol, opening time) wins. A replay is a
no-op, including its supplied hits. SQLite errors propagate to the caller.
The first hit per symbol/named level/side/source period claims that watch in the
same transaction. Repeats advance candles without saving or emailing another hit.

The engine advances in memory before this commit. If a commit fails, the caller
must stop processing and rebuild the engine from committed baselines before
retrying the candle. Database rollback does not rewind an existing engine.

Reference snapshot bundles commit separately, before their candles are processed.
Stored completed levels are immutable; missing levels may be filled later.
"""

import sqlite3
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from os import PathLike
from pathlib import Path
from typing import Iterable
from types import MappingProxyType

from crypto_monitor.adapters.binance_feed import ClosedKline
from crypto_monitor.core.level_sweep import MINUTE, Levels, reference_periods, reference_regime
from crypto_monitor.core.monitoring_engine import CandleBaseline, ReferenceLevels, SweepAlert


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def _utc(value: datetime) -> str:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")
    # Fixed-width UTC text sorts chronologically, with no float conversion.
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _decimal(value: Decimal) -> str:
    if not isinstance(value, Decimal):
        raise TypeError("stored prices and volumes must be Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError("stored prices and volumes must be finite and nonnegative")
    return str(value)


@dataclass(frozen=True)
class OutboxEntry:
    hit: SweepAlert
    attempt_count: int
    attempted_at: datetime | None
    next_attempt_at: datetime | None
    last_error: str | None
    sent_at: datetime | None
    message_id: str | None


@dataclass(frozen=True)
class StoredAlert:
    hit: SweepAlert
    email_status: str


@dataclass(frozen=True)
class DashboardData:
    pending_email_count: int
    delivery_error: str | None
    delivery_error_at: datetime | None
    alerts: tuple[StoredAlert, ...]
    latest_committed: tuple[tuple[str, datetime], ...] = ()


def _hit(row) -> SweepAlert:
    alert_id, symbol, candle_time, name, side, period, price, volume, detected = row
    return SweepAlert(alert_id=alert_id, symbol=symbol,
        candle_time=datetime.fromisoformat(candle_time), level_name=name, side=side,
        reference_period_start=datetime.fromisoformat(period), level_price=Decimal(price),
        captured_quote_volume=Decimal(volume), detected_at=datetime.fromisoformat(detected))


def _reference_payload(as_of, references):
    periods = reference_periods(as_of)
    if set(references) != {name for name, period in periods.items() if period is not None}:
        raise ValueError("snapshot must describe every active reference period")
    payload = {}
    for name, period in periods.items():
        if period is None:
            payload[name] = None  # Explicitly inactive Monday.
            continue
        ref = references[name]
        if not isinstance(ref, ReferenceLevels) or ref.period != period:
            raise ValueError(f"incorrect source period for {name}")
        ReferenceLevels(ref.period, ref.levels)  # Validate prices even for injected data.
        payload[name] = {"start": _utc(period.start), "end": _utc(period.end),
                         "high": _decimal(ref.levels.high) if ref.levels is not None else None,
                         "low": _decimal(ref.levels.low) if ref.levels is not None else None}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class SQLiteStore:
    """A configurable SQLite file; context-manager exit closes the connection."""

    def __init__(self, path: str | PathLike[str]):
        self._connection = sqlite3.connect(path)
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            with self._connection:
                self._connection.execute("BEGIN IMMEDIATE")
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS processed_candles (
                        symbol TEXT NOT NULL,
                        open_time TEXT NOT NULL,
                        close_price TEXT NOT NULL,
                        PRIMARY KEY (symbol, open_time)
                    )
                """)
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS sweep_hits (
                        alert_id TEXT PRIMARY KEY NOT NULL,
                        symbol TEXT NOT NULL,
                        candle_time TEXT NOT NULL,
                        level_name TEXT NOT NULL,
                        side TEXT NOT NULL CHECK (side IN ('high', 'low')),
                        reference_period_start TEXT NOT NULL,
                        level_price TEXT NOT NULL,
                        captured_quote_volume TEXT NOT NULL,
                        detected_at TEXT NOT NULL,
                        FOREIGN KEY (symbol, candle_time)
                            REFERENCES processed_candles (symbol, open_time)
                    )
                """)
                # Additive migration: existing hits are not retroactively queued.
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS email_outbox (
                        alert_id TEXT PRIMARY KEY NOT NULL REFERENCES sweep_hits(alert_id),
                        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                        attempted_at TEXT,
                        next_attempt_at TEXT,
                        last_error TEXT,
                        sent_at TEXT,
                        message_id TEXT,
                        CHECK ((sent_at IS NULL) = (message_id IS NULL))
                    )
                """)
                self._connection.execute("""
                    CREATE INDEX IF NOT EXISTS email_pending ON email_outbox(next_attempt_at)
                    WHERE sent_at IS NULL
                """)
                self._connection.execute("""
                    CREATE INDEX IF NOT EXISTS recent_hits ON sweep_hits(detected_at DESC, alert_id)
                """)
                self._connection.execute("""
                    CREATE INDEX IF NOT EXISTS email_errors ON email_outbox(attempted_at DESC, alert_id)
                    WHERE last_error IS NOT NULL
                """)
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS reference_snapshots (
                        symbol TEXT NOT NULL,
                        valid_start TEXT NOT NULL,
                        valid_end TEXT NOT NULL,
                        levels_json TEXT NOT NULL,
                        PRIMARY KEY (symbol, valid_start, valid_end),
                        CHECK (valid_start < valid_end)
                    )
                """)
                # Migrate once under the write lock. Preserve history, choosing
                # an already-sent hit first so old pending repeats cannot send.
                if self._connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'alert_claims'"
                ).fetchone() is None:
                    self._connection.execute("""
                        CREATE TABLE alert_claims (
                            symbol TEXT NOT NULL,
                            level_name TEXT NOT NULL,
                            side TEXT NOT NULL CHECK (side IN ('high', 'low')),
                            reference_period_start TEXT NOT NULL,
                            alert_id TEXT NOT NULL UNIQUE REFERENCES sweep_hits(alert_id),
                            PRIMARY KEY (symbol, level_name, side, reference_period_start)
                        )
                    """)
                    self._connection.execute("""
                        INSERT INTO alert_claims
                        SELECT symbol, level_name, side, reference_period_start, alert_id
                        FROM (
                            SELECT h.*, ROW_NUMBER() OVER (
                                PARTITION BY h.symbol, h.level_name, h.side, h.reference_period_start
                                ORDER BY o.sent_at IS NULL, h.candle_time, h.alert_id
                            ) AS position
                            FROM sweep_hits h LEFT JOIN email_outbox o ON o.alert_id = h.alert_id
                        ) WHERE position = 1
                    """)
        except Exception:
            self._connection.close()
            raise

    @classmethod
    def open_readonly(cls, path: str | PathLike[str]) -> "SQLiteStore":
        """No migrations or file creation; dashboard readers cannot modify data."""
        store = cls.__new__(cls)
        store._connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)
        try:
            store._connection.execute("PRAGMA query_only = ON")
        except Exception:
            store.close()
            raise
        return store

    def dashboard_data(self, *, recent_limit: int = 100) -> DashboardData:
        """Read one consistent view; bounded candidates are ranked by the UI."""
        if type(recent_limit) is not int or not 1 <= recent_limit <= 100:
            raise ValueError("recent limit must be between 1 and 100")
        try:
            self._connection.execute("BEGIN")
            pending = self._connection.execute(
                "SELECT COUNT(*) FROM email_outbox o JOIN alert_claims a USING (alert_id) "
                "WHERE o.sent_at IS NULL").fetchone()[0]
            error = self._connection.execute("""
                SELECT last_error, attempted_at FROM email_outbox JOIN alert_claims USING (alert_id)
                WHERE last_error IS NOT NULL AND last_error != 'Delivery outcome not yet recorded'
                ORDER BY attempted_at DESC, alert_id LIMIT 1
            """).fetchone()
            rows = self._connection.execute("""
                SELECT h.alert_id, h.symbol, h.candle_time, h.level_name, h.side,
                       h.reference_period_start, h.level_price, h.captured_quote_volume, h.detected_at,
                       CASE WHEN o.alert_id IS NULL THEN 'unqueued'
                            WHEN o.sent_at IS NOT NULL THEN 'sent'
                            WHEN o.last_error IS NOT NULL
                                 AND o.last_error != 'Delivery outcome not yet recorded' THEN 'retry'
                            ELSE 'pending' END
                FROM sweep_hits h JOIN alert_claims a ON h.alert_id = a.alert_id
                LEFT JOIN email_outbox o ON h.alert_id = o.alert_id
                ORDER BY h.detected_at DESC, h.alert_id LIMIT ?
            """, (recent_limit,)).fetchall()
            baselines = self._connection.execute("""
                SELECT symbol, MAX(open_time) FROM processed_candles
                GROUP BY symbol ORDER BY symbol
            """).fetchall()
            return DashboardData(pending, error[0] if error else None,
                datetime.fromisoformat(error[1]) if error and error[1] else None,
                tuple(StoredAlert(_hit(row[:9]), row[9]) for row in rows),
                tuple((symbol, datetime.fromisoformat(opened)) for symbol, opened in baselines))
        finally:
            self._connection.rollback()  # Release the read snapshot promptly, including on errors.

    def close(self) -> None:
        self._connection.close()

    def load_reference_snapshot(self, symbol: str, opened: datetime):
        """Resolve by valid time; validate stored boundaries, source periods and prices."""
        regime = reference_regime(opened)
        rows = self._connection.execute("""
            SELECT valid_start, valid_end, levels_json FROM reference_snapshots
            WHERE symbol = ? AND valid_start <= ? AND valid_end > ? LIMIT 2
        """, (_text(symbol, "symbol"), _utc(opened), _utc(opened))).fetchall()
        if not rows:
            return None
        if len(rows) != 1 or rows[0][:2] != (_utc(regime.start), _utc(regime.end)):
            raise ValueError("overlapping or noncanonical reference validity range")
        payload, refs = json.loads(rows[0][2]), {}
        periods = reference_periods(opened)
        if not isinstance(payload, dict) or set(payload) != set(periods):
            raise ValueError("malformed stored reference snapshot")
        for name, period in periods.items():
            row = payload[name]
            if period is None:
                if row is not None:
                    raise ValueError("inactive Monday must not have levels")
                continue
            if not isinstance(row, dict) or set(row) != {"start", "end", "high", "low"}:
                raise ValueError("malformed stored reference level")
            if (row["start"], row["end"]) != (_utc(period.start), _utc(period.end)):
                raise ValueError("incorrect stored source period")
            if row["high"] is None and row["low"] is None:
                levels = None
            else:
                if not isinstance(row["high"], str) or not isinstance(row["low"], str):
                    raise ValueError("stored reference prices must be Decimal strings")
                levels = Levels(Decimal(row["high"]), Decimal(row["low"]))
            refs[name] = ReferenceLevels(period, levels)
        return MappingProxyType(refs)

    def save_reference_snapshots(self, as_of: datetime, snapshots):
        """Atomically store an entire symbol bundle; never prune or downgrade it.

        Known levels are immutable. Incomplete snapshots may gain missing levels;
        conflicting values reject the whole transaction, including other symbols.
        BEGIN IMMEDIATE serializes concurrent publishers before inspecting rows.
        """
        regime = reference_regime(as_of)
        for symbol, refs in snapshots.items():
            _text(symbol, "symbol")
            _reference_payload(as_of, refs)
        result = {}
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            for symbol, refs in snapshots.items():
                merged = dict(refs)
                prior = self.load_reference_snapshot(symbol, as_of)
                if prior is not None:
                    for name, old in prior.items():
                        new = merged[name]
                        if old.levels is not None:
                            if new.levels is not None and new.levels != old.levels:
                                raise ValueError(f"conflicting saved reference levels: {symbol} {name}")
                            merged[name] = old
                payload = _reference_payload(as_of, merged)
                self._connection.execute("""
                    INSERT INTO reference_snapshots (symbol, valid_start, valid_end, levels_json)
                    VALUES (?, ?, ?, ?) ON CONFLICT(symbol, valid_start, valid_end)
                    DO UPDATE SET levels_json = excluded.levels_json
                """, (symbol, _utc(regime.start), _utc(regime.end), payload))
                result[symbol] = MappingProxyType(merged)
        return MappingProxyType(result)

    def __enter__(self) -> "SQLiteStore":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def commit_candle(self, event: ClosedKline, hits: Iterable[SweepAlert]) -> bool:
        """Atomically save a closed 1m candle, hits and outbox; False means replay.

        Duplicate alert IDs within this candle are ignored (first hit wins).
        Reusing an ID owned by another candle is invalid and rolls back the
        whole transaction. Never update an existing candle or append late hits.
        Only the first committed hit per symbol/name/side/source period is
        retained and queued. Later sweeps still advance the candle baseline.
        """
        symbol = _text(event.symbol, "symbol")
        candle = event.candle
        if candle.closed is not True or candle.interval != MINUTE:
            raise ValueError("only closed one-minute candles may be committed")
        baseline = CandleBaseline(candle.open_time, candle.close)
        opened = _utc(baseline.open_time)
        with self._connection:
            inserted = self._connection.execute("""
                INSERT INTO processed_candles (symbol, open_time, close_price)
                VALUES (?, ?, ?) ON CONFLICT (symbol, open_time) DO NOTHING
            """, (symbol, opened, _decimal(baseline.close)))
            if inserted.rowcount == 0:
                return False
            for hit in hits:
                if hit.symbol != symbol or hit.candle_time != candle.open_time:
                    raise ValueError("hit must belong to the committed symbol and candle time")
                if hit.side not in ("high", "low"):
                    raise ValueError("hit side must be high or low")
                period_start = _utc(hit.reference_period_start)
                if any((hit.reference_period_start.hour, hit.reference_period_start.minute,
                        hit.reference_period_start.second, hit.reference_period_start.microsecond)):
                    raise ValueError("reference period start must be UTC midnight")
                values = (
                    _text(hit.alert_id, "alert ID"), symbol, _utc(hit.candle_time),
                    _text(hit.level_name, "level name"), hit.side, period_start,
                    _decimal(hit.level_price), _decimal(hit.captured_quote_volume),
                    _utc(hit.detected_at),
                )
                owner = self._connection.execute(
                    "SELECT symbol, candle_time FROM sweep_hits WHERE alert_id = ?",
                    (hit.alert_id,),
                ).fetchone()
                if owner is not None:
                    if owner != (symbol, opened):
                        raise ValueError("alert ID already belongs to another candle")
                    continue
                key = (symbol, hit.level_name, hit.side, period_start)
                if self._connection.execute("""
                    SELECT 1 FROM alert_claims
                    WHERE symbol = ? AND level_name = ? AND side = ? AND reference_period_start = ?
                """, key).fetchone() is not None:
                    continue
                inserted = self._connection.execute("""
                    INSERT INTO sweep_hits (
                        alert_id, symbol, candle_time, level_name, side,
                        reference_period_start, level_price, captured_quote_volume, detected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (alert_id) DO NOTHING
                """, values)
                if inserted.rowcount == 1:
                    self._connection.execute(
                        "INSERT INTO alert_claims VALUES (?, ?, ?, ?, ?)", (*key, hit.alert_id))
                    self._connection.execute(
                        "INSERT INTO email_outbox (alert_id) VALUES (?)", (hit.alert_id,))
        return True

    def load_candle_alerts(self, symbol: str, opened: datetime) -> list[SweepAlert]:
        """Read the alerts actually retained for a committed candle."""
        rows = self._connection.execute("""
            SELECT h.alert_id, h.symbol, h.candle_time, h.level_name, h.side,
                   h.reference_period_start, h.level_price, h.captured_quote_volume, h.detected_at
            FROM sweep_hits h JOIN alert_claims a ON h.alert_id = a.alert_id
            WHERE h.symbol = ? AND h.candle_time = ? ORDER BY h.alert_id
        """, (symbol, _utc(opened)))
        return [_hit(row) for row in rows]

    def load_baselines(self) -> dict[str, CandleBaseline]:
        """Return each symbol's greatest committed opening time, not arrival time."""
        rows = self._connection.execute("""
            SELECT c.symbol, c.open_time, c.close_price
            FROM processed_candles AS c
            WHERE c.open_time = (
                SELECT MAX(other.open_time) FROM processed_candles AS other
                WHERE other.symbol = c.symbol
            )
            ORDER BY c.symbol
        """)
        return {
            symbol: CandleBaseline(datetime.fromisoformat(opened), Decimal(close))
            for symbol, opened, close in rows
        }

    def load_hits(self) -> list[SweepAlert]:
        """Read full history in ID order, including suppressed legacy hits."""
        rows = self._connection.execute("""
            SELECT alert_id, symbol, candle_time, level_name, side,
                   reference_period_start, level_price, captured_quote_volume, detected_at
            FROM sweep_hits ORDER BY alert_id
        """)
        return [_hit(row) for row in rows]

    def load_outbox(self, *, due_at: datetime | None = None,
                    include_sent: bool = False) -> list[OutboxEntry]:
        """Unsuppressed entries; due_at filters persisted retry deadlines.

        Historical repeats remain in SQLite for audit but cannot be dispatched.
        """
        rows = self._connection.execute("""
            SELECT h.alert_id, h.symbol, h.candle_time, h.level_name, h.side,
                   h.reference_period_start, h.level_price, h.captured_quote_volume, h.detected_at,
                   o.attempt_count, o.attempted_at, o.next_attempt_at, o.last_error,
                   o.sent_at, o.message_id
            FROM email_outbox o JOIN sweep_hits h ON h.alert_id = o.alert_id
            JOIN alert_claims a ON h.alert_id = a.alert_id
            WHERE (? OR o.sent_at IS NULL)
              AND (? IS NULL OR o.next_attempt_at IS NULL OR o.next_attempt_at <= ?)
            ORDER BY h.alert_id
        """, (include_sent, _utc(due_at) if due_at else None, _utc(due_at) if due_at else None))
        def date(value):
            return datetime.fromisoformat(value) if value is not None else None
        return [OutboxEntry(_hit(row[:9]), row[9], date(row[10]), date(row[11]),
                            row[12], date(row[13]), row[14]) for row in rows]

    def begin_email_attempt(self, alert_ids: Iterable[str], *, now: datetime,
                            next_attempt_at: datetime) -> None:
        """Persist the attempt before I/O, including a retry deadline for crashes.

        Run one email dispatcher per database. No transaction spans SES I/O.
        """
        if next_attempt_at <= now:
            raise ValueError("next attempt must be after this attempt")
        with self._connection:
            for alert_id in alert_ids:
                changed = self._connection.execute("""
                    UPDATE email_outbox SET attempt_count = attempt_count + 1,
                        attempted_at = ?, next_attempt_at = ?,
                        last_error = 'Delivery outcome not yet recorded'
                    WHERE alert_id = ? AND sent_at IS NULL
                      AND alert_id IN (SELECT alert_id FROM alert_claims)
                      AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                """, (_utc(now), _utc(next_attempt_at), alert_id, _utc(now)))
                if changed.rowcount != 1:
                    raise ValueError("outbox entry is not pending and due")

    def record_email_failure(self, alert_ids: Iterable[str], error: str) -> None:
        """Store a sanitized error category, never raw SDK request/exception text."""
        error = _text(error, "delivery error")
        with self._connection:
            self._connection.executemany("""
                UPDATE email_outbox SET last_error = ? WHERE alert_id = ? AND sent_at IS NULL
            """, ((error, alert_id) for alert_id in alert_ids))

    def mark_email_sent(self, alert_ids: Iterable[str], *, message_id: str, sent_at: datetime) -> None:
        """Atomically acknowledge exactly the accepted digest's rows."""
        message_id, sent_at_text = _text(message_id, "SES MessageId"), _utc(sent_at)
        with self._connection:
            for alert_id in alert_ids:
                changed = self._connection.execute("""
                    UPDATE email_outbox SET sent_at = ?, message_id = ?,
                        last_error = NULL, next_attempt_at = NULL
                    WHERE alert_id = ? AND sent_at IS NULL AND attempt_count > 0
                """, (sent_at_text, message_id, alert_id))
                if changed.rowcount != 1:
                    raise ValueError("outbox entry has no unacknowledged attempt")
