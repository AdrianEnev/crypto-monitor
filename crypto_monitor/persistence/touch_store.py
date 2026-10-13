"""Durable independent level status and fresh-only email queue.

These additive tables preserve the previous closed-candle monitor's history.
Use one connection serially on its owning thread, as with SQLiteStore.
"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Mapping

from crypto_monitor.core.level_sweep import MINUTE, reference_periods
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.core.touch_monitor import (
    TouchActivation, TouchOutboxEntry, TouchWatch, next_eligible, price_value,
    range_touches, utc_time, validate_delays, watch_window,
)
from crypto_monitor.persistence.sqlite_store import SQLiteStore, _utc


def _date(value):
    return datetime.fromisoformat(value) if value else None


def _watch(row):
    return TouchWatch(row[0], row[1], row[2], _date(row[3]), _date(row[4]),
        _date(row[5]), _date(row[6]), Decimal(row[7]) if row[7] is not None else None,
        _date(row[8]), bool(row[9]), row[10], _date(row[11]), row[12], row[13], bool(row[14]))


def _activation(row):
    return TouchActivation(row[0], row[1], row[2], row[3], _date(row[4]),
        Decimal(row[5]), _date(row[6]), _date(row[7]), row[8], Decimal(row[9]), bool(row[10]))


_WATCH_COLUMNS = """symbol, name, side, source_start, source_end, watch_start,
    watch_end, level_price, history_cursor, available, reason, accepted_at,
    accepted_count, origin, approximate"""
_ACTIVATION_COLUMNS = """a.activation_id, a.symbol, a.name, a.side,
    a.source_start, a.level_price, a.touched_at, a.observed_at, a.origin,
    a.quote_volume, a.approximate"""


class TouchStore(SQLiteStore):
    def __init__(self, path):
        super().__init__(path)
        try:
            with self._connection:
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS touch_watches (
                        symbol TEXT NOT NULL, name TEXT NOT NULL,
                        side TEXT NOT NULL CHECK (side IN ('high', 'low')),
                        source_start TEXT NOT NULL, source_end TEXT NOT NULL,
                        watch_start TEXT NOT NULL, watch_end TEXT NOT NULL,
                        level_price TEXT, history_cursor TEXT NOT NULL,
                        available INTEGER NOT NULL DEFAULT 0,
                        reason TEXT, accepted_at TEXT,
                        accepted_count INTEGER NOT NULL DEFAULT 0,
                        origin TEXT, approximate INTEGER NOT NULL DEFAULT 0,
                        active INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY (symbol, name, side, source_start)
                    )
                """)
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS touch_activations (
                        activation_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL, name TEXT NOT NULL, side TEXT NOT NULL,
                        source_start TEXT NOT NULL, level_price TEXT NOT NULL,
                        touched_at TEXT NOT NULL, observed_at TEXT NOT NULL,
                        origin TEXT NOT NULL CHECK (origin IN ('live','history')),
                        quote_volume TEXT NOT NULL, approximate INTEGER NOT NULL,
                        FOREIGN KEY (symbol, name, side, source_start)
                            REFERENCES touch_watches(symbol, name, side, source_start)
                    )
                """)
                self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS touch_email_outbox (
                        activation_id TEXT PRIMARY KEY REFERENCES touch_activations(activation_id),
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        attempted_at TEXT, next_attempt_at TEXT, last_error TEXT,
                        sent_at TEXT, message_id TEXT, muted_at TEXT, muted_reason TEXT
                    )
                """)
                self._connection.execute("""
                    CREATE INDEX IF NOT EXISTS touch_recent_live
                    ON touch_activations(observed_at DESC) WHERE origin = 'live'
                """)
        except Exception:
            self.close()
            raise

    def initialize_watches(self, symbol: str, references: Mapping[str, ReferenceLevels],
                           as_of: datetime) -> tuple[TouchWatch, ...]:
        """Activate this symbol's current reference keys without resetting old state.

        History completeness is checked separately. A newly unavailable source
        has its own unavailable state; other references can still work.
        """
        utc_time(as_of)
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol is required")
        periods = reference_periods(as_of)
        if set(references) - set(periods):
            raise ValueError("unknown reference names")
        horizon = as_of.replace(second=0, microsecond=0)
        with self._connection:
            self._connection.execute("UPDATE touch_watches SET active=0 WHERE symbol=?", (symbol,))
            for name, period in periods.items():
                if period is None:
                    # No Monday range is defined until Tuesday 00:00 UTC.
                    continue
                reference = references.get(name)
                if reference is not None and reference.period != period:
                    raise ValueError(f"incorrect source period for {name}")
                window = watch_window(name, period)
                levels = reference.levels if reference else None
                for side in ("high", "low"):
                    price = getattr(levels, side) if levels is not None else None
                    if price is not None:
                        price_value(price)
                    key = (symbol, name, side, _utc(period.start))
                    saved = self._connection.execute(
                        "SELECT level_price, history_cursor FROM touch_watches "
                        "WHERE symbol=? AND name=? AND side=? AND source_start=?", key).fetchone()
                    if saved is not None and saved[0] is not None and price is not None:
                        if Decimal(saved[0]) != price:
                            raise ValueError("completed reference prices must remain immutable")
                    if price is None and saved is not None and saved[0] is not None:
                        price = Decimal(saved[0])
                    cursor = _date(saved[1]) if saved else window.start
                    available = price is not None and cursor >= horizon
                    reason = None if available else ("history checking" if price is not None
                                                    else "reference history unavailable")
                    self._connection.execute("""
                        INSERT INTO touch_watches
                        (symbol,name,side,source_start,source_end,watch_start,watch_end,
                         level_price,history_cursor,available,reason,active)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
                        ON CONFLICT(symbol,name,side,source_start) DO UPDATE SET
                          active=1, level_price=COALESCE(touch_watches.level_price,excluded.level_price),
                          available=excluded.available, reason=excluded.reason
                    """, key + (_utc(period.end), _utc(window.start), _utc(window.end),
                               str(price) if price is not None else None, _utc(cursor),
                               int(available), reason))
        return self.history_jobs(symbol, as_of)

    def history_jobs(self, symbol: str, as_of: datetime | None = None) -> tuple[TouchWatch, ...]:
        rows = self._connection.execute(f"SELECT {_WATCH_COLUMNS} FROM touch_watches "
                                        "WHERE symbol=? AND active=1 ORDER BY name,side", (symbol,)).fetchall()
        return tuple(_watch(row) for row in rows)

    def mark_history_complete(self, symbol, name, source_start, through):
        """Mark both sides complete only if their durable cursor reached through."""
        utc_time(through)
        with self._connection:
            self._connection.execute("""
                UPDATE touch_watches SET available=1,reason=NULL
                WHERE symbol=? AND name=? AND source_start=?
                  AND level_price IS NOT NULL AND history_cursor>=?
            """, (symbol, name, _utc(source_start), _utc(through)))

    def mark_history_unavailable(self, symbol, name, source_start, reason, *, side=None):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_watches SET available=0,reason=?
                WHERE symbol=? AND name=? AND source_start=?
                  AND (? IS NULL OR side=?)
            """, (str(reason), symbol, name, _utc(source_start), side, side))

    def advance_history(self, watch: TouchWatch, through: datetime):
        """Advance over history whose complete daily range proves no touch."""
        utc_time(through)
        if through.second or through.microsecond or through > watch.watch_end:
            raise ValueError("history cursor must be an aligned in-period boundary")
        with self._connection:
            self._connection.execute("""
                UPDATE touch_watches SET history_cursor=MAX(history_cursor,?)
                WHERE symbol=? AND name=? AND side=? AND source_start=?
            """, (_utc(through), watch.symbol, watch.name, watch.side, _utc(watch.source_start)))

    def _record(self, symbol, low, high, at, delays, *, origin, notify, quote_volume,
                observed_at, eligible_time=None, watch_keys=None, approximate=False):
        validate_delays(delays)
        for value in (low, high, quote_volume):
            price_value(value)
        utc_time(at)
        utc_time(observed_at)
        if low > high or origin not in ("live", "history"):
            raise ValueError("invalid range or activation origin")
        applicable_at = eligible_time if eligible_time is not None else at
        utc_time(applicable_at)
        rows = self._connection.execute(f"SELECT {_WATCH_COLUMNS} FROM touch_watches "
                                        "WHERE symbol=? AND active=1", (symbol,)).fetchall()
        activations = []
        for row in rows:
            watch = _watch(row)
            key = (watch.name, watch.side, watch.source_start)
            if watch_keys is not None and key not in watch_keys:
                continue
            if not watch.watch_start <= applicable_at < watch.watch_end:
                continue
            if watch.level_price is None or (origin == "live" and not watch.available):
                continue
            if not range_touches(watch.side, watch.level_price, low, high):
                continue
            if watch.accepted_at is not None:
                if at < next_eligible(watch.name, watch.accepted_at, watch.watch_end, delays):
                    continue
                if watch.name == "monday":
                    continue
            identity = json.dumps([symbol, watch.name, watch.side,
                                   watch.source_start.isoformat(), at.isoformat()], separators=(",", ":"))
            activation = TouchActivation(identity, symbol, watch.name, watch.side,
                watch.source_start, watch.level_price, at, observed_at, origin,
                quote_volume, approximate)
            inserted = self._connection.execute("""
                INSERT OR IGNORE INTO touch_activations
                (activation_id,symbol,name,side,source_start,level_price,touched_at,
                 observed_at,origin,quote_volume,approximate) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (identity, symbol, watch.name, watch.side, _utc(watch.source_start),
                   str(watch.level_price), _utc(at), _utc(observed_at), origin,
                   str(quote_volume), int(approximate))).rowcount
            if not inserted:
                continue
            self._connection.execute("""
                UPDATE touch_watches SET accepted_at=?,accepted_count=accepted_count+1,
                    origin=?,approximate=?
                WHERE symbol=? AND name=? AND side=? AND source_start=?
            """, (_utc(at), origin, int(approximate), symbol, watch.name,
                   watch.side, _utc(watch.source_start)))
            if origin == "live" and notify:
                self._connection.execute("INSERT INTO touch_email_outbox(activation_id) VALUES (?)", (identity,))
            activations.append(activation)
        return activations

    def record_price(self, symbol, price, at, delays, *, origin="live", notify=False,
                     quote_volume=Decimal(0), observed_at=None, verified_only=False):
        """Save eligible touches atomically; equality and remaining beyond qualify."""
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            keys = {(w.name, w.side, w.source_start) for w in self.history_jobs(symbol)
                    if w.available} if verified_only else None
            return self._record(symbol, price, price, at, delays, origin=origin,
                notify=notify, quote_volume=quote_volume, observed_at=observed_at or at,
                watch_keys=keys)

    def record_range(self, symbol, low, high, at, delays, *, origin="history", notify=False,
                     quote_volume=Decimal(0), observed_at=None, verified_only=False):
        """Quietly seed an unfinished historical minute before enabling live alerts."""
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            keys = {(w.name, w.side, w.source_start) for w in self.history_jobs(symbol)
                    if w.available} if verified_only else None
            return self._record(symbol, low, high, at, delays, origin=origin,
                notify=notify, quote_volume=quote_volume, observed_at=observed_at or at,
                approximate=origin == "history", watch_keys=keys)

    def replay_history_candles(self, symbol, candles, delays, *, observed_at,
                               watch_keys=None):
        """Commit a contiguous batch, quiet activations and each side's next cursor.

        Historical timestamps use the end of the containing minute. Coverage is
        checked again here even if an injected REST provider skips validation.
        """
        validate_delays(delays)
        activations = []
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            for event in candles:
                candle = event.candle
                if event.symbol != symbol or not candle.closed or candle.interval != MINUTE:
                    raise ValueError("history must contain closed one-minute candles for this symbol")
                if candle.open_time.second or candle.open_time.microsecond:
                    raise ValueError("historical candles must be minute-aligned")
                current = self.history_jobs(symbol)
                applicable = []
                for watch in current:
                    key = (watch.name, watch.side, watch.source_start)
                    if watch_keys is not None and key not in watch_keys:
                        continue
                    if watch.level_price is None or not watch.watch_start <= candle.open_time < watch.watch_end:
                        continue
                    if candle.open_time < watch.history_cursor:
                        continue
                    if candle.open_time != watch.history_cursor:
                        raise ValueError(f"missing history before {candle.open_time.isoformat()}")
                    applicable.append(key)
                if not applicable:
                    continue
                ended = candle.open_time + MINUTE
                activations.extend(self._record(symbol, candle.low, candle.high, ended, delays,
                    origin="history", notify=False, quote_volume=Decimal(0), observed_at=observed_at,
                    eligible_time=candle.open_time, watch_keys=set(applicable), approximate=True))
                for name, side, source_start in applicable:
                    self._connection.execute("""
                        UPDATE touch_watches SET history_cursor=?
                        WHERE symbol=? AND name=? AND side=? AND source_start=?
                    """, (_utc(ended), symbol, name, side, _utc(source_start)))
        return activations

    def dashboard_snapshot(self, markets, asof, delays):
        validate_delays(delays)
        utc_time(asof)
        symbols = tuple(m.symbol if hasattr(m, "symbol") else m for m in markets)
        rows = []
        for symbol in symbols:
            for watch in self.history_jobs(symbol):
                if not watch.watch_start <= asof < watch.watch_end:
                    continue
                eligible = (next_eligible(watch.name, watch.accepted_at, watch.watch_end, delays)
                            if watch.accepted_at is not None else None)
                swept = eligible is not None and asof < eligible
                status = "unavailable" if not watch.available else "swept" if swept else "untouched"
                hide_old_quarter = watch.name == "previous_quarter" and not swept
                rows.append({"symbol": symbol, "name": watch.name, "side": watch.side,
                    "level_price": watch.level_price, "status": status, "available": watch.available,
                    "accepted_at": None if hide_old_quarter else watch.accepted_at,
                    "eligible_at": eligible if swept else None,
                    "count": (int(swept) if watch.name == "previous_quarter" else watch.accepted_count),
                    "source": None if hide_old_quarter else watch.origin,
                    "reason": watch.reason, "approximate": watch.approximate})
            if reference_periods(asof)["monday"] is None:
                for side in ("high", "low"):
                    rows.append({"symbol": symbol, "name": "monday", "side": side,
                        "level_price": None, "status": "unavailable", "available": False,
                        "accepted_at": None, "eligible_at": None, "count": 0, "source": None,
                        "reason": "Monday candle still forming", "approximate": False})
        fresh_rows = self._connection.execute(f"SELECT {_ACTIVATION_COLUMNS} FROM touch_activations a "
            "JOIN touch_watches w ON (a.symbol=w.symbol AND a.name=w.name AND a.side=w.side "
            "AND a.source_start=w.source_start) WHERE a.origin='live' AND w.active=1 "
            "AND w.watch_start<=? AND w.watch_end>? ORDER BY a.observed_at DESC LIMIT 100",
            (_utc(asof), _utc(asof))).fetchall()
        fresh = tuple({"symbol": a.symbol, "name": a.level_name, "side": a.side,
                       "level_price": a.level_price, "touched_at": a.touched_at,
                       "observed_at": a.observed_at}
                      for a in map(_activation, fresh_rows) if a.symbol in symbols)
        pending = self._connection.execute("SELECT COUNT(*) FROM touch_email_outbox "
            "WHERE sent_at IS NULL AND muted_at IS NULL").fetchone()[0]
        return {"watches": tuple(rows), "fresh": fresh, "pending_email_count": pending}

    def begin_run(self, at):
        # Run this before accepting any new events. Every existing pending row
        # belongs to an earlier run, including rows dated ahead after a clock
        # correction, and must not become a startup email.
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET muted_at=?,muted_reason='previous run'
                WHERE sent_at IS NULL AND muted_at IS NULL
            """, (_utc(at),))

    def mute_touch_outbox_before(self, run_started_at):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET muted_at=?,muted_reason='previous run'
                WHERE sent_at IS NULL AND muted_at IS NULL AND activation_id IN
                  (SELECT activation_id FROM touch_activations WHERE observed_at<=?)
            """, (_utc(run_started_at), _utc(run_started_at)))

    def mute_touch_outbox(self, activation_id, reason="expired"):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET muted_at=COALESCE(attempted_at,
                    (SELECT observed_at FROM touch_activations WHERE activation_id=?)),muted_reason=?
                WHERE activation_id=? AND sent_at IS NULL
            """, (activation_id, reason, activation_id))

    def expire_touch_outbox(self, before):
        """Mute stale retries even when their scheduled retry is not due yet."""
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET muted_at=?,muted_reason='expired'
                WHERE sent_at IS NULL AND muted_at IS NULL AND activation_id IN
                  (SELECT activation_id FROM touch_activations WHERE touched_at<=?)
            """, (_utc(before), _utc(before)))

    def load_touch_outbox(self, now, limit=100):
        utc_time(now)
        rows = self._connection.execute(f"""
            SELECT {_ACTIVATION_COLUMNS},o.attempt_count,o.next_attempt_at,o.last_error
            FROM touch_email_outbox o JOIN touch_activations a USING(activation_id)
            WHERE o.sent_at IS NULL AND o.muted_at IS NULL
              AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?)
            ORDER BY a.touched_at,a.activation_id LIMIT ?
        """, (_utc(now), limit)).fetchall()
        return tuple(TouchOutboxEntry(_activation(row[:11]), row[11], _date(row[12]), row[13])
                     for row in rows)

    def begin_touch_outbox_attempt(self, activation_id, attempted_at):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET attempt_count=attempt_count+1,attempted_at=?,
                    last_error='Delivery outcome not yet recorded'
                WHERE activation_id=? AND sent_at IS NULL AND muted_at IS NULL
            """, (_utc(attempted_at), activation_id))

    def record_touch_outbox_failure(self, activation_id, error, next_attempt_at):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET last_error=?,next_attempt_at=?
                WHERE activation_id=? AND sent_at IS NULL AND muted_at IS NULL
            """, (str(error), _utc(next_attempt_at), activation_id))

    def mark_touch_outbox_sent(self, activation_id, sent_at, message_id=None):
        with self._connection:
            self._connection.execute("""
                UPDATE touch_email_outbox SET sent_at=?,message_id=?,last_error=NULL,next_attempt_at=NULL
                WHERE activation_id=? AND sent_at IS NULL AND muted_at IS NULL
            """, (_utc(sent_at), message_id, activation_id))
