"""Reconnect/rotate the existing feed while keeping one recovery worker alive.

Binance permits a 24-hour connection lifetime; rotate after 23 hours by default.
https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
No candle deduplication here: every closed event goes to RecoveryWorker.submit,
whose processor uses SQLite's committed cursor before evaluating any candle.
"""

import asyncio
import logging
import math
import random
import ssl
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Literal

from websockets.asyncio.client import connect, process_exception
from websockets.exceptions import ConnectionClosed, InvalidStatus

from crypto_monitor.adapters.binance_feed import BinanceKlineFeed, FeedConnectionError
from crypto_monitor.runtime.gap_recovery import MonitoringFailure, RecoveryWorker


@dataclass(frozen=True)
class ConnectionStatus:
    state: Literal["stopped", "connecting", "connected", "backoff", "rotating", "unhealthy"] = "stopped"
    last_disconnect: str | None = None
    last_error: str | None = None
    reconnect_attempts: int = 0  # Actual attempts after the first, including rotations.
    consecutive_failures: int = 0
    retry_delay: float | None = None


def _transient(error: Exception) -> bool:
    if not isinstance(error, FeedConnectionError):
        return False  # Includes message, callback, configuration and worker errors.
    cause = error.__cause__
    if cause is None:
        return True  # The feed explicitly reports a normal remote close this way.
    if isinstance(cause, (ssl.SSLCertVerificationError, PermissionError)):
        return False
    if isinstance(cause, ConnectionClosed):
        close = cause.rcvd or cause.sent
        return close is None or close.code in (1000, 1001, 1006, 1011, 1012, 1013, 1014)
    if isinstance(cause, InvalidStatus) and cause.response.status_code == 429:
        return True
    # Library policy covers network/timeouts, EOF during handshake and 500/502/503/504.
    # Other handshake/protocol failures are permanent until configuration changes.
    return process_exception(cause) is None


class WebSocketSupervisor:
    """Own a feed loop AND the existing worker; don't also run that worker yourself.

    connector retains websockets.connect's async context-manager interface.
    sleep, random_value and monotonic are injectable for deterministic tests.
    Equal jitter chooses [ceiling/2, ceiling], where the exponential ceiling
    never exceeds max_delay. Backoff resets after stable_after connected seconds
    or a successful planned rotation, not after a short-lived handshake.

    Status is an immutable snapshot, replaced on transitions. Run counters reset
    on each explicit run(). Errors/last disconnect remain visible after recovery.
    stop/cancellation close the socket and interrupt the worker at a safe commit
    boundary; an in-flight synchronous REST request must first return/time out.
    """

    def __init__(
        self, worker: RecoveryWorker, *, connector: Callable[..., Any] | None = None,
        base_delay: float = 1.0, max_delay: float = 60.0,
        rotation_interval: float = 23 * 60 * 60, stable_after: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        for name, value in (("base_delay", base_delay), ("max_delay", max_delay),
                            ("rotation_interval", rotation_interval), ("stable_after", stable_after)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if max_delay < base_delay:
            raise ValueError("max_delay must be at least base_delay")
        if rotation_interval >= 24 * 60 * 60:
            raise ValueError("rotation_interval must be below Binance's 24-hour limit")
        self.worker = worker
        # Reuse the feed's actual symbol validation/normalization without opening a socket.
        self.symbols = BinanceKlineFeed([m.symbol for m in worker.markets], worker.submit).symbols
        self._connector = connector if connector is not None else connect
        self.base_delay, self.max_delay = base_delay, max_delay
        self.rotation_interval, self.stable_after = rotation_interval, stable_after
        self._sleep, self._random, self._monotonic = sleep, random_value, monotonic
        self.status = ConnectionStatus()
        self._connected_since: float | None = None
        self._running = False
        self._reconfigure = asyncio.Event()

    def update_symbols(self, symbols) -> bool:
        """Rotate only the socket; retain the worker, queue, and SQLite cursors."""
        selected = BinanceKlineFeed(symbols, self.worker.submit).symbols
        if selected == self.symbols:
            return False
        self.symbols = selected
        self._reconfigure.set()
        return True

    def _status(self, **changes) -> None:
        self.status = replace(self.status, **changes)
        logging.getLogger(__name__).info(
            "connection state=%s reconnect_attempts=%s retry_delay=%s last_disconnect=%s last_error=%s",
            self.status.state, self.status.reconnect_attempts, self.status.retry_delay,
            self.status.last_disconnect, self.status.last_error,
        )

    async def _session(self, stop: asyncio.Event) -> bool:
        """Return True after a planned rotation; False after requested shutdown."""
        opened, session_stop = asyncio.Event(), asyncio.Event()
        self._connected_since = None

        @asynccontextmanager
        async def connector(*args, **kwargs):
            async with self._connector(*args, **kwargs) as socket:
                self._connected_since = self._monotonic()
                self._status(state="connected", retry_delay=None)
                opened.set()
                yield socket

        async def rotate():
            await opened.wait()  # Connection lifetime starts after the handshake.
            await self._sleep(self.rotation_interval)

        feed = BinanceKlineFeed(self.symbols, self.worker.submit, connector=connector)
        session = asyncio.create_task(feed.run(session_stop))
        rotation = asyncio.create_task(rotate())
        stopping = asyncio.create_task(stop.wait())
        changed = asyncio.create_task(self._reconfigure.wait())
        try:
            done, _ = await asyncio.wait((session, rotation, stopping, changed), return_when=asyncio.FIRST_COMPLETED)
            if session in done:
                await session
                raise FeedConnectionError("market feed ended unexpectedly")
            if stopping in done:
                return False
            if changed in done:
                reason = "market membership changed"
            else:
                await rotation  # Surface errors in an injected timer.
                reason = "planned connection rotation"
            self._status(state="rotating", last_disconnect=reason)
            session_stop.set()
            await session  # Close the old session before opening the next one.
            return True
        finally:
            for task in (session, rotation, stopping, changed):
                if not task.done():
                    task.cancel()
            await asyncio.gather(session, rotation, stopping, changed, return_exceptions=True)

    async def _backoff(self, delay: float, stop: asyncio.Event) -> None:
        sleeping = asyncio.create_task(self._sleep(delay))
        stopping = asyncio.create_task(stop.wait())
        changed = asyncio.create_task(self._reconfigure.wait())
        try:
            done, _ = await asyncio.wait((sleeping, stopping, changed), return_when=asyncio.FIRST_COMPLETED)
            if sleeping in done:
                await sleeping
        finally:
            for task in (sleeping, stopping, changed):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeping, stopping, changed, return_exceptions=True)

    async def _connections(self, stop: asyncio.Event) -> None:
        first, ceiling = True, self.base_delay
        while not stop.is_set():
            self._reconfigure.clear()
            self._status(state="connecting", retry_delay=None,
                         reconnect_attempts=self.status.reconnect_attempts + (not first))
            first = False
            try:
                rotated = await self._session(stop)
                if not rotated:
                    return
                ceiling = self.base_delay
                self._status(consecutive_failures=0)
            except Exception as exc:
                self._status(last_disconnect=str(exc), last_error=str(exc))
                if not _transient(exc):
                    raise
                stable = (self._connected_since is not None
                          and self._monotonic() - self._connected_since >= self.stable_after)
                if stable:
                    ceiling = self.base_delay
                    self._status(consecutive_failures=0)
                sample = self._random()
                if not isinstance(sample, (int, float)) or not math.isfinite(sample) or not 0 <= sample <= 1:
                    raise ValueError("random_value must return a finite value in [0, 1]")
                delay = ceiling * (0.5 + 0.5 * sample)
                self._status(state="backoff", retry_delay=delay,
                             consecutive_failures=self.status.consecutive_failures + 1)
                await self._backoff(delay, stop)
                ceiling = min(self.max_delay, ceiling * 2)

    async def run(self, stop: asyncio.Event) -> None:
        if self._running:
            raise RuntimeError("WebSocket supervisor is already running")
        if stop.is_set():
            return
        self._running = True
        self.status = ConnectionStatus(state="connecting")
        worker_stop = asyncio.Event()
        worker = asyncio.create_task(self.worker.run(worker_stop))
        ready = asyncio.create_task(self.worker.ready.wait())
        stopping = asyncio.create_task(stop.wait())
        tasks = [worker, ready, stopping]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await worker
                raise MonitoringFailure("recovery worker stopped unexpectedly")
            if stopping in done:
                return
            connections = asyncio.create_task(self._connections(stop))
            tasks.append(connections)
            done, _ = await asyncio.wait((worker, connections, stopping), return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await worker  # Storage/unresolved-gap failures are fatal, not socket retries.
                raise MonitoringFailure("recovery worker stopped unexpectedly")
            if connections in done:
                await connections
        except Exception as exc:
            self._status(state="unhealthy", last_error=str(exc), retry_delay=None)
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._running = False
            if self.status.state != "unhealthy":
                self._status(state="stopped", retry_delay=None)
