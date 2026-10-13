"""Durable reference lookup by candle opening time; synchronous worker-side I/O."""

from threading import RLock

from crypto_monitor.runtime.gap_recovery import ReferenceUnavailable
from crypto_monitor.core.level_sweep import reference_periods, reference_regime
from crypto_monitor.core.monitoring_engine import ReferenceLevels
from crypto_monitor.persistence.sqlite_store import SQLiteStore


def references_from_levels(as_of, levels):
    periods = reference_periods(as_of)
    if set(levels) != set(periods):
        raise ValueError("incomplete reference level response")
    return {name: ReferenceLevels(period, levels[name])
            for name, period in periods.items() if period is not None}


def complete(references):
    return references is not None and all(ref.levels is not None for ref in references.values())


class DurableReferences:
    """SQLite is authoritative; cache only complete, immutable symbol/regime pairs.

    Call on a processing thread, never on the event loop. The lock coalesces
    misses for this provider and publishes a full batch to its cache only after
    SQLite commits. Connections are opened/closed on the calling thread. No
    transaction is held across HTTP, and no snapshots are automatically pruned.
    """

    def __init__(self, database, fetch_levels, *, store_factory=SQLiteStore):
        self.database, self.fetch_levels, self.store_factory = database, fetch_levels, store_factory
        self._cache = {}
        self._lock = RLock()

    def persist(self, as_of, snapshots):
        regime = reference_regime(as_of)
        with self._lock:
            with self.store_factory(self.database) as store:
                saved = store.save_reference_snapshots(as_of, snapshots)
            for symbol, refs in saved.items():
                if complete(refs):
                    self._cache[(symbol, regime)] = refs
            return saved

    def __call__(self, symbol, opened):
        regime = reference_regime(opened)
        key = (symbol, regime)
        with self._lock:
            if key in self._cache:
                return self._cache[key]
            try:
                with self.store_factory(self.database) as store:
                    refs = store.load_reference_snapshot(symbol, opened)
                if not complete(refs):
                    levels = self.fetch_levels(symbol, as_of=opened)
                    refs = references_from_levels(opened, levels)
                    refs = self.persist(opened, {symbol: refs})[symbol]
                if not complete(refs):
                    missing = ", ".join(name for name, ref in refs.items() if ref.levels is None)
                    raise ValueError(f"required daily history unavailable: {missing}")
                self._cache[key] = refs
                return refs
            except Exception as exc:
                raise ReferenceUnavailable(
                    f"unresolved reference snapshot for {symbol} at {opened.isoformat()} "
                    f"(valid {regime.start.isoformat()} to {regime.end.isoformat()}): {exc}"
                ) from exc
