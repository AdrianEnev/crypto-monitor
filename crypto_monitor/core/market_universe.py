"""Select markets and rank alerts from supplied Binance-shaped snapshots.

No network access. Inputs are exchangeInfo's envelope and ticker/24hr's list.
Field reference: https://developers.binance.com/docs/derivatives/
usds-margined-futures/market-data/rest-api/Exchange-Information
and the adjacent 24hr-Ticker-Price-Change-Statistics documentation.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Collection, Iterable, Mapping


# Explicit project policy, not an exhaustive or automatically updated registry.
# Pass stablecoin_bases to replace this set when classifications change.
DEFAULT_STABLECOIN_BASES = frozenset({
    "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "USDS",
    "USDD", "USDE", "PYUSD", "USD1", "RLUSD", "FRAX", "LUSD", "GUSD",
    "SUSD", "EURC", "EURI", "AEUR",
})


def _parse_volume(value: Any) -> Decimal:
    """Accept exact decimal inputs; never convert a binary float."""
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("quote volume must be a decimal string, integer, or Decimal")
    try:
        volume = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid quote volume") from exc
    if not volume.is_finite() or volume < 0:
        raise ValueError("quote volume must be finite and nonnegative")
    return volume


def _index_by_symbol(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    indexed = {}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("each row must have a nonempty symbol")
        if symbol in indexed:
            raise ValueError(f"duplicate symbol: {symbol}")
        indexed[symbol] = row
    return indexed


@dataclass(frozen=True)
class Market:
    symbol: str
    base_asset: str
    quote_volume: Decimal


def is_eligible_market(
    info: Mapping[str, Any],
    *,
    excluded_symbols: Collection[str] = (),
    stablecoin_bases: Collection[str] = DEFAULT_STABLECOIN_BASES,
) -> bool:
    """Shared eligibility predicate; exclusion/base names must be uppercase.

    This deliberately does not require a ticker so adapters can detect missing
    data before the selector's usual missing-ticker omission takes effect.
    """
    base = info.get("baseAsset")
    return (
        info.get("status") == "TRADING"
        and info.get("contractType") == "PERPETUAL"
        and info.get("quoteAsset") == "USDT"
        and info.get("marginAsset") == "USDT"
        and isinstance(base, str)
        and bool(base)
        and base not in stablecoin_bases
        and info.get("symbol") not in excluded_symbols
    )


def select_markets(
    exchange_info: Mapping[str, Any],
    tickers: Iterable[Mapping[str, Any]],
    *,
    top_n: int = 20,
    include_symbols: Iterable[str] = (),
    exclude_symbols: Iterable[str] = (),
    stablecoin_bases: Iterable[str] = DEFAULT_STABLECOIN_BASES,
) -> list[Market]:
    """Return top N eligible markets union eligible inclusions, volume-ranked.

    Exclusions and stablecoin filtering always win. Membership is decided after
    filtering, so exclusions do not leave avoidable holes in the top N. Ties
    use ascending symbol. Configuration names are normalized to uppercase;
    snapshot fields retain Binance's exact spelling and case.

    Unknown inclusions and markets without tickers are omitted. Missing
    eligibility fields fail closed. Invalid volumes on eligible markets or
    duplicate snapshot symbols raise ValueError rather than inventing a rank.
    Zero volume is valid. top_n=0 selects only eligible explicit inclusions.
    """
    if type(top_n) is not int or top_n < 0:
        raise ValueError("top_n must be a nonnegative integer")
    included = {name.strip().upper() for name in include_symbols}
    excluded = {name.strip().upper() for name in exclude_symbols}
    stablecoins = {name.strip().upper() for name in stablecoin_bases}
    symbols = _index_by_symbol(exchange_info["symbols"])
    volumes = _index_by_symbol(tickers)

    eligible = []
    for symbol, info in symbols.items():
        base = info.get("baseAsset")
        if (
            not is_eligible_market(
                info, excluded_symbols=excluded, stablecoin_bases=stablecoins
            )
            or symbol not in volumes
        ):
            continue
        eligible.append(Market(
            symbol, base, _parse_volume(volumes[symbol].get("quoteVolume"))
        ))

    # Stable sorts avoid Decimal negation (which can round under its context).
    eligible.sort(key=lambda market: market.symbol)
    eligible.sort(key=lambda market: market.quote_volume, reverse=True)
    selected = {market.symbol for market in eligible[:top_n]} | included
    return [market for market in eligible if market.symbol in selected]


@dataclass(frozen=True)
class Alert:
    """Ranking metadata with volume captured at detection, not looked up later.

    alert_id is a caller-provided stable identity used only to break ties.
    """

    symbol: str
    captured_quote_volume: Decimal
    detected_at: datetime
    alert_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.captured_quote_volume, Decimal):
            raise TypeError("captured quote volume must be Decimal")
        _parse_volume(self.captured_quote_volume)
        if self.detected_at.utcoffset() != timedelta(0):
            raise ValueError("detection time must be timezone-aware UTC")
        if not self.symbol or not self.alert_id:
            raise ValueError("symbol and alert_id must be nonempty")


def rank_alerts(alerts: Iterable[Alert]) -> list[Alert]:
    """Volume descending, time descending, then symbol and alert ID ascending.

    Exact duplicates retain input order. No mutation or deduplication occurs.
    """
    ranked = sorted(alerts, key=lambda alert: (alert.symbol, alert.alert_id))
    ranked.sort(key=lambda alert: alert.detected_at, reverse=True)
    ranked.sort(key=lambda alert: alert.captured_quote_volume, reverse=True)
    return ranked
