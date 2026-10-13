"""Explicit file configuration for the live touch monitor; no delay fallbacks."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


DELAY_KEYS = {"previous_week": "PWLH_delay", "previous_month": "PMLH_delay",
              "previous_quarter": "PQLH_delay"}


@dataclass(frozen=True)
class TouchSettings:
    delays: Mapping[str, Decimal]
    max_age_seconds: Decimal
    email_retry_seconds: Decimal
    highlight_seconds: Decimal


def load_touch_settings(path):
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"required environment file not found: {path}")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read required environment file: {path}") from exc
    values = {}
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            raise ValueError(f"{path.name}:{number}: expected NAME=value")
        key, value = (part.strip() for part in line.split("=", 1))
        if key in values:
            raise ValueError(f"{path.name}:{number}: duplicate setting {key}")
        if value[:1] in ("'", '"'):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"{path.name}:{number}: unclosed quote")
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].strip()
        values[key] = value

    def positive(key):
        try:
            value = Decimal(values.get(key, ""))
        except InvalidOperation:
            raise ValueError(f"{path.name}: set {key} to a positive number") from None
        if not value.is_finite() or value <= 0:
            raise ValueError(f"{path.name}: {key} must be positive; zero is not allowed")
        # datetime/timedelta must be able to represent the configured durations.
        if value > Decimal("365000"):
            raise ValueError(f"{path.name}: {key} is too large")
        return value

    delays = MappingProxyType({name: positive(key) for name, key in DELAY_KEYS.items()})
    from crypto_monitor.core.touch_monitor import validate_delays
    validate_delays(delays)
    return TouchSettings(delays, positive("NOTIFICATION_MAX_AGE_SECONDS"),
                         positive("EMAIL_RETRY_SECONDS"), positive("FRESH_HIGHLIGHT_SECONDS"))
