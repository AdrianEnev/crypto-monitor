"""Compatibility entry point; ``crypto-monitor`` calls the same main function."""

from crypto_monitor.runtime.service import main


if __name__ == "__main__":
    raise SystemExit(main())
