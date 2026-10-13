# Cryptocurrency level-touch monitor

Read-only Binance USD-M futures monitoring. Live trade prices activate a watched
high or low as soon as they reach or pass it, including equality. Candle closes
are not required. This project does not place trades.

Read [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) for plain-language examples,
settings, historical discovery, email behavior, and project structure.

## Setup and run

Requires Python 3.10+ and the dependencies in `requirements.txt`:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
cp .env.example .env
```

First edit `.env` and supply your own positive values for `PWLH_delay`,
`PMLH_delay`, and `PQLH_delay`, in days. Blank, zero, negative, or invalid values
stop startup. Fractional days are accepted; there are no code defaults for these
settings. Monday low/high are first-only and have no delay variable.

```sh
python -m crypto_monitor --database ./monitor.sqlite3 --top-n 100 --layout table
```

`--env-file /path/to/settings.env` selects another required file. Inclusions add
eligible markets beyond the top N; exclusions win. `--top-n 0` selects only
explicit inclusions. Public Binance data does not require API credentials.

## Behavior

- Startup shows a loading screen, quietly scans current watch histories, and
  marks genuinely missing data unavailable. It begins fresh alerts after the
  live handoff. Historical minute times are approximate, using the minute end.
- Previous-week, monthly, and quarterly watches repeat after independently
  configured delays. Ignored touches do not extend those delays. A fresh price
  still beyond a level can activate again after expiry without returning first.
- Monday watches each activate once; they expire at the next Monday open and
  are undefined throughout Monday until the completed range is available Tuesday.
- Monthly counts include accepted activations in the current monthly watch.
  Quarterly badges hide accepted activations after their delay expires.
- State survives restart. Missed touches and reconnect recovery are quiet.
  Changing a delay recalculates eligibility from saved times, preserving counts.
- Market selection and reference levels refresh daily at 00:00 UTC. Volumes and
  unavailable-watch retries refresh at five-minute UTC boundaries. Removed coins
  leave the current view; newly selected coins receive quiet history checks.
- Terminal timestamps use Europe/Sofia; internal comparisons and periods use UTC.

## Terminal controls

Start with `--layout table` or `--layout cards`; change layouts while running:

| Key | Action |
|---|---|
| t / c | Table / cards |
| n / p | Next / previous page |
| a / s | All / swept coins |
| q | Stop |

All selected coins are accessible through pages. `S` means swept live, `E` swept
earlier, `-` ready again, and `N/A` unavailable. Monthly parentheses show accepted
counts. A fresh panel emphasizes new live activations for
`FRESH_HIGHLIGHT_SECONDS`. Use `--no-tui` for plain logs. Redirected output keeps
plain mode. A display failure restores plain logging and monitoring continues.

## Optional separate email alerts

Email is off unless explicitly enabled:

```sh
python -m crypto_monitor --top-n 100 --email-enabled \
  --email-sender monitor@example.com --email-recipient you@example.com \
  --email-region eu-west-1
```

The sender must be verified in that SES region; sandbox accounts also require a
verified recipient. The standard Boto3 credential chain supplies authentication.
[SES identity verification](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html).

Fresh eligible activations send separate messages promptly. Historical touches
never queue email. Disabled-email runs accumulate no later mail backlog. Old
pending notifications are muted on restart. `EMAIL_RETRY_SECONDS` controls retry
spacing; `NOTIFICATION_MAX_AGE_SECONDS` expires stale events and failed messages.
Both timings are explicit editable values in `.env`. Sending holds no database
transaction. SES acceptance does not guarantee inbox delivery, and a lost
acceptance response can still cause a duplicate on retry.

## Reliability and limits

Use one service instance per SQLite database. Connections stay on their owning
threads; live state and new outbox entries commit atomically. Do not delete the
database to bypass a history problem. New additive touch tables preserve the
legacy candle/hit records. No other exchange's history is silently substituted
for Binance history.

The first history scan for many coins can take time; daily ranges and saved
cursors reduce unnecessary minute requests. Extremely short delays can exceed
historical minute precision. Incoming prices remain subject to exchange update,
network, and processing latency. Malformed messages and local storage failures
stop the service visibly; transient socket failures reconnect automatically.
Per-watch historical failures are unavailable and retried without pretending
those levels are untouched. Ctrl+C and SIGTERM stop the service; in-flight
synchronous requests and email work must finish or time out during cleanup.
Exit codes: 0 orderly stop, 1 runtime failure, 2 argument/settings errors.

## Code and research

The executable entry point runs `runtime/touch_service.py`. The new touch engine,
store, history scanner, trade adapter, dashboard, and email adapter are described
in the implementation guide. Existing daily-reference and market-selection code
is reused. Legacy closed-candle modules remain for compatibility, with their old
unit tests; [RECOVERY.md](RECOVERY.md) describes that older harness.

`research/` contains independent historical studies and is not used by the live
service. Planning discussions remain in `SWEEP_REFACTOR_PROPOSAL.md` and
`SWEEP_REFACTOR_FOLLOWUP.md`; the implementation guide describes current behavior.

Offline checks:

```sh
python -B -m unittest discover -s tests -t .
```
