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
cp -n .env.example .env
```

First edit `.env` and supply your own positive values for `PWLH_delay`,
`PMLH_delay`, and `PQLH_delay`, in days. Blank, zero, negative, or invalid values
stop startup. Fractional days are accepted; there are no code defaults for these
settings. Monday low/high are first-only and have no delay variable.

```sh
crypto-monitor
```

The default run monitors the **top 100 markets**, uses the **table dashboard**,
and starts with **email on**. Press `e` to toggle email while running. In this
workspace, `crypto-monitor` is installed on your shell's PATH and uses the
project's existing virtual environment; activation is not needed for daily use.
For a fresh installation elsewhere, the editable install above adds the command
to the activated virtual environment.

Common overrides stay short:

```sh
crypto-monitor --no-email
crypto-monitor -n 50
crypto-monitor -l cards
```

All existing options remain available. `crypto-monitor --help` lists them, and
`python -m crypto_monitor` remains supported with the same defaults. For example:

```sh
crypto-monitor --top-n 50 --include BTCUSDT,ETHUSDT --exclude BNBUSDT \
  --database ./custom.sqlite3 --layout cards --no-email
```

The default database and settings paths are anchored to
`crypto-monitor/monitor.sqlite3` and `crypto-monitor/.env`, even when launched
from another directory. An explicit `--database ./custom.sqlite3` remains relative
to the directory where you run the command. `--env-file /path/to/settings.env`
selects another required file. Inclusions add
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
| e | Toggle email alerts on/off |
| q | Stop |

All selected coins are accessible through pages. `S` means swept live, `E` swept
earlier, `-` ready again, and `N/A` unavailable. Monthly parentheses show accepted
counts. A fresh panel emphasizes new live activations for
`FRESH_HIGHLIGHT_SECONDS`. Use `--no-tui` for plain logs. Redirected output keeps
plain mode. A display failure restores plain logging and monitoring continues.

## Optional separate email alerts

Email starts on by default. Start with it off using:

```sh
crypto-monitor --no-email
```

In the interactive dashboard, press `e` to turn email on or off without
restarting. The header shows **Email ON (e)** or **Email OFF (e)** in both table
and card layouts. This changes the current session only; each new run starts
with email on unless `--no-email` (alias `--email-disabled`) is supplied.
`--email-enabled` remains accepted to explicitly select the default. Plain-log
runs use the startup choice.

Turning email off stops new notifications and discards pending retries. An email
already in progress can finish and save its result. Monitoring continues while
email is off. Turning it back on sends only future eligible live activations,
with no backlog from the disabled period. If settings are missing or invalid
when you press `e`, email stays off and the dashboard shows an attention message.
Invalid enabled-email settings at startup stop the run with a configuration
error; use `--no-email` to start without email.

Startup loads both `Trading_NEW/.env` and the project's required `.env` (or
`--env-file`). Existing process variables take precedence, followed by project
values, then shared parent values. Neither file is modified. Monitor delays and
notification timings still come strictly from the project file.

The shared SES SMTP configuration uses `SES_SMTP_HOST`, `SES_SMTP_PORT` (default
587), `SES_SMTP_USER` (or `SES_IAM_USER`), and `SES_SMTP_PASS`. SMTP uses verified
TLS, with STARTTLS on port 587 and implicit TLS on ports 465/2465. `EMAIL_FROM`
and `ALERT_EMAIL_TO` supply the sender and single recipient; `EMAIL_TO` and
`EMAIL_RECIPIENT` are recipient fallbacks. `--email-sender` and
`--email-recipient` override these address defaults. SMTP delivery always uses
the endpoint and region in `SES_SMTP_HOST`.

When `SES_SMTP_HOST` is absent, the existing SES API transport uses Boto3's
standard credential chain, including AWS variables loaded from these files.
For the API, `--email-region` selects the region, defaulting to `AWS_REGION` or
`AWS_DEFAULT_REGION`.
SMTP credentials are used only with SMTP; they are not AWS API access keys.
[SES SMTP credentials](https://docs.aws.amazon.com/ses/latest/dg/send-email-smtp.html).
The sender must be verified in the SES region; sandbox accounts also require a
verified recipient.
[SES identity verification](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html).
If SES rejects a message because an identity is unverified, verify `EMAIL_FROM`
(and the recipient for sandbox accounts) in the region of the SMTP endpoint or
selected API region. Valid SMTP credentials alone do not verify an address.

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
