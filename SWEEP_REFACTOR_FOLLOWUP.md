# Sweep monitor: brief follow-up

October 13, 2026. Review document only; implementation has not started.

This updates the earlier [proposal](SWEEP_REFACTOR_PROPOSAL.md). The distance/retreat idea is removed. The examples below explain the choices that still need an answer.

## 1. Your latest decisions

- A sweep is price reaching or passing the level, including equality. No candle close is required.
- Monday low/high are first-only; there will be no `MONLH_delay`. One calendar-boundary detail is clarified below.
- Previous-week, monthly, and quarterly sides use their own required `PWLH_delay`, `PMLH_delay`, and `PQLH_delay` values. Positive fractional days are allowed; zero is rejected. For example, `0.1` days equals 2 hours 24 minutes. There are no hidden numeric defaults.
- Delays are fixed. An ignored touch does not extend a deadline. Low and high remain independent for every coin.
- When the delay expires, another alert is allowed immediately. A return or minimum move away is not required.
- New reference periods reset the relevant state and delay.
- Fresh alerts begin once loading finishes. Historical discoveries remain visible without creating emails or urgent messages.
- Each notification email stays separate. With email disabled, no email backlog accumulates.
- Missing levels/history display **unavailable**. Internal time remains UTC; terminal times use Sofia.

**Consequence of delay-only repeats:** if price stays above a high for 15 days and the delay is five days, it can activate initially and again at days 5, 10, and 15. Those repeats do not require another physical crossing. A fresh live price must confirm it is still beyond the level; stale cached prices should not create emails. If price is back inside at expiry, wait until it reaches the level again.

## 2. Monthly count: a simpler choice

With the distance rule removed, we have two possible meanings of “how many sweeps”:

In this example, price returns inside the level before day 6, so expiry alone does not activate it. Its next crossing is on day 7.

| Example: five-day delay | Count every crossing | Count only accepted activations |
|---|---|---|
| Day 1: price crosses the monthly high | Count 1; alert allowed. | Count 1; alert allowed. |
| Day 4: price crosses it again | Count 2; no alert. | Still count 1; no alert. |
| Day 7: price crosses it again | Count 3; alert allowed. | Count 2; alert allowed. |

**My recommendation is to count only accepted activations.** That prevents hundreds of small crossings during the delay from inflating the count. Quiet historical activations can count too, without sending email.

With your expiry rule, an activation can also occur while price remains beyond the level. The count would therefore mean “how many times this level became an accepted activation,” rather than “how many separate physical crossings occurred.”

The monthly count stays visible for the entire current monthly watch period, even after a swept badge clears. Keeping the count visible after expiry is separate from counting every ignored crossing. Counting every crossing remains possible, but it can still produce the large consolidation counts you wanted to avoid.

## 3. Historical accuracy: what we can know

Suppose an old one-minute candle covers 10:03–10:04 and its high reaches your level. We know price reached the level **during that minute**. The candle does not reveal the exact second or every crossing within it.

For the simpler delay-only version, I recommend minute-based historical reconstruction with approximate times clearly labelled. Live events can use the event time available from the live feed. Very short delays or several crossings inside one historical minute cannot be reconstructed exactly from that candle alone.

The choice is simply: are approximate historical minute times/counts sufficient, or do you need finer trade-history reconstruction before displaying them?

## 4. Quarterly badge: which touch keeps it visible?

Example: an accepted quarterly sweep occurs Monday at 10:00, its delay is five days, and another touch is ignored Thursday at 10:00.

| Badge rule | What happens |
|---|---|
| Use Monday's accepted activation | Badge clears Saturday at 10:00. The ignored Thursday touch changes neither the delay nor the badge. |
| Use Thursday's ignored touch too | Badge remains until Tuesday at 10:00, although the notification delay ended Saturday. |

**I recommend the first rule**, using exact elapsed hours. It keeps the terminal consistent with your fixed delay and “untouched again after expiry” behavior. Old quarterly activations outside that window disappear from the main view.

If a new eligible live activation occurs at Saturday's expiry, it starts a new delay and the badge becomes swept again immediately. There is no need to show an old two-month-old quarterly touch.

## 5. “Quiet” means visible, without a notification

An **offline touch** happened while the script was stopped. A **quiet touch** is a past touch discovered while loading or recovering history. Quiet does not mean hidden or discarded.

Example: you close the monitor Tuesday, price touches PWL Wednesday, and you restart Thursday. Startup shows PWL as swept if its delay is still active, records the historical time, and restores its deadline. It sends no email and does not place Wednesday's event in the fresh-alert panel. Monthly counts can include qualifying historical activations. Expired quarterly history stays outside the main view.

I recommend the same treatment for touches recovered after a connection failure: show the information as **Earlier**, with no urgent email. A delayed live event also needs a maximum age before it should be treated as Earlier.

One edge case needs your decision: a delay expired while the script was off, and price is already beyond the level when loading finishes. Should the first current live price activate immediately, or should that existing visit remain quiet until price leaves and reaches the level again? Your immediate-expiry rule and your no-offline-email rule overlap here.

## 6. Emails and settings changes

I recommend initiating **one separate email per eligible fresh level activation**, promptly after it happens. No combined emails, no historical emails, and no queued mail while email is disabled. Immediate detection and immediate email sending are separate changes: the current sender waits for five-minute boundaries.

Still unanswered: what to do with an email that failed to send before you closed the script. My recommendation is to mute unsent messages from an earlier run when the new run begins. Retain their records and level state, but do not send yesterday's message today. During an active run, failed fresh sends can retry for a chosen useful age, then become quiet history.

For changed delay settings, I recommend using the saved last accepted time plus the new delay to calculate the current deadline. Preserve already recorded counts and history. A settings change alone should not send an email; notification eligibility is evaluated against live data after loading.

For unavailable data, my recommendation is to start the verified watches and mark only the affected watches unavailable. An unavailable watch creates no alerts until its necessary data is verified. Other exchanges can be deferred for this simpler version.

## 7. Terminal examples

### Loading screen

```text
CRYPTO MONITOR | LOADING

Reference levels       100 / 100 coins checked
Past touches            62 / 100 coins checked

Recording earlier touches quietly.
Fresh alerts begin when loading finishes.
```

Completed checks can identify unavailable data; they must not falsely label it untouched. Once all startup checks and the live handoff finish, replace this view with the selected layout.

### Option A: compact overview — recommended

```text
CRYPTO MONITOR | LIVE | 100 coins | Email on
13 Oct 16:32:04 Europe/Sofia

Fresh
16:31:58  FILUSDT  Previous-week high  1.231

Coin      Monday L/H  Week L/H  Month L/H    Quarter L/H
FILUSDT   E / -       - / S     E(2) / -(1)  - / -
ADAUSDT   - / -       E / -     -(1) / -     - / E
NEWUSDT   - / -       - / -     unavailable  unavailable

E = swept earlier; S = swept live; - = ready for another alert
(n) = monthly activation count; L/H = low/high
Page 1/3 | 47 matching coins | 4 unavailable levels
```

The monthly counts here are illustrative until you choose the counting rule. `-(1)` means its delay has expired while its count remains one. The Earlier/Live letter records the origin of the active badge; a live badge receives fresh emphasis only temporarily.

### Option B: readable coin cards

```text
CRYPTO MONITOR | LIVE | Email on
13 Oct 16:32:04 Europe/Sofia

FILUSDT
Monday         Low: swept earlier    High: ready
Previous week  Low: ready            High: swept live
Previous month Low: swept, count 2   High: ready, count 1
Previous qtr   Low: ready            High: ready

NEWUSDT
Previous month unavailable
Previous qtr   unavailable

Page 1/24 | 2 coins shown | All / Swept filter
```

Cards are easier to read; the table lets you see more coins at once. Paging and an All/Swept filter make every coin accessible. A selected coin can show its prices, historical times, and next eligible times in a detail view.

### Fresh-event emphasis

```text
NEW LIVE ACTIVATION
16:31:58  FILUSDT  Previous-week high  1.231
Next eligible: 18 Oct 16:31:58 Sofia
```

This would be highlighted in the actual terminal. An earlier discovery would use ordinary styling and never appear here. My suggested starting point is visual emphasis without sound, clearing the fresh highlight after one minute. That duration is a proposal, not a hidden code default.

## 8. Remaining answers

1. **Monthly count:** count only accepted activations, as recommended, or every physical crossing even during the delay?
2. **Historical precision:** are approximate minute-based historical times/counts acceptable?
3. **Quarterly badge:** clear it based on the accepted activation's fixed deadline, ignoring suppressed touches, as recommended?
4. **Initial values:** what values should the environment file contain for `PWLH_delay`, `PMLH_delay`, and `PQLH_delay`?
5. **Monday boundary:** you said “until the next Monday close,” which is Tuesday 00:00 UTC. Should the old Monday levels remain active through the following Monday, then be replaced Tuesday? The existing code instead removes them Monday at 00:00 UTC and leaves Monday unavailable until Tuesday. Which do you want?
6. **Startup beyond the level:** if its delay expired while off and price is still beyond it when loading finishes, activate immediately from the first live price, or keep that existing visit quiet until a new crossing?
7. **Reconnect and event age:** approve showing recovered touches as Earlier without email? How old can a delayed live event be and still deserve an urgent notification—30 seconds, one minute, or another value?
8. **Email timing and retries:** approve prompt separate sends? Approve muting pending messages from previous runs? How long may a failed fresh email retry during the active run? Email times can use Sofia for convenience unless you prefer UTC there.
9. **UI:** table or cards? Show all selected coins or swept coins by default? Is the proposed one-minute visual highlight sufficient, or do you want a sound or a different duration?

The recommended settings-change and unavailable-data policies above can be used unless you want a different approach. This document creates no runtime or environment-file changes.
