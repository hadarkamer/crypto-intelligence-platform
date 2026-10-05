# Experimental notification roster, 2026-10-05

Owner request: retain XRP R2732 and later experiments, replace the original
SOL proximity experiment with the 48-closed-trade range variant, and add SOL
g65/k49 plus the HYPE, DOGE and XRP MaxPain variants.

The permanent roster in `alert_delivery_policy.py` applies even when the
deployment still specifies `ALERT_DELIVERY_PROFILE=ALL`. Ordinary Watch
notifications and research collection retain their existing policy. There are
no order, execution-service, exchange-account or position-sizing changes.

| Active rule | Research identity | Direction |
|---|---|---|
| R2732_XRP_SHORT_NY_WEEKDAYS_LOCK | XRP R2732 weekdays, original lock | Short |
| HYPE_ROW71205_SHORT | ROW71205, explicitly prospective Hyperliquid source variant | Short |
| SOL_MAXPAIN_DIST1_3_RANGE24 | 7815ee3bcf3531fb | Both |
| SOL_G65_K49_PROFIT_LOCK | btcgrid_SOL_SHORT_g65_k049__cap1__LOCK_075TP_025TP | Short |
| HYPE_MAXPAIN_DIST05_15_LONG_TF | 66e5f77c438ba155, explicitly prospective Hyperliquid source variant | Both |
| DOGE_MAXPAIN_DIST15_25_LONG_TF | c39e5acaacf1247f | Both |
| XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF | 8e6bdb02709aa0cc | Long |

## Retirement and migration

Older manual formulas, ordered-v7 notifications, dual CVD65, the dedicated
MP65/CVD experiment and U21 cannot admit or send new experimental alerts.
U21 loads an existing observation after restart and continues only its outcome
monitoring. It does not create a missing scope or load new BTC signal history.

SOL uses a whitelisted, transactional configuration migration. Old pending
entries are cancelled, while OPEN/UNKNOWN positions keep their original levels
and identity. Pending unsent notifications are cancelled. Receipts and episode
history remain, preventing old target replay. In-flight delivery postpones the
migration until its uncertainty is recorded. Old and new counters are separated.

## Concurrency and evidence

The single-position limits of R2732, ROW71205 and g65 are per formula. There is
no new cross-formula mutual exclusion: a MaxPain long and an independent short
can coexist. Each MaxPain formula enforces its own 0.2% target-price exclusion.
HYPE/XRP may admit a larger-timeframe leg only with an explicit current target
liquidity chain. Missing proof fails closed; an unrelated cluster score cannot
authorize another leg. The live proof contract is conservative and is not a
claim that the unavailable historical proof-producer was recovered byte-for-byte.

Historical P&L remains research evidence, not executed alert performance.
Especially, HYPE Hyperliquid perpetual minute paths do not inherit the research
statistics of another source. New workers bootstrap observed targets without
emitting historical signals. MaxPain pending plans expire after 24 hours; filled
positions have no holding-time limit. Source gaps or ambiguous minute ordering
cannot create an assumed profitable outcome.

The public cached health endpoint reports the selected roster, each worker's
source/readiness, separate legacy SOL positions and migration counts. It does
not trigger extra provider requests.
