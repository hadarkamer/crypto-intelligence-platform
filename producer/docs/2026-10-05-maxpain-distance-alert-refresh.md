# Requested MaxPain experimental alert refresh — 2026-10-05

Notification and observation only. No order, position sizing, leverage, or Testnet
execution path is introduced. The source quote S and original CoinGlass target T
are frozen from the same captured Watch row. Let d = T-S (signed).

| Rule | Research ID | Direction / source target filter | Entry | Take | Stop | Growth exception |
|---|---|---|---|---|---|---|
| SOL_MAXPAIN_DIST1_3_RANGE24 | 7815ee3bcf3531fb | Both; 1% <= abs(d)/S*100 < 3%; T inside preceding closed 24h SOL Spot range | S-2d | T | S-5d | No |
| HYPE_MAXPAIN_DIST05_15_LONG_TF | 66e5f77c438ba155 | Both; 0.5%–<1.5%; 3d,1w,2w,1m | S-2d | S+0.5d | S-5d | Verified tiers only |
| DOGE_MAXPAIN_DIST15_25_LONG_TF | c39e5acaacf1247f | Both; 1.5%–<2.5%; 3d,1w,2w,1m | S-2d | S+0.5d | S-5d | No |
| XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF | 8e6bdb02709aa0cc | Long; 2%–<4%; 12h,24h,48h | S-0.5d | T | S-5d | Verified tiers only |

All four wait for adverse entry for at most 24 hours, arm from the next minute
following the prospective decision, and have no holding deadline or stop
promotion. The score slot must still be SCORED with an exact target match and
finite final score. Distance replaces the old proximity-score cutoff. All seven
source timeframes must be valid to admit new plans. Existing absent/return target
cycles remain independent; unchanged already-touched targets cannot re-enter.

SOL uses exactly the 1,440 completed minutes before the captured decision minute
for its range predicate; the current partial minute is never included. The
partial current minute can only veto a source target already touched before
admission or a stale notification, never prove an entry/outcome. Partial-take
levels do not cancel pending entries; only reaching the original target does.
When OHLC cannot order an entry and take or both exits, the observation is UNKNOWN
and retains capacity instead of manufacturing a win or sending a new entry.

## Source contracts and evidence limits

SOL, DOGE, and XRP monitor Binance Spot trade minutes. HYPE explicitly monitors
Hyperliquid HYPE perpetual trade minutes using the already validated bounded
adapter; it never falls back to another market. Source quote provenance
(price_source, price_market, price_pair, price_instrument) is retained separately
because the operational S used by Watch can originate from a different provider.
The alert distinguishes the collected S source from the monitoring route.
The HYPE research results from its historical route do not validate the new
Hyperliquid source variant. Health and notification copy label that limitation.

The historical full-research results requested were respectively 48/38, 34/28,
31/25, and 51/50 closed/winning positions; net 136.13, 83.28, 72.51, and 76.91 USD
at 5 USD price risk, 5,000 USD notional ceiling and 0.06% roundtrip fees. These
research-only sizing assumptions are not notification execution rules.

## Concurrent observations and liquidity proof

Each formula has a distinct state scope. Its pending, OPEN, and UNKNOWN positions
reserve target prices. A target within inclusive 0.2% of another reserved target
is blocked unless that formula enables the growth exception and all proofs below
are present. Independent formulas may overlap; no cross-formula suppression is
silently applied.

For HYPE/XRP an incoming longer timeframe must prove every existing nearby leg,
and every previously filled leg of the same exact target episode, from the
current frozen Watch generation. The old timeframe must still quote the exact
old leg target, direction must agree, every intervening timeframe must exist,
all chain prices must fit within 0.2%, and every adjacent amount increase must
meet the bot's frozen ranking thresholds:

12h→24h 15%; 24h→48h 20%; 48h→3d 15%; 3d→1w 25%; 1w→2w 25%; 2w→1m 30%.

No missing amount, skipped intermediate tier, aggregate cluster score, unrelated
cluster, previous snapshot, or opposite direction substitutes for this proof.
The original research proof consumer and selected journals were available, but
the upstream proof-producing archive was unavailable during this activation.
Accordingly this explicit, conservative reconstruction of current bot tiers is
not asserted to reproduce every historical growth exception. Health labels the
proof contract. Without full evidence the overlapping entry is declined, while
nonoverlapping valid targets continue normally.

## Replacing the SOL version safely

Only legacy configuration SHA256
`698fec7779c0b692d1c939f9541aad2c79c3781b24a140c26c9741c95e2d3941`
is eligible for automatic one-way migration. The existing scope is locked using
its PostgreSQL advisory and row lock. Pending old-version plans and unsent entry
intents are cancelled. Their consumed episode flags prevent retrospective
replacement entries. Filled OPEN and unresolved UNKNOWN observations retain all
original prices, identity, cursor and exits, and are marked `legacy_formula`.
They continue to reserve their targets. Old counts are retained separately.
A delivery in flight blocks migration until terminal; unknown delivery is never
retried. The config fingerprint fences every old-process write after migration.
Restart is idempotent and never cancels or changes a filled exit.

Offline regression coverage: all four boundaries and geometries, closed range,
slot identity, source completeness, growth tiers and previous filled legs,
partial take ambiguity, source routing without fallback, and transactional
migration of two OPEN plus eight PENDING plans with restart/config fences.
