# ETH MaxPain long experiment

Owner-authorized addition to the existing Telegram experimental roster.
Research identity: `88a0ed51ab391d6e`. Rule: `ETH_MAXPAIN_LONG_DIST1_3`.

For an eligible newly observed target T above source price S, require
1% <= (T/S - 1)*100 < 3%. All seven source timeframes are eligible:
12h, 24h, 48h, 3d, 1w, 2w, 1m. There is no prior-24-hour-range filter.
With D=T-S, wait for entry S-2D, target S+0.5D, and stop S-5D.
For S=2000 and T=2040, entry=1920, take=2020, stop=1800.
The take is halfway from the ORIGINAL SOURCE price to MaxPain, not halfway
from entry. The planned reward/risk ratio is 5/6 before costs.

Pending plans expire after 24 hours or cancel when original MaxPain is taken
first. Filled observations have no holding-time limit and no stop promotion.
Multiple distinct targets may coexist. A <=0.2% target-price gap blocks an
additional observation unless the shared explicit liquidity-growth proof
authorizes a larger timeframe. This proof remains conservative; historical
proof-producer parity has not been established. Target lifecycle, suspicious
post-touch handling, source validation, unknown outcomes and outbox rules all
reuse the already deployed MaxPain engine.

The ETH path uses Binance Spot ETHUSDT TRADE minute candles, matching the
archived ETH source. A separate durable scope bootstraps without historical
alerts. No existing formula configuration fingerprint or persistent state is
changed. No orders, account settings or position sizing are introduced.

Archived research: 31 profitable out of 32 closed, net $121.684326, mean
$3.802635, B20=2.498386. Three positions remained open at the cutoff.
Source coverage: 2026-08-29 21:35 to 2026-10-03 09:45 Israel time. These are
research results using $5 risk, $5000 notional cap and 0.06% round-trip costs,
not executed alert performance or an updated October5 backtest.
