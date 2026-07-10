Stage 17 — Timeframe Verification Fix

Recurring bug fixed:
CoinGlass sometimes accepted the click but kept showing the previous timeframe.
The old reader then saved the same table under a different timeframe label.

New safeguards:
- clicks tabs with several selector strategies
- polls until content changes
- checks the active tab when detectable
- creates a fingerprint from the first 10 rows
- rejects a timeframe if its fingerprint duplicates an earlier timeframe
- retries each timeframe up to 3 times
- rejected timeframes are marked missing and are not saved

Important:
It is better to save fewer verified rows than to save duplicated/mislabeled data.

Expected logs:
[dom] tf=24h verified=True ...
or
[dom] tf=24h REJECTED ... duplicate_of=12h

Test:
1. Deploy
2. /collect
3. Check Render logs for verified/rejected lines
4. /coin BTC
