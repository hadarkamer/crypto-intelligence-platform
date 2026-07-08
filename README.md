Stage 5 Fixes + BTC-like Patch

Changes:
1. /gap
   - Keeps Yoni's approved formula:
     abs(Short Max Pain - Long Max Pain) / Current Price * 100
   - Adds AvgGap$ column to help sanity-check extreme AvgGap% values.
   - Very high AvgGap% can be real for low-priced/high-volatility assets.

2. /consensus
   - Sorts AvgDist% from high to low inside the same score level, as requested.

3. /liqsum
   - Existing /liqsum still shows market balance by timeframe + TOTAL.
   - New /liqsum top [limit] shows coins with the highest total liquidity across all timeframes.
   - New /liqsum BTC shows liquidity balance for BTC by timeframe + TOTAL.

4. /btc_like [min_hits] [limit]
   - Shows coins whose closest Max Pain side matches BTC across timeframes.
   - Example: /btc_like or /btc_like 6
