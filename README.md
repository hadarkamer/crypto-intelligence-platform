Stage 2 Gap + Liquidity Sum Patch

Adds:
/gap [limit]
- Calculates average percentage gap between Short Max Pain and Long Max Pain:
  abs(short_max_pain - long_max_pain) / current_price * 100
- Shows AvgGap%, MaxGap timeframe, MinGap timeframe.

/liqsum
- Sums liquidation amounts by timeframe:
  Short$, Long$, Dominant side, Long-Short$, Ratio.
- Adds TOTAL row across all timeframes.

Test after deploy:
/collect
/gap
/gap 30
/liqsum
