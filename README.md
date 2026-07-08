Stage 3 Analysis Refactor

Adds analysis.py as the central calculation engine.

No intended user-facing output changes.
Existing commands now use analysis.py:
- /consensus
- /gap
- /liqsum

Purpose:
- reduce duplicated calculations inside main.py
- prepare for /market, /btc_like, and later probability/scoring engine

Test after deploy:
/collect
/consensus
/gap
/liqsum
