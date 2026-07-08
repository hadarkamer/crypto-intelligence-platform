Stage 8 v2 Hyperliquid Control Probe Patch

Fixes:
- Python SyntaxError in /hyper_debug failure branch.
- Validated all Python files with py_compile.

Purpose remains diagnostic only:
- open Hyperliquid page
- try selecting symbol
- try refresh
- return Telegram summary instead of hanging
- no DB writes

Test:
/hyper_debug BTC

Then send:
- Telegram output
- Render [hyper] lines
