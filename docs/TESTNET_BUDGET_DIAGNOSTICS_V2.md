# Read-only budget diagnostics v2

Scope: inspect the existing account configuration and clarify the precise reason for a precheck failure. No order requests, signatures, transfers, leverage changes, new credentials, or activation of unattended execution.

## Why change the old check

The previous `OUTSIDE_CONSERVATIVE_LAB_BUDGET` combined three different failures: requested quantity exceeding the exchange-reported size cap, full entry notional plus 1% exceeding unheld USDC, or that same full cash amount exceeding exchange-reported available margin. The log did not identify which comparison failed. Full notional coverage was an additional laboratory policy, not the exchange's leveraged margin formula.

## New behavior

- Preserve all three supplied prices and the same floored quantity from 20 USD / abs(entry - stop).
- Record each legacy comparison independently on the SAME fresh data sample. This is a fresh reproduction, not a reconstruction of historical balances.
- Read existing leverage and mark from activeAssetData; validate leverage against current metadata. Never choose a multiplier, use a missing-value default, or modify the account.
- Estimate initial margin as quantity * mark / existing leverage.
- Add adverse entry-versus-mark loss and a separate 1% reserve on the larger entry/mark notional. This buffer is not a statement of actual fees or a guarantee of maximum loss.
- Retain both exchange size and available margin caps, the unheld-USDC constraint, and the 5,000 USD laboratory notional bound. No resizing to force a pass.
- Continue conservatively using minima of the two capacity values: official documentation presents the arrays without labeling their side indices. This may reject a permitted single-side action; it is not a complete exchange buying-power implementation.
- Report each failed comparison, the existing leverage used, and a digest of the tested price plan; do not log addresses, keys, prices or balances. The public health page remains static.
- Check local key/address correspondence independently before the budget evaluation, so a budget failure no longer looks like an invalid key.

Missing leverage/metadata, malformed input and expired samples remain blocking. A pass is a read-only estimate, not authorization or proof that the exchange will accept an order. It does not assess liquidation risk or override the one-shot executor's separate checks. The actual sender remains unchanged and is not called by this runtime. No timestamps on source alerts are reset; a configured old alert is only being checked, not promoted to a new trade.

## Verification

60 offline runtime tests passed before publication (40 existing plus 20 new, with fixture updates for the newly checked leverage metadata). The local files were compared to repository blob identities; the code and test content submitted match the tested content (the test file differs only by an absent trailing newline).

The existing Render build runs this same test suite before deployment. Actual account results must be read from the post-deployment logs; no live outcome is claimed in this design note.

## Primary references checked

- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes
