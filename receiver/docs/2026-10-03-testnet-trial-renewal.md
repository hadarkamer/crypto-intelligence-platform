# Testnet trial renewal and short-permit preparation

The entry cap belongs to the bot's durable PostgreSQL journal, not Hyperliquid.
`HL_TESTNET_ENTRY_ATTEMPT_CAP=one_per_role_v1` allows one begun ENTRY per account
role since that role's `HL_TESTNET_*_NOT_BEFORE` UTC epoch. Rejected, uncertain and
definitely-unsent attempts all consume it. Restarting alone never resets it.

The 2026-10-03 BNB short attempt was durably begun and then certified
`TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED` before HTTP. It is not a fill and must not
be replayed. The timing repair warms a single thread-local PostgreSQL socket
before transport admission and reuses it for separately committed transactions
through one dispatcher cycle. All database identity, request integrity, account,
source age, emergency, budget and durable attempt checks remain. Transaction
failure poisons the scope; there is no reconnect/retry or permit refresh.
The one-second permit, five-second attempt and fifteen-second evidence limits
are unchanged. This removes repeated socket setup; a fresh real Testnet fill is
still required to validate live timing end to end.

## Operator steps in Render

1. Open the existing Testnet web service `hl-testnet-check-yoyo`
   (`srv-dakptbh594qs7395460g`), not the production alert service. Verify the
   deployed commit includes this repair. Do not create another worker/service.
2. Confirm both feeds are `CONNECTED_RECONCILED`, the emergency supervisor is
   `PASS_COMPLETE`, there are no unresolved requests/latches, and existing open
   positions (if any) are protected before renewing the entry scope.
3. In **Environment**, keep `HL_TESTNET_ENTRY_ATTEMPT_CAP=one_per_role_v1`.
   Keep all Testnet routing, risk and protection variables unchanged.
4. For only the account(s) being renewed, change
   `HL_TESTNET_LONG_NOT_BEFORE` and/or `HL_TESTNET_SHORT_NOT_BEFORE` to the
   current UTC timestamp at the time of renewal, in ISO-8601 form
   `YYYY-MM-DDTHH:MM:SS+00:00`. In Israel on this date, UTC is local time minus
   three hours. Use a fresh actual timestamp, not a date copied from this file.
   This starts a new bounded epoch and excludes all older alert sources.
5. The matching `HL_TESTNET_LONG_ENTRY_ENABLED` or
   `HL_TESTNET_SHORT_ENTRY_ENABLED` must be `true` for that account's next
   fresh alert to be eligible. Both are currently true; no change is needed.
6. Save the changed environment and deploy once. If Render offers **Save,
   rebuild, and deploy**, use that; if only saved without deploy, manually
   deploy the latest commit once. Wait for **Live** before checking health.
7. Observe the next newly delivered trade card: exact entry request and actual
   fills, correct account/side, STOP/TP coverage of the filled quantity, and
   subsequent cancellation/closure evidence. Merely recording an alert or
   receiving an order ACK does not prove a fill or full protection.

Renewing both roles permits at most two begun entry attempts in total: one
per role, regardless of coin. Renewing only one role leaves the other cap
consumed. Never delete request/nonce rows, erase the cap variable, alter risk,
disable emergency checks or move the epoch backward. Ordinary informational
Magnet/CVD messages are not automatically executable trade cards.

No trial epoch is renewed by the timing deployment itself. The operator
renewal is separate from repairing or deploying code.
