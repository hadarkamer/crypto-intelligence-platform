# Testnet entry and budget recovery

The deployed stream repeatedly spent background REST weight setting up entries
which the durable emergency circuit would reject. Never-submitted flat candidate
buckets also received duplicate normal and emergency observations. Those reads
could exhaust background admission and make the safety supervisor unavailable.

Changes:

- Read the durable circuit before fresh entry setup. This is a scheduling hint;
  the final database fence still runs and still blocks every real incident,
  including `CLOSED_VERIFIED` until explicit archival. JSON null is not an incident.
- Skip duplicate normal maintenance of never-submitted flat candidates. Their
  entry cycle still obtains current evidence, inventory, capacity and ownership.
- On a healthy reconciled notification feed, the emergency supervisor omits aged
  idle reads for never-submitted candidates. Pending/attempted orders, dirty or
  disconnected feeds, owned exposure and incomplete evidence retain observation.
- Record the exact alert symbol, direction and original entry/STOP/TP in intake
  logs, linked to the card ID. No credential or message body is logged.
- Expose startup bucket/role/incident and bounded-trial epoch/deadline diagnostics.

Explicit recovery uses `HL_TESTNET_STARTUP_RECOVERY=verified_closed_incidents_v1`.
Both persistent entry flags must be `false`. Before execution workers start, the
existing `release_closed_incident` path independently refreshes closure proof,
checks two matching current inventories and unresolved requests, then archives
the exact incident using revision/digest fences. Active or unproved incidents
are retained. This operation never submits an order or changes entry flags.
Clear the recovery setting before separately re-enabling the tested entry flags.

No strategy prices, source expiry, risk, exchange weights, admission ceilings,
account routing, unknown-outcome fences or Mainnet support were changed.

Validation: offline runtime discovery and executor/forwarder tests, plus the
existing PostgreSQL CI workflow for real JSON-null, circuit, incident archival,
partial-fill, restart and concurrent-dispatch regressions. Deployment must wait
for PostgreSQL CI and retain disabled entries until archival is verified.
