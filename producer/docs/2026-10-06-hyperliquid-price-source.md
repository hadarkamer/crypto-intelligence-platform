# Experimental alert prices: Hyperliquid Perpetual TRADE 1m

All eight selected experimental formulas use Hyperliquid public perpetual trade prices for new decisions. CoinGlass remains the source of MaxPain target levels, liquidity and captured score metadata. The source quote used by each MaxPain formula is now the last closed Hyperliquid minute before the frozen Watch decision timestamp. SOL range24 and all BTC context features use the same venue.

This is an explicit prospective source variant. Binance historical performance is not inherited as validated Hyperliquid performance. TRADE candles do not establish execution parity with exchange MARK-triggered TP/SL orders. Notification-only behavior and formula math remain unchanged; no Testnet or Mainnet order path is modified.

## Cutover

Exact predecessor hashes are allowlisted. Pending unfilled observations and unsent legacy alert intents are cancelled. In-flight deliveries settle under the existing two-minute grace. Already OPEN/UNKNOWN observations retain their original prices, identifiers, management state and original price venue until closure. Source migration does not resend alerts or restore consumed MaxPain targets. Existing targets remain unverified until a complete absence and subsequent return. Legacy MaxPain observations still participate in target-proximity blocking.

## Shared source and durable history

A thread-safe shared cache validates all six instruments (BTC, ETH, SOL, HYPE, DOGE, XRP), requires continuous aligned minute bars and rejects source mixing, holes and revisions. One bounded HTTP attempt per missing-page request; HTTP429 uses a shared cooldown. Live candles are transient and cannot enter closed features/archive.

Startup migration `056_hyperliquid_perpetual_archive.sql` extends the existing immutable source-separated archive to BTC/ETH/SOL/XRP/DOGE. HYPE retains its existing route and history. The continuous worker preserves all old Binance comparison/legacy routes. No DDL is run by recurring work.

Deployment must apply migration056 via the existing explicitly authorized schema-admin startup mechanism. Use FORMULA_SCHEMA_APPLY=1 and FORMULA_SCHEMA_APPLY_ONLY=056_hyperliquid_perpetual_archive.sql for a targeted application, then disable schema application after verified completion. RESEARCH_PRICE_ARCHIVE_ENABLED must remain enabled. Missing configured archive schema fails closed.

## Historical evidence

The official rolling API retains only the latest5000 candles per interval; pagination does not extend the minute window. Older stored candles remain available. Research exports use an explicit cutoff, record per-coin coverage and preserve unresolved trades. A B20 score requires the existing minimum30 closed trades and concentration policy. Short-window results must not be described as full-history evidence.

## Validation

Offline tests cover source provenance/as-of quotes, shared request deduplication, retention/gaps, frozen predecessor positions, source migration idempotence, target lifecycle, no resend, and independent legacy cursors. PostgreSQL CI exercises immutable archival and idempotent constraint migration. The rollout must verify all8selected workers, legacy counts, new source labels, schema056 and continuous archive progress.
