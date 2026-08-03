# Stage 94 — Database Schema Locking Fix

## Scope

This stage changes database lifecycle behavior only. It does not change Max Pain, scoring, alerts, Watch, Price+OI formulas, CVD formulas, collection intervals, or Telegram display.

## Changes

1. Every schema initializer is idempotent in memory and runs DDL only once per process.
2. All PostgreSQL CREATE/ALTER operations use one shared advisory lock, preventing old/new Render instances from migrating concurrently during deploy overlap.
3. Core, historical Price+OI, live Price+OI, and Futures/Spot CVD schemas are fully initialized before collectors start.
4. Existing compatibility calls to `init_db()` remain safe but become no-ops after startup.
5. Snapshot and CVD writes retry up to two times after a PostgreSQL deadlock with short bounded backoff.

## Expected outcome

Routine collectors perform SELECT/INSERT/UPDATE only. AccessExclusiveLock requests are confined to startup, eliminating the observed DDL-versus-write deadlocks.
