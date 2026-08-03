# Stage 95 — Database Concurrency Fix

## Purpose
Prevent PostgreSQL deadlocks during Render rolling deploys and concurrent Price+OI / CVD collection.

## Changes
- Existing schemas are inspected before any DDL is executed.
- `ALTER TABLE` runs only for columns that are actually missing.
- Existing production tables skip repeated `CREATE TABLE/INDEX` work on every deploy.
- Cross-process PostgreSQL advisory locks prevent old/new Render instances from running the same OI or Flow collector simultaneously.
- Existing in-process asyncio locks and deadlock retry behavior remain unchanged.

## Unchanged
No score, Max Pain, OI, CVD, alert, command, display, or collection-frequency formula was changed.
