Stage 16 v2 — PostgreSQL Schema Fix

Fixes the failed deploy:
- PostgreSQL no longer receives SQLite syntax.
- alert_history.id uses BIGSERIAL PRIMARY KEY.
- alert_history.created_at uses TIMESTAMPTZ.
- priority uses DOUBLE PRECISION.
- fingerprint remains UNIQUE.

All Stage 16 functionality remains:
- SKHY filtering
- readable /alert_check cards
- score component breakdown
- automatic Watch commands

Test after deploy:
1. /watch_status
2. /collect
3. /alert_check
4. /watch_now
