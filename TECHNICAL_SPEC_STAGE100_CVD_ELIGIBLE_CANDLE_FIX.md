# Stage 100 — CVD Eligible Candle Fix

The automatic CVD collector now decides whether data is current by comparing the
latest stored timestamp with the newest 30-minute candle that has actually
closed and cleared the configured grace period.

This replaces the former age comparison against a rounded request boundary,
which could incorrectly mark a database one full candle behind as current and
skip downloading an already available row.

No command, schema, database path, or collection cadence changed.
