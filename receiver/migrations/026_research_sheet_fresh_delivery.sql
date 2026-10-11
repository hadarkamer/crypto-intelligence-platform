-- Source time remains separate from queue creation/update/delivery timestamps.
ALTER TABLE research_sheet_upsert_outbox
    ADD COLUMN IF NOT EXISTS source_time_utc TIMESTAMPTZ;

-- Legacy JSON may contain malformed or timezone-less values. Reject these
-- rather than inheriting the database timezone or aborting the migration.
CREATE OR REPLACE FUNCTION research_sheet_source_timestamp(value TEXT)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    IF value !~ '^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$' THEN
        RETURN NULL;
    END IF;
    RETURN value::TIMESTAMPTZ;
EXCEPTION WHEN data_exception THEN
    RETURN NULL;
END;
$$;

-- Payload metadata lets the old staging SQL keep accepting source rows while
-- the new sender is being deployed. Only the visible row needs this metadata.
CREATE OR REPLACE FUNCTION research_sheet_capture_source_timestamp()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.source_time_utc := research_sheet_source_timestamp(CASE NEW.sheet_name
        WHEN 'Snapshots' THEN NEW.payload->'row'->>'timestamp_utc'
        WHEN 'Telegram_Events' THEN NEW.payload->'row'->>'timestamp_utc'
        WHEN 'Episodes' THEN NEW.payload->'row'->>'opened_at_utc'
        WHEN 'Formula_Results' THEN NEW.payload->'row'->>'last_evaluated_at'
        WHEN 'תצוגת לייב' THEN NEW.payload->>'source_time_utc'
    END);
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS research_sheet_capture_source_timestamp ON research_sheet_upsert_outbox;
CREATE TRIGGER research_sheet_capture_source_timestamp
    BEFORE INSERT OR UPDATE OF payload ON research_sheet_upsert_outbox
    FOR EACH ROW EXECUTE FUNCTION research_sheet_capture_source_timestamp();

WITH sources AS MATERIALIZED (
    SELECT sheet_name,row_key,research_sheet_source_timestamp(CASE sheet_name
        WHEN 'Snapshots' THEN payload->'row'->>'timestamp_utc'
        WHEN 'Telegram_Events' THEN payload->'row'->>'timestamp_utc'
        WHEN 'Episodes' THEN payload->'row'->>'opened_at_utc'
        WHEN 'Formula_Results' THEN payload->'row'->>'last_evaluated_at'
        WHEN 'תצוגת לייב' THEN payload->>'source_time_utc'
    END) AS source_time
    FROM research_sheet_upsert_outbox WHERE source_time_utc IS NULL
)
UPDATE research_sheet_upsert_outbox AS queued SET source_time_utc=sources.source_time
FROM sources WHERE queued.sheet_name=sources.sheet_name AND queued.row_key=sources.row_key
  AND sources.source_time IS NOT NULL;

UPDATE research_sheet_upsert_outbox AS live SET source_time_utc = snapshot.source_time_utc
FROM research_sheet_upsert_outbox AS snapshot
WHERE live.sheet_name='תצוגת לייב' AND snapshot.sheet_name='Snapshots'
  AND live.row_key=snapshot.row_key AND live.source_time_utc IS NULL
  AND snapshot.source_time_utc IS NOT NULL
  AND live.payload->'row'->>'snapshot_id'=snapshot.payload->'row'->>'snapshot_id';

CREATE TABLE IF NOT EXISTS research_sheet_delivery_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    next_slot BIGINT NOT NULL DEFAULT 0 CHECK(next_slot>=0)
);
INSERT INTO research_sheet_delivery_cursor(singleton) VALUES (TRUE)
    ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_research_sheet_fresh_claim
    ON research_sheet_upsert_outbox
        (sheet_name,source_time_utc DESC,next_attempt_at_utc,created_at_utc,row_key)
    WHERE sync_status IN ('PENDING','RETRY','IN_FLIGHT') AND source_time_utc IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_research_sheet_backlog_claim
    ON research_sheet_upsert_outbox(sheet_name,next_attempt_at_utc,created_at_utc,row_key)
    WHERE sync_status IN ('PENDING','RETRY','IN_FLIGHT');
CREATE INDEX IF NOT EXISTS idx_research_sheet_fallback_claim
    ON research_sheet_upsert_outbox(next_attempt_at_utc,created_at_utc,row_key)
    WHERE sync_status IN ('PENDING','RETRY','IN_FLIGHT');
