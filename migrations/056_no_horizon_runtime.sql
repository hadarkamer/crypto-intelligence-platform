-- Additive executor for complete, frozen global research cohorts. No source,
-- legacy evidence, alert, or trading tables are written by this migration.
CREATE TABLE IF NOT EXISTS research_no_horizon_schema (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    version TEXT NOT NULL
);
INSERT INTO research_no_horizon_schema(singleton,version)
VALUES(TRUE,'no-horizon-postgres-cohort-store-v1') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS research_no_horizon_plans (
    plan_id TEXT PRIMARY KEY CHECK(plan_id ~ '^[0-9a-f]{64}$'),
    prepared_plan_id TEXT NOT NULL CHECK(prepared_plan_id ~ '^[0-9a-f]{64}$'),
    cohort_key TEXT UNIQUE,
    implementation_sha256 TEXT NOT NULL CHECK(implementation_sha256 ~ '^[0-9a-f]{64}$'),
    backend_identity_json TEXT NOT NULL,
    identity_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    cutoff_utc TIMESTAMPTZ NOT NULL,
    source_candles INTEGER NOT NULL CHECK(source_candles BETWEEN 0 AND 44640),
    scope_count INTEGER NOT NULL CHECK(scope_count BETWEEN 1 AND 64),
    admission_sealed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS research_no_horizon_candles (
    plan_id TEXT NOT NULL REFERENCES research_no_horizon_plans,
    open_time_utc TIMESTAMPTZ NOT NULL,
    candle_json TEXT NOT NULL,
    PRIMARY KEY(plan_id,open_time_utc)
);
CREATE TABLE IF NOT EXISTS research_no_horizon_scopes (
    plan_id TEXT NOT NULL REFERENCES research_no_horizon_plans,
    scope_ordinal INTEGER NOT NULL CHECK(scope_ordinal BETWEEN 0 AND 63),
    scope_plan_json TEXT NOT NULL,
    total_entries INTEGER NOT NULL CHECK(total_entries BETWEEN 0 AND 100000),
    input_error TEXT,
    status TEXT NOT NULL CHECK(status IN ('PENDING','RUNNING','COMPLETE','BLOCKED','INPUT_BLOCKED')),
    next_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(next_ordinal>=0 AND next_ordinal<=total_entries),
    candle_evaluations BIGINT NOT NULL DEFAULT 0 CHECK(candle_evaluations>=0),
    worker_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token>=0),
    lease_until TIMESTAMPTZ,
    -- New plans join the same age-ordered queue as resumed work; they cannot
    -- perpetually jump ahead of existing scopes by retaining a NULL ticket.
    last_claimed_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(plan_id,scope_ordinal),
    CHECK((status='RUNNING')=(worker_id IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK(status='RUNNING' OR (worker_id IS NULL AND lease_until IS NULL)),
    CHECK((status='INPUT_BLOCKED')=(input_error IS NOT NULL)),
    CHECK(status NOT IN ('COMPLETE','BLOCKED') OR next_ordinal=total_entries)
);
CREATE INDEX IF NOT EXISTS idx_no_horizon_scope_queue
    ON research_no_horizon_scopes(last_claimed_at_utc NULLS FIRST,plan_id,scope_ordinal)
    WHERE status IN ('PENDING','RUNNING');
CREATE INDEX IF NOT EXISTS idx_no_horizon_plan_implementation
    ON research_no_horizon_plans(implementation_sha256,plan_id);
CREATE TABLE IF NOT EXISTS research_no_horizon_entries (
    plan_id TEXT NOT NULL,
    scope_ordinal INTEGER NOT NULL,
    entry_ordinal INTEGER NOT NULL CHECK(entry_ordinal>=0),
    entry_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY(plan_id,scope_ordinal,entry_ordinal),
    UNIQUE(plan_id,scope_ordinal,entry_id),
    FOREIGN KEY(plan_id,scope_ordinal) REFERENCES research_no_horizon_scopes
);
CREATE TABLE IF NOT EXISTS research_no_horizon_progress (
    plan_id TEXT NOT NULL,
    scope_ordinal INTEGER NOT NULL,
    entry_ordinal INTEGER NOT NULL,
    checkpoint_json TEXT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY(plan_id,scope_ordinal,entry_ordinal),
    FOREIGN KEY(plan_id,scope_ordinal,entry_ordinal) REFERENCES research_no_horizon_entries
);
CREATE TABLE IF NOT EXISTS research_no_horizon_receipts (
    plan_id TEXT NOT NULL,
    scope_ordinal INTEGER NOT NULL,
    receipt_sha256 TEXT NOT NULL CHECK(receipt_sha256 ~ '^[0-9a-f]{64}$'),
    payload_json TEXT NOT NULL,
    PRIMARY KEY(plan_id,scope_ordinal),
    FOREIGN KEY(plan_id,scope_ordinal) REFERENCES research_no_horizon_scopes
);
CREATE TABLE IF NOT EXISTS research_no_horizon_work_commits (
    plan_id TEXT NOT NULL,
    scope_ordinal INTEGER NOT NULL,
    fencing_token BIGINT NOT NULL CHECK(fencing_token>0),
    starting_evaluations BIGINT NOT NULL CHECK(starting_evaluations>=0),
    ending_evaluations BIGINT NOT NULL CHECK(ending_evaluations>=starting_evaluations),
    starting_ordinal INTEGER NOT NULL CHECK(starting_ordinal>=0),
    ending_ordinal INTEGER NOT NULL CHECK(ending_ordinal>=starting_ordinal),
    PRIMARY KEY(plan_id,scope_ordinal,fencing_token),
    FOREIGN KEY(plan_id,scope_ordinal) REFERENCES research_no_horizon_scopes
);

CREATE OR REPLACE FUNCTION research_no_horizon_immutable_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable no-horizon research input or receipt';
END $$;

DO $$ DECLARE name TEXT;
BEGIN
    FOREACH name IN ARRAY ARRAY['research_no_horizon_schema',
        'research_no_horizon_candles','research_no_horizon_entries',
        'research_no_horizon_receipts','research_no_horizon_work_commits']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS no_horizon_immutable ON %I',name);
        EXECUTE format('CREATE TRIGGER no_horizon_immutable BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION research_no_horizon_immutable_v1()',name);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION research_no_horizon_plan_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'immutable no-horizon plan';
    END IF;
    IF OLD.admission_sealed OR NEW.admission_sealed IS NOT TRUE
       OR (to_jsonb(NEW)-'admission_sealed') IS DISTINCT FROM (to_jsonb(OLD)-'admission_sealed') THEN
        RAISE EXCEPTION 'immutable no-horizon plan identity';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS no_horizon_immutable ON research_no_horizon_plans;
DROP TRIGGER IF EXISTS no_horizon_plan_guard ON research_no_horizon_plans;
CREATE TRIGGER no_horizon_plan_guard BEFORE UPDATE OR DELETE ON research_no_horizon_plans
    FOR EACH ROW EXECUTE FUNCTION research_no_horizon_plan_guard_v1();

CREATE OR REPLACE FUNCTION research_no_horizon_admission_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM research_no_horizon_plans p
        WHERE p.plan_id=NEW.plan_id AND p.admission_sealed=FALSE) THEN
        RAISE EXCEPTION 'immutable sealed no-horizon materialization';
    END IF;
    RETURN NEW;
END $$;
DO $$ DECLARE name TEXT;
BEGIN
    FOREACH name IN ARRAY ARRAY['research_no_horizon_candles','research_no_horizon_scopes',
        'research_no_horizon_entries','research_no_horizon_progress']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS no_horizon_admission_guard ON %I',name);
        EXECUTE format('CREATE TRIGGER no_horizon_admission_guard BEFORE INSERT ON %I '
            'FOR EACH ROW EXECUTE FUNCTION research_no_horizon_admission_guard_v1()',name);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION research_no_horizon_scope_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'immutable no-horizon scope';
    END IF;
    IF (NEW.plan_id,NEW.scope_ordinal,NEW.scope_plan_json,NEW.total_entries,NEW.input_error)
       IS DISTINCT FROM
       (OLD.plan_id,OLD.scope_ordinal,OLD.scope_plan_json,OLD.total_entries,OLD.input_error)
       OR OLD.status IN ('COMPLETE','BLOCKED','INPUT_BLOCKED') THEN
        RAISE EXCEPTION 'immutable no-horizon scope identity or terminal result';
    END IF;
    IF NEW.next_ordinal<OLD.next_ordinal OR NEW.candle_evaluations<OLD.candle_evaluations
       OR NEW.fencing_token<OLD.fencing_token THEN
        RAISE EXCEPTION 'no-horizon work cannot move backward';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS no_horizon_scope_guard ON research_no_horizon_scopes;
CREATE TRIGGER no_horizon_scope_guard BEFORE UPDATE OR DELETE ON research_no_horizon_scopes
    FOR EACH ROW EXECUTE FUNCTION research_no_horizon_scope_guard_v1();

CREATE OR REPLACE FUNCTION research_no_horizon_progress_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'immutable no-horizon progress identity';
    END IF;
    IF (NEW.plan_id,NEW.scope_ordinal,NEW.entry_ordinal) IS DISTINCT FROM
       (OLD.plan_id,OLD.scope_ordinal,OLD.entry_ordinal) OR OLD.processed
       OR EXISTS(SELECT 1 FROM research_no_horizon_scopes s
          WHERE s.plan_id=OLD.plan_id AND s.scope_ordinal=OLD.scope_ordinal
          AND s.status IN ('COMPLETE','BLOCKED','INPUT_BLOCKED')) THEN
        RAISE EXCEPTION 'immutable no-horizon completed checkpoint';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS no_horizon_progress_guard ON research_no_horizon_progress;
CREATE TRIGGER no_horizon_progress_guard BEFORE UPDATE OR DELETE ON research_no_horizon_progress
    FOR EACH ROW EXECUTE FUNCTION research_no_horizon_progress_guard_v1();
