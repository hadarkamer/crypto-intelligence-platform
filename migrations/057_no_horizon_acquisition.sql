-- Explicit frozen requests, one durable source anchor, and immutable transport
-- evidence. No source collection, automatic declaration, or legacy writes.
CREATE TABLE IF NOT EXISTS research_no_horizon_acquisition_schema (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), version TEXT NOT NULL
);
INSERT INTO research_no_horizon_acquisition_schema VALUES(TRUE,'no-horizon-cohort-acquisition-store-v1')
    ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS research_no_horizon_acquisition_requests (
    request_id TEXT PRIMARY KEY CHECK(request_id ~ '^[0-9a-f]{64}$'),
    request_key TEXT UNIQUE,
    identity_json TEXT NOT NULL,
    declaration_json TEXT NOT NULL,
    implementation_sha256 TEXT NOT NULL CHECK(implementation_sha256 ~ '^[0-9a-f]{64}$'),
    not_before_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'WAITING' CHECK(status IN ('WAITING','FETCHING','READY','ADMITTED','BLOCKED')),
    worker_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token>=0),
    lease_until TIMESTAMPTZ,
    last_claimed_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    retained_proof_bytes BIGINT NOT NULL DEFAULT 0 CHECK(retained_proof_bytes BETWEEN 0 AND 536870912),
    executor_plan_id TEXT REFERENCES research_no_horizon_plans(plan_id),
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK((worker_id IS NULL)=(lease_until IS NULL)),
    CHECK((status='ADMITTED')=(executor_plan_id IS NOT NULL)),
    CHECK(status NOT IN ('ADMITTED','BLOCKED') OR worker_id IS NULL)
);
CREATE INDEX IF NOT EXISTS idx_no_horizon_acquisition_due
    ON research_no_horizon_acquisition_requests(last_claimed_at_utc,request_id)
    WHERE status NOT IN ('ADMITTED','BLOCKED');
CREATE TABLE IF NOT EXISTS research_no_horizon_acquisition_anchors (
    request_id TEXT PRIMARY KEY REFERENCES research_no_horizon_acquisition_requests,
    proof_json TEXT NOT NULL,
    anchor_json TEXT,
    validation_error TEXT,
    CHECK((anchor_json IS NULL)=(validation_error IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS research_no_horizon_acquisition_leaves (
    request_id TEXT NOT NULL REFERENCES research_no_horizon_acquisition_requests,
    leaf_ordinal INTEGER NOT NULL CHECK(leaf_ordinal>=0),
    task_json TEXT NOT NULL,
    proof_json TEXT NOT NULL,
    PRIMARY KEY(request_id,leaf_ordinal)
);
CREATE TABLE IF NOT EXISTS research_no_horizon_acquisition_receipts (
    request_id TEXT PRIMARY KEY REFERENCES research_no_horizon_acquisition_requests,
    payload_json TEXT NOT NULL
);
DO $$ DECLARE name TEXT;
BEGIN
    FOREACH name IN ARRAY ARRAY['research_no_horizon_acquisition_schema',
        'research_no_horizon_acquisition_anchors','research_no_horizon_acquisition_leaves',
        'research_no_horizon_acquisition_receipts']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS acquisition_immutable ON %I',name);
        EXECUTE format('CREATE TRIGGER acquisition_immutable BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION research_no_horizon_immutable_v1()',name);
    END LOOP;
END $$;
CREATE OR REPLACE FUNCTION research_no_horizon_acquisition_request_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'immutable no-horizon acquisition request';
    END IF;
    IF (NEW.request_id,NEW.request_key,NEW.identity_json,NEW.declaration_json,
        NEW.implementation_sha256,NEW.not_before_utc,NEW.created_at_utc) IS DISTINCT FROM
       (OLD.request_id,OLD.request_key,OLD.identity_json,OLD.declaration_json,
        OLD.implementation_sha256,OLD.not_before_utc,OLD.created_at_utc)
       OR OLD.status IN ('ADMITTED','BLOCKED')
       OR NEW.fencing_token<OLD.fencing_token
       OR NEW.retained_proof_bytes<OLD.retained_proof_bytes
       OR (OLD.status='READY' AND NEW.status NOT IN ('READY','ADMITTED','BLOCKED'))
       OR (OLD.status='FETCHING' AND NEW.status='WAITING') THEN
        RAISE EXCEPTION 'immutable acquisition identity, terminal state, or progress';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS acquisition_request_guard ON research_no_horizon_acquisition_requests;
CREATE TRIGGER acquisition_request_guard BEFORE UPDATE OR DELETE ON research_no_horizon_acquisition_requests
    FOR EACH ROW EXECUTE FUNCTION research_no_horizon_acquisition_request_guard_v1();
CREATE OR REPLACE FUNCTION research_no_horizon_acquisition_insert_guard_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE phase TEXT;
BEGIN
    SELECT status INTO phase FROM research_no_horizon_acquisition_requests WHERE request_id=NEW.request_id;
    IF phase IS NULL OR phase IN ('ADMITTED','BLOCKED')
       OR (TG_TABLE_NAME='research_no_horizon_acquisition_anchors' AND phase<>'WAITING')
       OR (TG_TABLE_NAME='research_no_horizon_acquisition_leaves' AND phase<>'FETCHING') THEN
        RAISE EXCEPTION 'immutable acquisition evidence population';
    END IF;
    RETURN NEW;
END $$;
DO $$ DECLARE name TEXT;
BEGIN
    FOREACH name IN ARRAY ARRAY['research_no_horizon_acquisition_anchors',
        'research_no_horizon_acquisition_leaves','research_no_horizon_acquisition_receipts']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS acquisition_insert_guard ON %I',name);
        EXECUTE format('CREATE TRIGGER acquisition_insert_guard BEFORE INSERT ON %I '
            'FOR EACH ROW EXECUTE FUNCTION research_no_horizon_acquisition_insert_guard_v1()',name);
    END LOOP;
END $$;
