-- Additive research-only validation. No historic registration is backdated.
CREATE TABLE IF NOT EXISTS research_ordered_validation_acceptance (
    policy_version TEXT PRIMARY KEY,
    binding_sha256 TEXT NOT NULL,
    policy JSONB NOT NULL CHECK (jsonb_typeof(policy)='object'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(binding_sha256)
);
CREATE TABLE IF NOT EXISTS research_ordered_validation_freezes (
    freeze_id TEXT PRIMARY KEY,
    scope_key TEXT NOT NULL UNIQUE,
    definition_sha256 TEXT NOT NULL,
    frozen_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    registration JSONB NOT NULL CHECK (jsonb_typeof(registration)='object')
);
CREATE TABLE IF NOT EXISTS research_ordered_validation_waves (
    freeze_id TEXT NOT NULL REFERENCES research_ordered_validation_freezes(freeze_id),
    btc_parent_movement_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('DISCOVERY','PROSPECTIVE')),
    parent_start_time_utc TIMESTAMPTZ NOT NULL,
    representative_event_ids JSONB NOT NULL CHECK(jsonb_typeof(representative_event_ids)='array'),
    representative_sha256 TEXT NOT NULL,
    representative_conflict BOOLEAN NOT NULL DEFAULT FALSE,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(freeze_id,btc_parent_movement_id)
);
CREATE TABLE IF NOT EXISTS research_ordered_validation_evaluations (
    freeze_id TEXT NOT NULL REFERENCES research_ordered_validation_freezes(freeze_id),
    evidence_sha256 TEXT NOT NULL,
    result JSONB NOT NULL CHECK(jsonb_typeof(result)='object'),
    evaluated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(freeze_id,evidence_sha256)
);
CREATE INDEX IF NOT EXISTS idx_ordered_validation_latest
    ON research_ordered_validation_evaluations(freeze_id,evaluated_at_utc DESC);
CREATE OR REPLACE FUNCTION research_ordered_validation_immutable_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Immutable ordered validation record; create a new version without rewriting prior evidence';
END $$;
DROP TRIGGER IF EXISTS ordered_validation_freeze_immutable ON research_ordered_validation_freezes;
CREATE TRIGGER ordered_validation_freeze_immutable BEFORE UPDATE OR DELETE ON research_ordered_validation_freezes
    FOR EACH ROW EXECUTE FUNCTION research_ordered_validation_immutable_v1();
DROP TRIGGER IF EXISTS ordered_validation_policy_immutable ON research_ordered_validation_acceptance;
CREATE TRIGGER ordered_validation_policy_immutable BEFORE UPDATE OR DELETE ON research_ordered_validation_acceptance
    FOR EACH ROW EXECUTE FUNCTION research_ordered_validation_immutable_v1();
DROP TRIGGER IF EXISTS ordered_validation_evaluation_immutable ON research_ordered_validation_evaluations;
CREATE TRIGGER ordered_validation_evaluation_immutable BEFORE UPDATE OR DELETE ON research_ordered_validation_evaluations
    FOR EACH ROW EXECUTE FUNCTION research_ordered_validation_immutable_v1();
CREATE OR REPLACE FUNCTION research_ordered_validation_wave_guard_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Ordered validation waves cannot be deleted';
    END IF;
    IF ROW(NEW.freeze_id,NEW.btc_parent_movement_id,NEW.phase,NEW.parent_start_time_utc,
           NEW.representative_event_ids,NEW.representative_sha256)
       IS DISTINCT FROM
       ROW(OLD.freeze_id,OLD.btc_parent_movement_id,OLD.phase,OLD.parent_start_time_utc,
           OLD.representative_event_ids,OLD.representative_sha256)
       OR (OLD.representative_conflict AND NOT NEW.representative_conflict) THEN
        RAISE EXCEPTION 'Ordered validation entries and conflict history are immutable';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS ordered_validation_wave_guard ON research_ordered_validation_waves;
CREATE TRIGGER ordered_validation_wave_guard BEFORE UPDATE OR DELETE ON research_ordered_validation_waves
    FOR EACH ROW EXECUTE FUNCTION research_ordered_validation_wave_guard_v1();
COMMENT ON TABLE research_ordered_validation_acceptance IS
    'Empty until a documented exactly bound ordered-v7/full-common-window policy exists; no legacy numerical gates copied.';
COMMENT ON TABLE research_ordered_validation_freezes IS
    'Real DB clock freeze. Whole BTC waves beginning on/before freeze stay DISCOVERY even when outcomes arrive later.';
