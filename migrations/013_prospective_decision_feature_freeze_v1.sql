-- Prospective decision-feature freeze v1
--
-- Additive only: earlier sampler rows and Shadow checks remain immutable and
-- auditable.  No historical decision features are reconstructed or
-- backfilled.  Only sampler v4 rows can enter the prospective Shadow view,
-- and every such row owns the exact hashed feature bundle captured at its
-- decision time.

ALTER TABLE research_prospective_anchor_slots
    ADD COLUMN IF NOT EXISTS decision_feature_bundle JSONB;

ALTER TABLE research_prospective_anchor_slots
    ADD COLUMN IF NOT EXISTS feature_bundle_policy_version TEXT;

ALTER TABLE research_prospective_anchor_slots
    ADD COLUMN IF NOT EXISTS feature_bundle_sha256 CHAR(64);

ALTER TABLE research_prospective_anchor_attempts
    ADD COLUMN IF NOT EXISTS feature_bundle_policy_version TEXT;

ALTER TABLE research_prospective_anchor_attempts
    ADD COLUMN IF NOT EXISTS feature_bundle_sha256 CHAR(64);

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_decision_bundle_object'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_decision_bundle_object
            CHECK (
                decision_feature_bundle IS NULL
                OR JSONB_TYPEOF(decision_feature_bundle) = 'object'
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_feature_bundle_policy'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_feature_bundle_policy
            CHECK (
                feature_bundle_policy_version IS NULL
                OR BTRIM(feature_bundle_policy_version) <> ''
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_feature_bundle_hash'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_feature_bundle_hash
            CHECK (
                feature_bundle_sha256 IS NULL
                OR BTRIM(feature_bundle_sha256) ~ '^[0-9a-f]{64}$'
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_v4_decision_bundle'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_v4_decision_bundle
            CHECK (
                sampler_version <>
                    'prospective-neutral-anchor-v4-decision-features-frozen'
                OR COALESCE((
                    JSONB_TYPEOF(decision_feature_bundle) = 'object'
                    AND feature_bundle_policy_version =
                        'prospective-decision-feature-bundle-v1'
                    AND BTRIM(feature_bundle_sha256)
                        ~ '^[0-9a-f]{64}$'
                    AND NOT (frozen_inputs ? 'decision_feature_bundle')
                    AND JSONB_TYPEOF(frozen_inputs->'max_pain') = 'object'
                    AND JSONB_TYPEOF(
                        frozen_inputs#>'{max_pain,features}'
                    ) = 'object'
                    AND frozen_inputs#>>'{max_pain,evaluation_status}'
                        IN ('EVALUABLE', 'UNEVALUABLE')
                ), FALSE)
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_attempts'::regclass
           AND conname = 'research_anchor_attempts_feature_bundle_policy'
    ) THEN
        ALTER TABLE research_prospective_anchor_attempts
            ADD CONSTRAINT research_anchor_attempts_feature_bundle_policy
            CHECK (
                feature_bundle_policy_version IS NULL
                OR BTRIM(feature_bundle_policy_version) <> ''
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_attempts'::regclass
           AND conname = 'research_anchor_attempts_feature_bundle_hash'
    ) THEN
        ALTER TABLE research_prospective_anchor_attempts
            ADD CONSTRAINT research_anchor_attempts_feature_bundle_hash
            CHECK (
                feature_bundle_sha256 IS NULL
                OR BTRIM(feature_bundle_sha256)
                    ~ '^[0-9a-f]{64}$'
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_attempts'::regclass
           AND conname = 'research_anchor_attempts_v4_feature_bundle_ref'
    ) THEN
        ALTER TABLE research_prospective_anchor_attempts
            ADD CONSTRAINT research_anchor_attempts_v4_feature_bundle_ref
            CHECK (
                sampler_version <>
                    'prospective-neutral-anchor-v4-decision-features-frozen'
                OR evaluation_status <> 'EVALUABLE'
                OR COALESCE((
                    feature_bundle_policy_version =
                        'prospective-decision-feature-bundle-v1'
                    AND BTRIM(feature_bundle_sha256)
                        ~ '^[0-9a-f]{64}$'
                    AND JSONB_TYPEOF(frozen_inputs->'max_pain') = 'object'
                    AND JSONB_TYPEOF(
                        frozen_inputs#>'{max_pain,features}'
                    ) = 'object'
                    AND frozen_inputs#>>'{max_pain,evaluation_status}'
                        IN ('EVALUABLE', 'UNEVALUABLE')
                ), FALSE)
            );
    END IF;
END;
$migration$;

COMMENT ON COLUMN research_prospective_anchor_slots.decision_feature_bundle IS
    'Slot-only formula-visible input features frozen at decision time; never copied into event snapshots or reconstructed later.';

COMMENT ON COLUMN research_prospective_anchor_slots.feature_bundle_policy_version IS
    'Canonicalization and contents policy used to construct the slot-only decision feature bundle.';

COMMENT ON COLUMN research_prospective_anchor_slots.feature_bundle_sha256 IS
    'Canonical SHA-256 identity of the slot-only decision_feature_bundle.';

COMMENT ON COLUMN research_prospective_anchor_attempts.feature_bundle_policy_version IS
    'Bundle policy reference for an EVALUABLE sampler-v4 attempt; no full bundle is duplicated into the audit row.';

COMMENT ON COLUMN research_prospective_anchor_attempts.feature_bundle_sha256 IS
    'SHA-256 reference to the captured slot bundle; NULL is allowed for legacy or unevaluable attempts.';

COMMENT ON CONSTRAINT research_anchor_slots_v4_decision_bundle
    ON research_prospective_anchor_slots IS
    'Sampler v4 requires one hashed slot-only decision feature bundle under the exact current bundle policy.';

COMMENT ON CONSTRAINT research_anchor_attempts_v4_feature_bundle_ref
    ON research_prospective_anchor_attempts IS
    'Every EVALUABLE sampler-v4 audit attempt preserves the exact bundle policy and hash reference; failures are not fabricated.';

-- Migration 008 continues to bind each event's base frozen_inputs exactly to
-- the slot.  The feature bundle is intentionally too large for an event
-- snapshot, so v4 events carry only the small policy/hash reference verified
-- here against the slot-owned bundle identity.
CREATE OR REPLACE FUNCTION validate_prospective_anchor_v4_feature_refs()
RETURNS TRIGGER AS $$
DECLARE
    long_snapshot JSONB;
    short_snapshot JSONB;
BEGIN
    IF NEW.sampler_version <>
        'prospective-neutral-anchor-v4-decision-features-frozen'
    THEN
        RETURN NEW;
    END IF;

    SELECT engine_snapshot INTO STRICT long_snapshot
      FROM research_events
     WHERE event_id = NEW.long_event_id;

    SELECT engine_snapshot INTO STRICT short_snapshot
      FROM research_events
     WHERE event_id = NEW.short_event_id;

    IF long_snapshot#>>'{prospective_anchor,feature_bundle_policy_version}'
            IS DISTINCT FROM NEW.feature_bundle_policy_version
       OR short_snapshot#>>'{prospective_anchor,feature_bundle_policy_version}'
            IS DISTINCT FROM NEW.feature_bundle_policy_version
       OR long_snapshot#>>'{prospective_anchor,feature_bundle_sha256}'
            IS DISTINCT FROM BTRIM(NEW.feature_bundle_sha256)
       OR short_snapshot#>>'{prospective_anchor,feature_bundle_sha256}'
            IS DISTINCT FROM BTRIM(NEW.feature_bundle_sha256)
       OR long_snapshot#>'{prospective_anchor,decision_feature_bundle}'
            IS NOT NULL
       OR short_snapshot#>'{prospective_anchor,decision_feature_bundle}'
            IS NOT NULL
       OR EXISTS (
            SELECT 1
              FROM research_events event
             WHERE event.event_id IN (NEW.long_event_id, NEW.short_event_id)
               AND (
                    event.source_side IS DISTINCT FROM 'RAW_NEUTRAL'
                    OR event.timeframe IS DISTINCT FROM '30m'
                    OR event.strategy_version IS DISTINCT FROM
                        'formula-prospective-neutral-v4'
               )
       )
    THEN
        RAISE EXCEPTION
            'invalid sampler-v4 decision feature references for slot % at %',
            NEW.symbol, NEW.source_candle_open_utc;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_prospective_anchor_v4_feature_refs
    ON research_prospective_anchor_slots;

CREATE TRIGGER trg_validate_prospective_anchor_v4_feature_refs
BEFORE INSERT OR UPDATE ON research_prospective_anchor_slots
FOR EACH ROW EXECUTE FUNCTION validate_prospective_anchor_v4_feature_refs();

COMMENT ON FUNCTION validate_prospective_anchor_v4_feature_refs() IS
    'Binds each sampler-v4 silent event policy/hash reference to the slot-only feature bundle without duplicating the bundle.';

-- Shadow-check evidence is backward compatible.  Existing rows receive the
-- explicit legacy marker and FALSE verification flag; only checks written
-- under the current frozen-evidence policy must carry all authoritative IDs.
ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS evidence_policy_version TEXT NOT NULL
        DEFAULT 'legacy-unverified';

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS prospective_anchor_slot_id BIGINT;

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS prospective_input_fingerprint CHAR(64);

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS feature_bundle_sha256 CHAR(64);

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS authoritative_verified BOOLEAN NOT NULL
        DEFAULT FALSE;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_formula_shadow_checks'::regclass
           AND conname = 'research_shadow_checks_anchor_slot_fk'
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_shadow_checks_anchor_slot_fk
            FOREIGN KEY (prospective_anchor_slot_id)
            REFERENCES research_prospective_anchor_slots(anchor_slot_id)
            ON DELETE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_formula_shadow_checks'::regclass
           AND conname = 'research_shadow_checks_evidence_policy_nonempty'
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_shadow_checks_evidence_policy_nonempty
            CHECK (BTRIM(evidence_policy_version) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_formula_shadow_checks'::regclass
           AND conname = 'research_shadow_checks_input_fingerprint_hash'
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_shadow_checks_input_fingerprint_hash
            CHECK (
                prospective_input_fingerprint IS NULL
                OR BTRIM(prospective_input_fingerprint)
                    ~ '^[0-9a-f]{64}$'
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_formula_shadow_checks'::regclass
           AND conname = 'research_shadow_checks_feature_bundle_hash'
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_shadow_checks_feature_bundle_hash
            CHECK (
                feature_bundle_sha256 IS NULL
                OR BTRIM(feature_bundle_sha256) ~ '^[0-9a-f]{64}$'
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_formula_shadow_checks'::regclass
           AND conname = 'research_shadow_checks_frozen_evidence_required'
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_shadow_checks_frozen_evidence_required
            CHECK (
                evidence_policy_version <>
                    'prospective-shadow-frozen-decision-features-v1'
                OR COALESCE((
                    prospective_anchor_slot_id IS NOT NULL
                    AND BTRIM(prospective_input_fingerprint)
                        ~ '^[0-9a-f]{64}$'
                    AND BTRIM(feature_bundle_sha256)
                        ~ '^[0-9a-f]{64}$'
                    AND authoritative_verified IS TRUE
                ), FALSE)
            );
    END IF;
END;
$migration$;

CREATE INDEX IF NOT EXISTS idx_shadow_checks_frozen_evidence
    ON research_formula_shadow_checks (
        prospective_anchor_slot_id,
        evidence_policy_version,
        authoritative_verified,
        formula_id,
        event_id
    );

COMMENT ON COLUMN research_formula_shadow_checks.evidence_policy_version IS
    'Evidence contract used by this check; legacy-unverified preserves pre-v4 rows without treating them as authoritative.';

COMMENT ON COLUMN research_formula_shadow_checks.prospective_anchor_slot_id IS
    'Authoritative sampler-v4 slot evaluated by this Shadow check; NULL for legacy checks.';

COMMENT ON COLUMN research_formula_shadow_checks.prospective_input_fingerprint IS
    'Verified identity of the slot base frozen_inputs used by this Shadow check.';

COMMENT ON COLUMN research_formula_shadow_checks.feature_bundle_sha256 IS
    'Verified identity of the slot-only decision feature bundle used by this Shadow check.';

COMMENT ON COLUMN research_formula_shadow_checks.authoritative_verified IS
    'TRUE only after slot, input fingerprint and decision feature bundle identities all pass fail-closed verification.';

COMMENT ON CONSTRAINT research_shadow_checks_frozen_evidence_required
    ON research_formula_shadow_checks IS
    'The current prospective Shadow policy cannot persist a check without complete, authoritative decision-time evidence.';

-- Prospective Shadow checks are decision-time evidence.  Keep both legacy and
-- v4 rows immutable after insertion so a later writer cannot manufacture or
-- rewrite readiness evidence.  Migration 005 is repeat-safe before this guard:
-- its legacy normalization UPDATE now selects only rows whose value changes.
DROP TRIGGER IF EXISTS trg_formula_shadow_checks_append_only
    ON research_formula_shadow_checks;

CREATE TRIGGER trg_formula_shadow_checks_append_only
BEFORE UPDATE OR DELETE ON research_formula_shadow_checks
FOR EACH ROW EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

DROP TRIGGER IF EXISTS trg_formula_shadow_checks_no_truncate
    ON research_formula_shadow_checks;

CREATE TRIGGER trg_formula_shadow_checks_no_truncate
BEFORE TRUNCATE ON research_formula_shadow_checks
FOR EACH STATEMENT EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

-- Fail closed at the database authorization boundary.  Earlier v1-v3 slots
-- stay in the archive but are intentionally absent from this view.
CREATE OR REPLACE VIEW research_prospective_shadow_events AS
SELECT
    slot.anchor_slot_id,
    slot.sampler_version,
    slot.coverage_policy_version,
    slot.coverage_snapshot,
    slot.source_candle_open_utc,
    slot.source_candle_close_utc,
    slot.base_eligible_at_utc,
    slot.expires_at_utc,
    slot.decision_time_utc,
    slot.input_fingerprint,
    event.event_id,
    event.alert_time_utc,
    event.symbol,
    event.direction,
    event.event_type,
    event.setup_key,
    event.event_fingerprint,
    event.current_price,
    event.engine_snapshot,
    slot.frozen_inputs,
    slot.source_timestamps,
    slot.source_provenance,
    slot.created_at_utc,
    slot.interval_minutes,
    slot.long_event_id,
    slot.short_event_id,
    slot.feature_bundle_policy_version,
    slot.feature_bundle_sha256,
    slot.decision_feature_bundle,
    event.source_side,
    event.timeframe,
    event.strategy_version,
    event.code_version
FROM research_prospective_anchor_slots slot
CROSS JOIN LATERAL (
    VALUES (slot.long_event_id), (slot.short_event_id)
) expected(event_id)
JOIN research_events event ON event.event_id = expected.event_id
WHERE slot.sampler_version =
        'prospective-neutral-anchor-v4-decision-features-frozen'
  AND slot.coverage_policy_version =
        'prospective-coverage-v3-completed-fully-validated-replay-run:no-dwell-first-touch-v6:historical-raw-opportunity-replay-v2-balanced-prior-session-width'
  AND slot.feature_bundle_policy_version =
        'prospective-decision-feature-bundle-v1'
  AND JSONB_TYPEOF(slot.decision_feature_bundle) = 'object'
  AND BTRIM(slot.feature_bundle_sha256) ~ '^[0-9a-f]{64}$'
  AND NOT (slot.frozen_inputs ? 'decision_feature_bundle')
  AND event.event_kind = 'DECISION_SAMPLE'
  AND event.event_type = 'PROSPECTIVE_NEUTRAL_30M'
  AND event.capture_stage = 'SILENT_NEUTRAL_ANCHOR'
  AND event.source_side = 'RAW_NEUTRAL'
  AND event.timeframe = '30m'
  AND event.strategy_version = 'formula-prospective-neutral-v4'
  AND event.delivery_status = 'NOT_APPLICABLE'
  AND event.engine_snapshot#>>'{prospective_anchor,sampler_version}'
        IS NOT DISTINCT FROM slot.sampler_version
  AND event.engine_snapshot#>>'{prospective_anchor,coverage_policy_version}'
        IS NOT DISTINCT FROM slot.coverage_policy_version
  AND event.engine_snapshot#>>'{prospective_anchor,input_fingerprint}'
        IS NOT DISTINCT FROM BTRIM(slot.input_fingerprint)
  AND event.engine_snapshot#>'{prospective_anchor,frozen_inputs}'
        IS NOT DISTINCT FROM slot.frozen_inputs
  AND event.engine_snapshot#>>'{prospective_anchor,feature_bundle_policy_version}'
        IS NOT DISTINCT FROM slot.feature_bundle_policy_version
  AND event.engine_snapshot#>>'{prospective_anchor,feature_bundle_sha256}'
        IS NOT DISTINCT FROM BTRIM(slot.feature_bundle_sha256)
  AND event.engine_snapshot#>'{prospective_anchor,decision_feature_bundle}'
        IS NULL;

COMMENT ON VIEW research_prospective_shadow_events IS
    'Only exact sampler-v4 silent DECISION_SAMPLE events with pair-bound, hashed decision-time feature evidence; never Telegram alerts.';
