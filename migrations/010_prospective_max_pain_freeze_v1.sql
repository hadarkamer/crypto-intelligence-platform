-- Prospective sampler v3: decision-frozen Max-Pain evidence.
--
-- Existing v1/v2 rows remain immutable and auditable, but cannot satisfy the
-- v3 feature-input contract. Every new v3 slot must carry the explicit
-- decision-time Max-Pain wrapper (EVALUABLE or UNEVALUABLE) inside the same
-- fingerprinted JSON payload as Price, OI and both CVD families.

ALTER TABLE research_prospective_anchor_slots
    DROP CONSTRAINT IF EXISTS research_anchor_slots_v3_max_pain_frozen;

ALTER TABLE research_prospective_anchor_slots
    ADD CONSTRAINT research_anchor_slots_v3_max_pain_frozen
    CHECK (
        sampler_version <> 'prospective-neutral-anchor-v3-max-pain-frozen'
        OR COALESCE((
            frozen_inputs ? 'max_pain'
            AND JSONB_TYPEOF(frozen_inputs->'max_pain') = 'object'
            AND JSONB_TYPEOF(frozen_inputs#>'{max_pain,features}') = 'object'
            AND frozen_inputs#>>'{max_pain,evaluation_status}'
                IN ('EVALUABLE', 'UNEVALUABLE')
        ), FALSE)
    );

ALTER TABLE research_prospective_anchor_attempts
    DROP CONSTRAINT IF EXISTS research_anchor_attempts_v3_max_pain_frozen;

ALTER TABLE research_prospective_anchor_attempts
    ADD CONSTRAINT research_anchor_attempts_v3_max_pain_frozen
    CHECK (
        sampler_version <> 'prospective-neutral-anchor-v3-max-pain-frozen'
        OR evaluation_status <> 'EVALUABLE'
        OR COALESCE((
            frozen_inputs ? 'max_pain'
            AND JSONB_TYPEOF(frozen_inputs->'max_pain') = 'object'
            AND JSONB_TYPEOF(frozen_inputs#>'{max_pain,features}') = 'object'
            AND frozen_inputs#>>'{max_pain,evaluation_status}'
                IN ('EVALUABLE', 'UNEVALUABLE')
        ), FALSE)
    );

COMMENT ON CONSTRAINT research_anchor_slots_v3_max_pain_frozen
    ON research_prospective_anchor_slots IS
    'Sampler v3 freezes Max-Pain result/provenance at the actual prospective decision time.';
