-- Prospective Shadow authorization view: indexed pair expansion v1
--
-- Migration 013 authorized the two immutable event IDs owned by each sampler
-- slot through a LATERAL VALUES expansion.  That shape prevents PostgreSQL
-- from pushing an external event_id restriction into the existing UNIQUE
-- indexes on long_event_id and short_event_id.  Keep the exact view contract
-- and every fail-closed authorization predicate, but expose the two indexed
-- joins as explicit UNION ALL branches.  No rows or evidence are rewritten.

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
JOIN research_events event ON event.event_id = slot.long_event_id
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
        IS NULL
UNION ALL
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
JOIN research_events event ON event.event_id = slot.short_event_id
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
    'Only exact sampler-v4 silent DECISION_SAMPLE events with pair-bound, hashed decision-time feature evidence; expanded through indexed LONG/SHORT slot joins and never Telegram alerts.';
