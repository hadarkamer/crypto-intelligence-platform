-- Prospective neutral decision anchors v1
--
-- Additive, research-only storage. A successful 30-minute source slot is one
-- atomic LONG/SHORT pair of silent DECISION_SAMPLE events. It is never an
-- ALERT and is never a Telegram-delivery candidate. Missing, late, stale, or
-- unofficial inputs remain explicit UNEVALUABLE attempts.

CREATE TABLE IF NOT EXISTS research_prospective_anchor_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    sampler_version TEXT NOT NULL CHECK (BTRIM(sampler_version) <> ''),
    coverage_policy_version TEXT NOT NULL CHECK (
        BTRIM(coverage_policy_version) <> ''
    ),
    coverage_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(coverage_snapshot) = 'object'
    ),
    symbol TEXT NOT NULL CHECK (BTRIM(symbol) <> ''),
    interval_minutes INTEGER NOT NULL CHECK (interval_minutes = 30),
    source_candle_open_utc TIMESTAMPTZ NOT NULL,
    source_candle_close_utc TIMESTAMPTZ NOT NULL,
    base_eligible_at_utc TIMESTAMPTZ NOT NULL,
    expires_at_utc TIMESTAMPTZ NOT NULL,
    decision_time_utc TIMESTAMPTZ,
    checked_at_utc TIMESTAMPTZ NOT NULL,
    evaluation_status TEXT NOT NULL CHECK (
        evaluation_status IN ('EVALUABLE', 'UNEVALUABLE', 'COVERAGE_EXCLUDED')
    ),
    evaluation_reason TEXT NOT NULL CHECK (BTRIM(evaluation_reason) <> ''),
    missing_sources JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(missing_sources) = 'array'
    ),
    source_timestamps JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(source_timestamps) = 'object'
    ),
    source_provenance JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(source_provenance) = 'object'
    ),
    frozen_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(input_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    attempt_fingerprint CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(attempt_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        source_candle_close_utc =
            source_candle_open_utc + INTERVAL '30 minutes'
    ),
    CHECK (
        base_eligible_at_utc = source_candle_close_utc + INTERVAL '2 minutes'
    ),
    CHECK (expires_at_utc = base_eligible_at_utc + INTERVAL '30 minutes'),
    CHECK (
        (evaluation_status = 'EVALUABLE'
         AND decision_time_utc IS NOT NULL
         AND decision_time_utc = checked_at_utc
         AND decision_time_utc >= base_eligible_at_utc
         AND decision_time_utc < expires_at_utc
         AND missing_sources = '[]'::jsonb
         AND source_timestamps ?& ARRAY[
             'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
         ]
         AND source_provenance ?& ARRAY[
             'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
         ])
        OR
        (evaluation_status <> 'EVALUABLE' AND decision_time_utc IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_prospective_anchor_attempt_status_time
    ON research_prospective_anchor_attempts (
        evaluation_status, source_candle_open_utc DESC, symbol
    );

COMMENT ON TABLE research_prospective_anchor_attempts IS
    'Prospective sampler audit. Identical repeated failures deduplicate by attempt_fingerprint; missing inputs are never substituted.';

-- This is the atomic captured-slot record. Callers must insert the two event
-- rows and this row in one transaction; the foreign keys and validation
-- trigger make a one-sided or mismatched pair impossible to publish here.
CREATE TABLE IF NOT EXISTS research_prospective_anchor_slots (
    anchor_slot_id BIGSERIAL PRIMARY KEY,
    sampler_version TEXT NOT NULL CHECK (BTRIM(sampler_version) <> ''),
    coverage_policy_version TEXT NOT NULL CHECK (
        BTRIM(coverage_policy_version) <> ''
    ),
    coverage_snapshot JSONB NOT NULL CHECK (
        JSONB_TYPEOF(coverage_snapshot) = 'object'
    ),
    symbol TEXT NOT NULL CHECK (BTRIM(symbol) <> ''),
    interval_minutes INTEGER NOT NULL CHECK (interval_minutes = 30),
    source_candle_open_utc TIMESTAMPTZ NOT NULL,
    source_candle_close_utc TIMESTAMPTZ NOT NULL,
    base_eligible_at_utc TIMESTAMPTZ NOT NULL,
    expires_at_utc TIMESTAMPTZ NOT NULL,
    decision_time_utc TIMESTAMPTZ NOT NULL,
    input_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(input_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    source_timestamps JSONB NOT NULL CHECK (
        JSONB_TYPEOF(source_timestamps) = 'object'
    ),
    source_provenance JSONB NOT NULL CHECK (
        JSONB_TYPEOF(source_provenance) = 'object'
    ),
    frozen_inputs JSONB NOT NULL,
    long_event_id BIGINT NOT NULL UNIQUE
        REFERENCES research_events(event_id) ON DELETE RESTRICT,
    short_event_id BIGINT NOT NULL UNIQUE
        REFERENCES research_events(event_id) ON DELETE RESTRICT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (sampler_version, symbol, source_candle_open_utc),
    CHECK (long_event_id <> short_event_id),
    CHECK (
        source_candle_close_utc =
            source_candle_open_utc + INTERVAL '30 minutes'
    ),
    CHECK (
        base_eligible_at_utc = source_candle_close_utc + INTERVAL '2 minutes'
    ),
    CHECK (expires_at_utc = base_eligible_at_utc + INTERVAL '30 minutes'),
    CHECK (
        decision_time_utc >= base_eligible_at_utc
        AND decision_time_utc < expires_at_utc
    ),
    CHECK (
        source_timestamps ?& ARRAY[
            'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
        ]
    ),
    CHECK (
        source_provenance ?& ARRAY[
            'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
        ]
    )
);

-- A pre-release candidate may have created the two relations before frozen
-- inputs became first-class columns.  Empty candidate relations can be
-- upgraded in place; populated relations fail closed instead of fabricating
-- historical input values.
ALTER TABLE research_prospective_anchor_attempts
    ADD COLUMN IF NOT EXISTS frozen_inputs JSONB;
ALTER TABLE research_prospective_anchor_slots
    ADD COLUMN IF NOT EXISTS frozen_inputs JSONB;
ALTER TABLE research_prospective_anchor_attempts
    ALTER COLUMN frozen_inputs SET DEFAULT '{}'::jsonb,
    ALTER COLUMN frozen_inputs SET NOT NULL;
ALTER TABLE research_prospective_anchor_slots
    ALTER COLUMN frozen_inputs SET NOT NULL;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_attempts'::regclass
           AND conname = 'research_anchor_attempts_frozen_inputs_object'
    ) THEN
        ALTER TABLE research_prospective_anchor_attempts
            ADD CONSTRAINT research_anchor_attempts_frozen_inputs_object
            CHECK (JSONB_TYPEOF(frozen_inputs) = 'object');
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_attempts'::regclass
           AND conname = 'research_anchor_attempts_evaluable_frozen_inputs'
    ) THEN
        ALTER TABLE research_prospective_anchor_attempts
            ADD CONSTRAINT research_anchor_attempts_evaluable_frozen_inputs
            CHECK (
                evaluation_status <> 'EVALUABLE'
                OR frozen_inputs ?& ARRAY[
                    'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
                ]
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_frozen_inputs_object'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_frozen_inputs_object
            CHECK (JSONB_TYPEOF(frozen_inputs) = 'object');
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'research_prospective_anchor_slots'::regclass
           AND conname = 'research_anchor_slots_frozen_inputs_complete'
    ) THEN
        ALTER TABLE research_prospective_anchor_slots
            ADD CONSTRAINT research_anchor_slots_frozen_inputs_complete
            CHECK (
                frozen_inputs ?& ARRAY[
                    'official_price', 'price_oi', 'futures_cvd', 'spot_cvd'
                ]
            );
    END IF;
END;
$migration$;

CREATE INDEX IF NOT EXISTS idx_prospective_anchor_slot_time
    ON research_prospective_anchor_slots (
        source_candle_open_utc DESC, symbol
    );

COMMENT ON TABLE research_prospective_anchor_slots IS
    'Atomic silent LONG/SHORT DECISION_SAMPLE pairs. Insert both events and the slot in one database transaction.';

CREATE OR REPLACE FUNCTION validate_prospective_anchor_pair()
RETURNS TRIGGER AS $$
DECLARE
    long_event research_events%ROWTYPE;
    short_event research_events%ROWTYPE;
    long_frozen_price_invalid BOOLEAN;
    short_frozen_price_invalid BOOLEAN;
    official_price_provenance_invalid BOOLEAN;
BEGIN
    SELECT * INTO STRICT long_event
      FROM research_events
     WHERE event_id = NEW.long_event_id;
    SELECT * INTO STRICT short_event
      FROM research_events
     WHERE event_id = NEW.short_event_id;

    long_frozen_price_invalid := CASE
        WHEN JSONB_TYPEOF(long_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs,official_price,price}'
        ) = 'number'
        THEN (long_event.engine_snapshot #>>
            '{prospective_anchor,frozen_inputs,official_price,price}'
        )::double precision IS DISTINCT FROM long_event.current_price
        ELSE TRUE
    END;
    short_frozen_price_invalid := CASE
        WHEN JSONB_TYPEOF(short_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs,official_price,price}'
        ) = 'number'
        THEN (short_event.engine_snapshot #>>
            '{prospective_anchor,frozen_inputs,official_price,price}'
        )::double precision IS DISTINCT FROM short_event.current_price
        ELSE TRUE
    END;
    official_price_provenance_invalid := CASE
        WHEN NEW.symbol = 'HYPE' THEN
            LOWER(NEW.source_provenance #>>
                '{official_price,source}') IS DISTINCT FROM
                'hyperliquid_spot_@107'
            OR UPPER(NEW.source_provenance #>>
                '{official_price,price_exchange}') IS DISTINCT FROM
                'HYPERLIQUID'
            OR UPPER(NEW.source_provenance #>>
                '{official_price,price_market}') IS DISTINCT FROM 'SPOT'
            OR REGEXP_REPLACE(
                UPPER(COALESCE(NEW.source_provenance #>>
                    '{official_price,price_pair}', '')),
                '[^A-Z0-9]', '', 'g'
            ) IS DISTINCT FROM 'HYPEUSDT'
            OR UPPER(NEW.source_provenance #>>
                '{official_price,price_instrument_id}') IS DISTINCT FROM
                '@107'
            OR LOWER(NEW.source_provenance #>>
                '{official_price,price_timeframe}') IS DISTINCT FROM '1m'
        ELSE
            LOWER(NEW.source_provenance #>>
                '{official_price,source}') IS DISTINCT FROM 'binance_spot'
            OR UPPER(NEW.source_provenance #>>
                '{official_price,price_exchange}') IS DISTINCT FROM
                'BINANCE'
            OR UPPER(NEW.source_provenance #>>
                '{official_price,price_market}') IS DISTINCT FROM 'SPOT'
            OR REGEXP_REPLACE(
                UPPER(COALESCE(NEW.source_provenance #>>
                    '{official_price,price_pair}', '')),
                '[^A-Z0-9]', '', 'g'
            ) IS DISTINCT FROM (NEW.symbol || 'USDT')
            OR LOWER(NEW.source_provenance #>>
                '{official_price,price_timeframe}') IS DISTINCT FROM '1m'
    END;

    IF long_event.event_kind IS DISTINCT FROM 'DECISION_SAMPLE'
       OR short_event.event_kind IS DISTINCT FROM 'DECISION_SAMPLE'
       OR long_event.event_type IS DISTINCT FROM 'PROSPECTIVE_NEUTRAL_30M'
       OR short_event.event_type IS DISTINCT FROM 'PROSPECTIVE_NEUTRAL_30M'
       OR long_event.delivery_status IS DISTINCT FROM 'NOT_APPLICABLE'
       OR short_event.delivery_status IS DISTINCT FROM 'NOT_APPLICABLE'
       OR long_event.capture_stage IS DISTINCT FROM 'SILENT_NEUTRAL_ANCHOR'
       OR short_event.capture_stage IS DISTINCT FROM 'SILENT_NEUTRAL_ANCHOR'
       OR long_event.direction IS DISTINCT FROM 'LONG'
       OR short_event.direction IS DISTINCT FROM 'SHORT'
       OR long_event.symbol IS DISTINCT FROM NEW.symbol
       OR short_event.symbol IS DISTINCT FROM NEW.symbol
       OR long_event.alert_time_utc IS DISTINCT FROM NEW.decision_time_utc
       OR short_event.alert_time_utc IS DISTINCT FROM NEW.decision_time_utc
       OR long_event.current_price IS NULL
       OR short_event.current_price IS NULL
       OR long_event.current_price <= 0
       OR short_event.current_price <= 0
       OR long_event.current_price IS DISTINCT FROM short_event.current_price
       OR BTRIM(long_event.event_fingerprint) !~ '^[0-9a-f]{64}$'
       OR BTRIM(short_event.event_fingerprint) !~ '^[0-9a-f]{64}$'
       OR long_event.engine_snapshot #> '{prospective_anchor}'
            IS DISTINCT FROM
            short_event.engine_snapshot #> '{prospective_anchor}'
       OR long_event.engine_snapshot #>>
            '{prospective_anchor,sampler_version}' IS DISTINCT FROM
            NEW.sampler_version
       OR short_event.engine_snapshot #>>
            '{prospective_anchor,sampler_version}' IS DISTINCT FROM
            NEW.sampler_version
       OR long_event.engine_snapshot #>>
            '{prospective_anchor,input_fingerprint}' IS DISTINCT FROM
            BTRIM(NEW.input_fingerprint)
       OR short_event.engine_snapshot #>>
            '{prospective_anchor,input_fingerprint}' IS DISTINCT FROM
            BTRIM(NEW.input_fingerprint)
       OR (long_event.engine_snapshot #>>
            '{prospective_anchor,source_candle_open_utc}')::timestamptz
            IS DISTINCT FROM NEW.source_candle_open_utc
       OR (short_event.engine_snapshot #>>
            '{prospective_anchor,source_candle_open_utc}')::timestamptz
            IS DISTINCT FROM NEW.source_candle_open_utc
       OR (long_event.engine_snapshot #>>
            '{prospective_anchor,source_candle_close_utc}')::timestamptz
            IS DISTINCT FROM NEW.source_candle_close_utc
       OR (short_event.engine_snapshot #>>
            '{prospective_anchor,source_candle_close_utc}')::timestamptz
            IS DISTINCT FROM NEW.source_candle_close_utc
       OR (long_event.engine_snapshot #>>
            '{prospective_anchor,base_eligible_at_utc}')::timestamptz
            IS DISTINCT FROM NEW.base_eligible_at_utc
       OR (short_event.engine_snapshot #>>
            '{prospective_anchor,base_eligible_at_utc}')::timestamptz
            IS DISTINCT FROM NEW.base_eligible_at_utc
       OR (long_event.engine_snapshot #>>
            '{prospective_anchor,expires_at_utc}')::timestamptz
            IS DISTINCT FROM NEW.expires_at_utc
       OR (short_event.engine_snapshot #>>
            '{prospective_anchor,expires_at_utc}')::timestamptz
            IS DISTINCT FROM NEW.expires_at_utc
       OR (long_event.engine_snapshot #>>
            '{prospective_anchor,decision_time_utc}')::timestamptz
            IS DISTINCT FROM NEW.decision_time_utc
       OR (short_event.engine_snapshot #>>
            '{prospective_anchor,decision_time_utc}')::timestamptz
            IS DISTINCT FROM NEW.decision_time_utc
       OR long_event.engine_snapshot #>>
            '{prospective_anchor,coverage_policy_version}' IS DISTINCT FROM
            NEW.coverage_policy_version
       OR short_event.engine_snapshot #>>
            '{prospective_anchor,coverage_policy_version}' IS DISTINCT FROM
            NEW.coverage_policy_version
       OR long_event.engine_snapshot #>
            '{prospective_anchor,coverage_snapshot}' IS DISTINCT FROM
            NEW.coverage_snapshot
       OR short_event.engine_snapshot #>
            '{prospective_anchor,coverage_snapshot}' IS DISTINCT FROM
            NEW.coverage_snapshot
       OR long_event.engine_snapshot #>
            '{prospective_anchor,source_timestamps}' IS DISTINCT FROM
            NEW.source_timestamps
       OR short_event.engine_snapshot #>
            '{prospective_anchor,source_timestamps}' IS DISTINCT FROM
            NEW.source_timestamps
       OR long_event.engine_snapshot #>
            '{prospective_anchor,source_provenance}' IS DISTINCT FROM
            NEW.source_provenance
       OR short_event.engine_snapshot #>
            '{prospective_anchor,source_provenance}' IS DISTINCT FROM
            NEW.source_provenance
       OR long_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs}' IS DISTINCT FROM
            NEW.frozen_inputs
       OR short_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs}' IS DISTINCT FROM
            NEW.frozen_inputs
       OR long_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs}' IS DISTINCT FROM
            short_event.engine_snapshot #>
            '{prospective_anchor,frozen_inputs}'
       OR long_frozen_price_invalid
       OR short_frozen_price_invalid
       OR official_price_provenance_invalid
       OR long_event.engine_snapshot #>>
            '{prospective_anchor,telegram_delivery_allowed}'
            IS DISTINCT FROM 'false'
       OR short_event.engine_snapshot #>>
            '{prospective_anchor,telegram_delivery_allowed}'
            IS DISTINCT FROM 'false'
       OR long_event.engine_snapshot #>>
            '{prospective_anchor,delivery_status}'
            IS DISTINCT FROM 'NOT_APPLICABLE'
       OR short_event.engine_snapshot #>>
            '{prospective_anchor,delivery_status}'
            IS DISTINCT FROM 'NOT_APPLICABLE'
       OR NOT (long_event.categories ? 'SILENT')
       OR NOT (short_event.categories ? 'SILENT')
    THEN
        RAISE EXCEPTION
            'invalid prospective DECISION_SAMPLE LONG/SHORT pair for % at %',
            NEW.symbol, NEW.source_candle_open_utc;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_prospective_anchor_pair
    ON research_prospective_anchor_slots;

CREATE TRIGGER trg_validate_prospective_anchor_pair
BEFORE INSERT OR UPDATE ON research_prospective_anchor_slots
FOR EACH ROW EXECUTE FUNCTION validate_prospective_anchor_pair();

-- Once a slot is authoritative, neither its ledger rows nor either referenced
-- Research Event may be rewritten.  The input SHA-256 remains the stable
-- payload identity; the row guards make that identity meaningful over time.
CREATE OR REPLACE FUNCTION prevent_prospective_anchor_archive_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prospective_anchor_attempts_append_only
    ON research_prospective_anchor_attempts;

CREATE TRIGGER trg_prospective_anchor_attempts_append_only
BEFORE UPDATE OR DELETE ON research_prospective_anchor_attempts
FOR EACH ROW EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

DROP TRIGGER IF EXISTS trg_prospective_anchor_slots_append_only
    ON research_prospective_anchor_slots;

CREATE TRIGGER trg_prospective_anchor_slots_append_only
BEFORE UPDATE OR DELETE ON research_prospective_anchor_slots
FOR EACH ROW EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

DROP TRIGGER IF EXISTS trg_prospective_anchor_attempts_no_truncate
    ON research_prospective_anchor_attempts;

CREATE TRIGGER trg_prospective_anchor_attempts_no_truncate
BEFORE TRUNCATE ON research_prospective_anchor_attempts
FOR EACH STATEMENT EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

DROP TRIGGER IF EXISTS trg_prospective_anchor_slots_no_truncate
    ON research_prospective_anchor_slots;

CREATE TRIGGER trg_prospective_anchor_slots_no_truncate
BEFORE TRUNCATE ON research_prospective_anchor_slots
FOR EACH STATEMENT EXECUTE FUNCTION prevent_prospective_anchor_archive_mutation();

CREATE OR REPLACE FUNCTION prevent_prospective_anchor_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM research_prospective_anchor_slots slot
         WHERE slot.long_event_id = OLD.event_id
            OR slot.short_event_id = OLD.event_id
    ) THEN
        RAISE EXCEPTION
            'Research Event % belongs to an immutable prospective anchor',
            OLD.event_id;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prospective_anchor_events_immutable
    ON research_events;

CREATE TRIGGER trg_prospective_anchor_events_immutable
BEFORE UPDATE OR DELETE ON research_events
FOR EACH ROW EXECUTE FUNCTION prevent_prospective_anchor_event_mutation();

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
    slot.frozen_inputs
FROM research_prospective_anchor_slots slot
CROSS JOIN LATERAL (
    VALUES (slot.long_event_id), (slot.short_event_id)
) expected(event_id)
JOIN research_events event ON event.event_id = expected.event_id
WHERE event.event_kind = 'DECISION_SAMPLE'
  AND event.event_type = 'PROSPECTIVE_NEUTRAL_30M'
  AND event.capture_stage = 'SILENT_NEUTRAL_ANCHOR'
  AND event.delivery_status = 'NOT_APPLICABLE';

COMMENT ON VIEW research_prospective_shadow_events IS
    'Silent neutral 30m DECISION_SAMPLE events eligible only for prospective Shadow evaluation; never Telegram alerts.';

-- Defense in depth: suppress every attempt to queue or mark sent a
-- DECISION_SAMPLE without rolling back unrelated Shadow persistence.  The
-- migration also refuses to install over a pre-existing invalid queue row, so
-- there is no earlier PENDING row that an UPDATE could preserve.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM research_formula_live_deliveries delivery
          JOIN research_events event ON event.event_id = delivery.event_id
         WHERE event.event_kind = 'DECISION_SAMPLE'
    ) THEN
        RAISE EXCEPTION
            'Cannot install neutral-anchor delivery guard: a DECISION_SAMPLE delivery row already exists';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION suppress_decision_sample_live_delivery()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM research_events event
         WHERE event.event_id = NEW.event_id
           AND event.event_kind = 'DECISION_SAMPLE'
    ) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_suppress_decision_sample_live_delivery
    ON research_formula_live_deliveries;

CREATE TRIGGER trg_suppress_decision_sample_live_delivery
BEFORE INSERT OR UPDATE ON research_formula_live_deliveries
FOR EACH ROW EXECUTE FUNCTION suppress_decision_sample_live_delivery();

-- Prevent a delivered or queued ALERT from being relabelled after the fact as
-- a DECISION_SAMPLE and thereby evading the delivery-table trigger.
CREATE OR REPLACE FUNCTION prevent_decision_sample_delivery_reclassification()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_kind = 'DECISION_SAMPLE'
       AND OLD.event_kind IS DISTINCT FROM NEW.event_kind
       AND EXISTS (
           SELECT 1
             FROM research_formula_live_deliveries delivery
            WHERE delivery.event_id = OLD.event_id
       )
    THEN
        RAISE EXCEPTION
            'Research Event % has a delivery row and cannot become a DECISION_SAMPLE',
            OLD.event_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_decision_sample_delivery_reclassification
    ON research_events;

CREATE TRIGGER trg_prevent_decision_sample_delivery_reclassification
BEFORE UPDATE OF event_kind ON research_events
FOR EACH ROW EXECUTE FUNCTION prevent_decision_sample_delivery_reclassification();
