-- Explicitly registered, unapplied PREVIEW first-message storage v1
--
-- Additive schema preparation only.  This file is registered in the explicit
-- one-shot research_formula_schema_admin installer, which refuses execution
-- unless FORMULA_SCHEMA_APPLY=1 and a permitted database URL are both present.
-- Registration does not apply this migration or grant dispatch, Telegram,
-- Stage-6, research-evidence or LIVE authority.

CREATE TABLE IF NOT EXISTS research_preview_first_message_reservations (
    reservation_key CHAR(64) PRIMARY KEY CHECK (
        BTRIM(reservation_key) ~ '^[0-9a-f]{64}$'
    ),
    reservation_candidate_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(reservation_candidate_id) ~ '^[0-9a-f]{64}$'
    ),
    contract_version TEXT NOT NULL CHECK (
        contract_version = 'preview-first-message-consumption-v1-not-persisted'
    ),
    reservation_policy_version TEXT NOT NULL CHECK (
        reservation_policy_version = 'preview-first-message-reservation-unique-v1'
    ),
    application_gate_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(application_gate_id) ~ '^[0-9a-f]{64}$'
    ),
    owner_approval_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(owner_approval_id) ~ '^[0-9a-f]{64}$'
    ),
    authorization_candidate_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(authorization_candidate_id) ~ '^[0-9a-f]{64}$'
    ),
    one_shot_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(one_shot_key) ~ '^[0-9a-f]{64}$'
    ),
    runtime_connector_registration_id CHAR(64) NOT NULL CHECK (
        BTRIM(runtime_connector_registration_id) ~ '^[0-9a-f]{64}$'
    ),
    activation_gate_id CHAR(64) NOT NULL CHECK (
        BTRIM(activation_gate_id) ~ '^[0-9a-f]{64}$'
    ),
    adapter_request_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(adapter_request_id) ~ '^[0-9a-f]{64}$'
    ),
    request_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(request_key) ~ '^[0-9a-f]{64}$'
    ),
    observed_at_utc TIMESTAMPTZ NOT NULL,
    expires_at_utc TIMESTAMPTZ NOT NULL CHECK (
        expires_at_utc > observed_at_utc
    ),
    reservation_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(reservation_payload) = 'object'
        AND reservation_payload ->> 'reservation_key' = reservation_key
        AND reservation_payload ->> 'reservation_candidate_id' =
            reservation_candidate_id
        AND reservation_payload ->> 'owner_approval_id' = owner_approval_id
        AND reservation_payload ->> 'one_shot_key' = one_shot_key
        AND reservation_payload ->> 'request_key' = request_key
        AND reservation_payload ->> 'status' =
            'RESERVATION_PREPARED_NOT_PERSISTED'
        AND reservation_payload -> 'dispatch_allowed' = 'false'::JSONB
        AND reservation_payload -> 'delivery_allowed' = 'false'::JSONB
        AND reservation_payload -> 'stage6_activated' = 'false'::JSONB
        AND reservation_payload ->> 'research_evidence_effect' = 'NONE'
        AND reservation_payload ->> 'live_effect' = 'NONE'
    ),
    recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_preview_first_message_reservation_binding UNIQUE (
        reservation_key,
        owner_approval_id,
        one_shot_key,
        adapter_request_id,
        request_key
    )
);

CREATE TABLE IF NOT EXISTS research_preview_first_message_consumptions (
    consumption_key CHAR(64) PRIMARY KEY CHECK (
        BTRIM(consumption_key) ~ '^[0-9a-f]{64}$'
    ),
    consumption_candidate_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(consumption_candidate_id) ~ '^[0-9a-f]{64}$'
    ),
    contract_version TEXT NOT NULL CHECK (
        contract_version = 'preview-first-message-consumption-v1-not-persisted'
    ),
    consumption_policy_version TEXT NOT NULL CHECK (
        consumption_policy_version = 'preview-first-message-consumption-unique-v1'
    ),
    reservation_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(reservation_key) ~ '^[0-9a-f]{64}$'
    ),
    owner_approval_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(owner_approval_id) ~ '^[0-9a-f]{64}$'
    ),
    one_shot_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(one_shot_key) ~ '^[0-9a-f]{64}$'
    ),
    adapter_request_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(adapter_request_id) ~ '^[0-9a-f]{64}$'
    ),
    request_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(request_key) ~ '^[0-9a-f]{64}$'
    ),
    delivery_attempt_id CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(delivery_attempt_id) ~ '^[0-9a-f]{64}$'
    ),
    telegram_message_id BIGINT NOT NULL UNIQUE CHECK (
        telegram_message_id <> 0
    ),
    confirmed_at_utc TIMESTAMPTZ NOT NULL,
    consumption_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(consumption_payload) = 'object'
        AND consumption_payload ->> 'consumption_key' = consumption_key
        AND consumption_payload ->> 'consumption_candidate_id' =
            consumption_candidate_id
        AND consumption_payload ->> 'reservation_key' = reservation_key
        AND consumption_payload ->> 'owner_approval_id' = owner_approval_id
        AND consumption_payload ->> 'one_shot_key' = one_shot_key
        AND consumption_payload ->> 'delivery_attempt_id' =
            delivery_attempt_id
        AND (consumption_payload ->> 'telegram_message_id')::BIGINT =
            telegram_message_id
        AND consumption_payload ->> 'status' =
            'CONSUMPTION_PREPARED_NOT_PERSISTED'
        AND consumption_payload -> 'automatic_retry_allowed' = 'false'::JSONB
        AND consumption_payload -> 'dispatch_allowed' = 'false'::JSONB
        AND consumption_payload -> 'delivery_allowed' = 'false'::JSONB
        AND consumption_payload -> 'stage6_activated' = 'false'::JSONB
        AND consumption_payload ->> 'research_evidence_effect' = 'NONE'
        AND consumption_payload ->> 'live_effect' = 'NONE'
    ),
    recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_preview_first_message_consumption_reservation
        FOREIGN KEY (
            reservation_key,
            owner_approval_id,
            one_shot_key,
            adapter_request_id,
            request_key
        )
        REFERENCES research_preview_first_message_reservations (
            reservation_key,
            owner_approval_id,
            one_shot_key,
            adapter_request_id,
            request_key
        )
);

CREATE OR REPLACE FUNCTION validate_preview_first_message_consumption()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM research_preview_first_message_reservations AS reservation
        WHERE reservation.reservation_key = NEW.reservation_key
          AND reservation.owner_approval_id = NEW.owner_approval_id
          AND reservation.one_shot_key = NEW.one_shot_key
          AND reservation.adapter_request_id = NEW.adapter_request_id
          AND reservation.request_key = NEW.request_key
          AND NEW.confirmed_at_utc >= reservation.observed_at_utc
          AND NEW.confirmed_at_utc < reservation.expires_at_utc
    ) THEN
        RAISE EXCEPTION
            'PREVIEW first-message consumption requires an exact, current reservation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION prevent_preview_first_message_storage_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'PREVIEW first-message storage is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_preview_first_message_consumption
    ON research_preview_first_message_consumptions;

CREATE TRIGGER trg_validate_preview_first_message_consumption
BEFORE INSERT ON research_preview_first_message_consumptions
FOR EACH ROW EXECUTE FUNCTION validate_preview_first_message_consumption();

DROP TRIGGER IF EXISTS trg_preview_first_message_reservations_append_only
    ON research_preview_first_message_reservations;

CREATE TRIGGER trg_preview_first_message_reservations_append_only
BEFORE UPDATE OR DELETE ON research_preview_first_message_reservations
FOR EACH ROW EXECUTE FUNCTION prevent_preview_first_message_storage_mutation();

DROP TRIGGER IF EXISTS trg_preview_first_message_reservations_no_truncate
    ON research_preview_first_message_reservations;

CREATE TRIGGER trg_preview_first_message_reservations_no_truncate
BEFORE TRUNCATE ON research_preview_first_message_reservations
FOR EACH STATEMENT EXECUTE FUNCTION prevent_preview_first_message_storage_mutation();

DROP TRIGGER IF EXISTS trg_preview_first_message_consumptions_append_only
    ON research_preview_first_message_consumptions;

CREATE TRIGGER trg_preview_first_message_consumptions_append_only
BEFORE UPDATE OR DELETE ON research_preview_first_message_consumptions
FOR EACH ROW EXECUTE FUNCTION prevent_preview_first_message_storage_mutation();

DROP TRIGGER IF EXISTS trg_preview_first_message_consumptions_no_truncate
    ON research_preview_first_message_consumptions;

CREATE TRIGGER trg_preview_first_message_consumptions_no_truncate
BEFORE TRUNCATE ON research_preview_first_message_consumptions
FOR EACH STATEMENT EXECUTE FUNCTION prevent_preview_first_message_storage_mutation();

COMMENT ON TABLE research_preview_first_message_reservations IS
    'Unregistered append-only reservation candidates for one private PREVIEW message; no dispatch or activation authority.';

COMMENT ON TABLE research_preview_first_message_consumptions IS
    'Unregistered append-only confirmed-delivery consumption candidates; no retry, research-evidence, Stage-6 or LIVE authority.';
