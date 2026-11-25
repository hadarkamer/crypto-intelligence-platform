-- Apply using the migration owner after session_state.sql. No legacy rows are read or copied.
CREATE TABLE IF NOT EXISTS collector_private.coinglass_dedicated_sessions (
    slot text PRIMARY KEY CHECK (slot ~ '^email-v1:[0-9a-f]{64}$'),
    revision bigint NOT NULL CHECK (revision > 0),
    status text NOT NULL CHECK (status IN ('ready','in_use','unconfirmed')),
    run_id uuid,
    encrypted_payload text,
    map_retry_after timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'in_use') = (run_id IS NOT NULL)),
    CHECK ((status IN ('ready','in_use')) = (encrypted_payload IS NOT NULL)),
    CHECK (encrypted_payload IS NULL OR octet_length(encrypted_payload) <= 131072)
);

CREATE TABLE IF NOT EXISTS collector_private.coinglass_dedicated_auth (
    id uuid PRIMARY KEY,
    slot text NOT NULL REFERENCES collector_private.coinglass_dedicated_sessions(slot),
    phase text NOT NULL CHECK (phase IN ('preparing','submitted','retry','blocked','verified','superseded')),
    attempts smallint NOT NULL CHECK (attempts BETWEEN 0 AND 2),
    retry_until timestamptz NOT NULL,
    retry_after timestamptz,
    owner_job uuid,
    owner_revision bigint,
    reason text CHECK (reason IN ('auth_invalid_credentials','auth_challenge','auth_code_required',
        'auth_rate_limited','auth_unconfirmed','auth_gate_after_verification','auth_retry_exhausted',
        'auth_form_unavailable','auth_configuration_invalid','auth_retry_pending')),
    notified_at timestamptz,
    verified_at timestamptz,
    verified_job uuid,
    superseded_by uuid REFERENCES collector_private.coinglass_dedicated_auth(id) DEFERRABLE INITIALLY DEFERRED,
    CHECK ((phase IN ('preparing','submitted')) = (owner_job IS NOT NULL)),
    CHECK ((owner_job IS NULL) = (owner_revision IS NULL)),
    CHECK ((phase = 'verified') = (verified_at IS NOT NULL)),
    CHECK ((verified_job IS NULL) = (verified_at IS NULL)),
    CHECK ((phase = 'superseded') = (superseded_by IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS coinglass_dedicated_open
    ON collector_private.coinglass_dedicated_auth(slot) WHERE phase NOT IN ('verified','superseded');
CREATE INDEX IF NOT EXISTS coinglass_dedicated_proof
    ON collector_private.coinglass_dedicated_auth(slot,verified_at DESC) WHERE phase='verified';
CREATE TABLE IF NOT EXISTS collector_private.coinglass_dedicated_auth_jobs (
    job_id uuid PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES collector_private.coinglass_dedicated_auth(id)
);
REVOKE ALL ON collector_private.coinglass_dedicated_sessions,
    collector_private.coinglass_dedicated_auth,collector_private.coinglass_dedicated_auth_jobs FROM PUBLIC;
-- Grant the verified collector role schema USAGE and SELECT, INSERT, UPDATE on these three
-- tables via the existing private migration path. No browser or public API role needs access.
