-- Candidate migration only: apply to an isolated database for tests.
-- Production requires a reviewed migration identity and verified runtime role.
-- Do not add this DDL to application startup or grant browser/API roles access.
CREATE SCHEMA IF NOT EXISTS collector_private;
REVOKE ALL ON SCHEMA collector_private FROM PUBLIC;

CREATE TABLE IF NOT EXISTS collector_private.coinglass_session_state (
    slot text PRIMARY KEY CHECK (slot = 'coinglass'),
    revision bigint NOT NULL CHECK (revision > 0),
    status text NOT NULL CHECK (status IN ('ready','in_use','blocked','unconfirmed')),
    run_id uuid,
    encrypted_payload text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'in_use') = (run_id IS NOT NULL)),
    CHECK ((status IN ('ready','in_use')) = (encrypted_payload IS NOT NULL)),
    CHECK (encrypted_payload IS NULL OR octet_length(encrypted_payload) <= 131072)
);
REVOKE ALL ON collector_private.coinglass_session_state FROM PUBLIC;

-- Approved deployment must grant only the verified collector role:
-- USAGE on collector_private; SELECT, INSERT, UPDATE on this table.
-- Do not grant DELETE, CREATE, ownership or access to frontend/API roles.
-- The application encryption key stays only in private worker configuration,
-- never in this database, the migration, job records, evidence, or browser env.
