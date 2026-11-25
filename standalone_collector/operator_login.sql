-- Apply in the worker's existing private database together with session_state.sql.
-- As with the session table, the migration owner must verify the actual worker role
-- and grant it only schema USAGE and table SELECT/INSERT/UPDATE.
CREATE TABLE IF NOT EXISTS collector_private.coinglass_login_incidents (
    id uuid PRIMARY KEY,
    opened_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    resolved_revision bigint CHECK (resolved_revision > 0),
    human_wait_ms integer CHECK (human_wait_ms BETWEEN 0 AND 600000),
    window_id uuid,
    window_expires_at timestamptz,
    CHECK ((resolved_at IS NULL) = (resolved_revision IS NULL)),
    CHECK ((window_id IS NULL) = (window_expires_at IS NULL)),
    CHECK (resolved_at IS NULL OR window_id IS NULL)
);
-- One source account is authorized for this collector.
CREATE UNIQUE INDEX IF NOT EXISTS coinglass_login_single_open
    ON collector_private.coinglass_login_incidents ((true)) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS collector_private.coinglass_login_jobs (
    job_id uuid PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES collector_private.coinglass_login_incidents(id),
    symbol text NOT NULL CHECK (symbol ~ '^[A-Z0-9]{1,16}$'),
    model smallint NOT NULL CHECK (model IN (1,2,3)),
    timeframe text NOT NULL CHECK (timeframe IN ('12H','24H','48H'))
);
CREATE INDEX IF NOT EXISTS coinglass_login_jobs_target
    ON collector_private.coinglass_login_jobs (incident_id,symbol,model,timeframe);
REVOKE ALL ON collector_private.coinglass_login_incidents,
    collector_private.coinglass_login_jobs FROM PUBLIC;
