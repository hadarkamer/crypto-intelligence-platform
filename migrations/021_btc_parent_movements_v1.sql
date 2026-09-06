-- Global causal BTC parent membership. This new conservative engineering policy
-- uses 200bps close reversals, never time buckets, retroactive pivots or outcomes.
CREATE TABLE IF NOT EXISTS research_btc_price_bars (
    open_time_utc TIMESTAMPTZ PRIMARY KEY,
    close_time_utc TIMESTAMPTZ NOT NULL UNIQUE,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    price_source TEXT NOT NULL DEFAULT 'BINANCE_SPOT_BTCUSDT_1M'
        CHECK (price_source='BINANCE_SPOT_BTCUSDT_1M'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (date_trunc('minute', open_time_utc)=open_time_utc),
    CHECK (close_time_utc=open_time_utc+INTERVAL '1 minute'-INTERVAL '1 millisecond'),
    CHECK (low > 0 AND high < 'Infinity'::double precision
        AND low <= open AND low <= close AND high >= open AND high >= close)
);

CREATE TABLE IF NOT EXISTS research_btc_parent_movements (
    btc_parent_movement_id TEXT PRIMARY KEY,
    episode_policy_version TEXT NOT NULL CHECK (
        episode_policy_version='btc-parent-close-reversal-200bps-v1'),
    start_time_utc TIMESTAMPTZ NOT NULL,
    end_time_utc TIMESTAMPTZ,
    confirmed_at_utc TIMESTAMPTZ,
    direction TEXT NOT NULL CHECK (direction IN ('UP','DOWN','UNKNOWN')),
    evidence_eligible BOOLEAN NOT NULL,
    boundary_reason TEXT NOT NULL CHECK (boundary_reason IN (
        'CAUSAL_CLOSE_REVERSAL','LEFT_BOUNDARY_UNVERIFIED','BTC_DATA_GAP')),
    observed_through_utc TIMESTAMPTZ NOT NULL,
    price_source TEXT NOT NULL CHECK (price_source='BINANCE_SPOT_BTCUSDT_1M'),
    state_json JSONB NOT NULL CHECK (jsonb_typeof(state_json)='object'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (btc_parent_movement_id, episode_policy_version),
    UNIQUE (episode_policy_version,start_time_utc),
    CHECK (end_time_utc IS NULL OR end_time_utc>start_time_utc),
    CHECK (observed_through_utc>=start_time_utc),
    CHECK ((evidence_eligible AND confirmed_at_utc IS NOT NULL
            AND confirmed_at_utc=start_time_utc
            AND boundary_reason='CAUSAL_CLOSE_REVERSAL' AND direction<>'UNKNOWN')
        OR (NOT evidence_eligible AND confirmed_at_utc IS NULL
            AND boundary_reason<>'CAUSAL_CLOSE_REVERSAL'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_btc_parent_active
    ON research_btc_parent_movements (episode_policy_version)
    WHERE end_time_utc IS NULL;
CREATE INDEX IF NOT EXISTS idx_research_btc_parent_time
    ON research_btc_parent_movements (episode_policy_version,start_time_utc DESC);

CREATE TABLE IF NOT EXISTS research_event_btc_movements (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    episode_policy_version TEXT NOT NULL CHECK (
        episode_policy_version='btc-parent-close-reversal-200bps-v1'),
    btc_parent_movement_id TEXT,
    decision_time_utc TIMESTAMPTZ NOT NULL,
    btc_observed_close_utc TIMESTAMPTZ,
    membership_status TEXT NOT NULL CHECK (membership_status IN (
        'LIVE','BTC_DATA_MISSING','BOUNDARY_UNVERIFIED')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id,episode_policy_version),
    FOREIGN KEY (btc_parent_movement_id,episode_policy_version)
        REFERENCES research_btc_parent_movements
            (btc_parent_movement_id,episode_policy_version),
    CHECK (membership_status='BTC_DATA_MISSING' OR (
        btc_parent_movement_id IS NOT NULL AND btc_observed_close_utc IS NOT NULL
        AND btc_observed_close_utc<=decision_time_utc
        AND btc_observed_close_utc>decision_time_utc-INTERVAL '1 minute'))
);
CREATE INDEX IF NOT EXISTS idx_research_event_btc_parent
    ON research_event_btc_movements (btc_parent_movement_id,event_id)
    WHERE membership_status='LIVE';

CREATE OR REPLACE FUNCTION research_validate_event_btc_membership_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    parent research_btc_parent_movements%ROWTYPE;
    event_time timestamptz;
BEGIN
    SELECT alert_time_utc INTO event_time FROM research_events
        WHERE event_id=NEW.event_id;
    IF event_time IS DISTINCT FROM NEW.decision_time_utc THEN
        RAISE EXCEPTION 'BTC membership decision must equal immutable event time';
    END IF;
    IF NEW.membership_status IN ('LIVE','BOUNDARY_UNVERIFIED') THEN
        SELECT * INTO parent FROM research_btc_parent_movements
            WHERE btc_parent_movement_id=NEW.btc_parent_movement_id
              AND episode_policy_version=NEW.episode_policy_version;
        IF NOT FOUND OR NEW.decision_time_utc<parent.start_time_utc
            OR (parent.end_time_utc IS NOT NULL
                AND NEW.decision_time_utc>=parent.end_time_utc)
            OR NEW.btc_observed_close_utc<parent.start_time_utc THEN
            RAISE EXCEPTION 'BTC membership is outside its causal parent interval';
        END IF;
        IF (NEW.membership_status='LIVE') IS DISTINCT FROM parent.evidence_eligible THEN
            RAISE EXCEPTION 'BTC membership eligibility disagrees with parent';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM research_btc_price_bars
            WHERE close_time_utc=NEW.btc_observed_close_utc) THEN
            RAISE EXCEPTION 'BTC membership requires its actual source bar';
        END IF;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS research_event_btc_membership_v1
    ON research_event_btc_movements;
CREATE TRIGGER research_event_btc_membership_v1
    BEFORE INSERT OR UPDATE ON research_event_btc_movements
    FOR EACH ROW EXECUTE FUNCTION research_validate_event_btc_membership_v1();
