-- Independent user-requested dual-CVD Watch rule; existing research and MP65
-- transition policies are untouched. Runtime never executes this migration.
CREATE TABLE IF NOT EXISTS dual_cvd65_scopes (
 subscription_scope text NOT NULL,
 rule_id text NOT NULL,
 activated_at_utc timestamptz NOT NULL,
 last_source_at_utc timestamptz,
 state jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(state)='object'),
 updated_at_utc timestamptz NOT NULL,
 PRIMARY KEY(subscription_scope,rule_id)
);
CREATE TABLE IF NOT EXISTS dual_cvd65_receipts (
 subscription_scope text NOT NULL,
 rule_id text NOT NULL,
 watch_scan_id text NOT NULL,
 source_at_utc timestamptz NOT NULL,
 input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
 result jsonb NOT NULL CHECK (jsonb_typeof(result)='object'),
 created_at_utc timestamptz NOT NULL,
 PRIMARY KEY(subscription_scope,rule_id,watch_scan_id),
 FOREIGN KEY(subscription_scope,rule_id) REFERENCES dual_cvd65_scopes(subscription_scope,rule_id)
);
CREATE INDEX IF NOT EXISTS dual_cvd65_receipts_created
 ON dual_cvd65_receipts(subscription_scope,created_at_utc);
CREATE TABLE IF NOT EXISTS dual_cvd65_intents (
 intent_id text PRIMARY KEY,
 subscription_scope text NOT NULL,
 rule_id text NOT NULL,
 watch_scan_id text NOT NULL,
 symbol text NOT NULL,
 direction text NOT NULL CHECK (direction IN ('LONG','SHORT')),
 episode bigint NOT NULL CHECK (episode>0),
 generation_key text NOT NULL CHECK (generation_key ~ '^[0-9a-f]{64}$'),
 bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
 source_at_utc timestamptz NOT NULL,
 expires_at timestamptz NOT NULL CHECK (expires_at>=source_at_utc AND expires_at<=source_at_utc+interval '10 minutes'),
 text text NOT NULL,
 payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
 status text NOT NULL DEFAULT 'PENDING'
   CHECK (status IN ('PENDING','IN_FLIGHT','DELIVERED','UNKNOWN','FAILED','EXPIRED')),
 attempt_token text,
 attempted_at_utc timestamptz,
 finished_at_utc timestamptz,
 created_at_utc timestamptz NOT NULL,
 UNIQUE(subscription_scope,rule_id,symbol,generation_key),
 FOREIGN KEY(subscription_scope,rule_id) REFERENCES dual_cvd65_scopes(subscription_scope,rule_id),
 CHECK ((status IN ('PENDING','EXPIRED') AND attempt_token IS NULL AND attempted_at_utc IS NULL)
     OR (status IN ('IN_FLIGHT','DELIVERED','UNKNOWN','FAILED') AND attempt_token IS NOT NULL
         AND attempted_at_utc IS NOT NULL)),
 CHECK (finished_at_utc IS NULL OR finished_at_utc>=COALESCE(attempted_at_utc,source_at_utc))
);
CREATE INDEX IF NOT EXISTS dual_cvd65_intents_pending
 ON dual_cvd65_intents(subscription_scope,source_at_utc,intent_id) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS dual_cvd65_intents_in_flight
 ON dual_cvd65_intents(subscription_scope,attempted_at_utc) WHERE status='IN_FLIGHT';

CREATE OR REPLACE FUNCTION dual_cvd65_immutable_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'Dual CVD receipt is immutable'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS dual_cvd65_receipt_immutable ON dual_cvd65_receipts;
CREATE TRIGGER dual_cvd65_receipt_immutable BEFORE UPDATE ON dual_cvd65_receipts
 FOR EACH ROW EXECUTE FUNCTION dual_cvd65_immutable_receipt();

CREATE OR REPLACE FUNCTION dual_cvd65_intent_transition_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.intent_id,NEW.subscription_scope,NEW.rule_id,NEW.watch_scan_id,NEW.symbol,
        NEW.direction,NEW.episode,NEW.generation_key,NEW.bundle_sha256,NEW.source_at_utc,
        NEW.expires_at,NEW.text,NEW.payload,NEW.created_at_utc)
    IS DISTINCT FROM
    ROW(OLD.intent_id,OLD.subscription_scope,OLD.rule_id,OLD.watch_scan_id,OLD.symbol,
        OLD.direction,OLD.episode,OLD.generation_key,OLD.bundle_sha256,OLD.source_at_utc,
        OLD.expires_at,OLD.text,OLD.payload,OLD.created_at_utc) THEN
   RAISE EXCEPTION 'Dual CVD intent evidence is immutable';
 END IF;
 IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 IF NOT ((OLD.status='PENDING' AND NEW.status IN ('IN_FLIGHT','EXPIRED'))
      OR (OLD.status='IN_FLIGHT' AND NEW.status IN ('PENDING','EXPIRED','DELIVERED','UNKNOWN','FAILED'))) THEN
   RAISE EXCEPTION 'Invalid dual CVD delivery transition';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS dual_cvd65_intent_guard ON dual_cvd65_intents;
CREATE TRIGGER dual_cvd65_intent_guard BEFORE UPDATE ON dual_cvd65_intents
 FOR EACH ROW EXECUTE FUNCTION dual_cvd65_intent_transition_guard();
