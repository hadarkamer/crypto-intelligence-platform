-- Additive bounded question search; old definitions and trials remain intact.
CREATE TABLE IF NOT EXISTS research_ordered_question_map (
  question_id TEXT PRIMARY KEY, definition JSONB NOT NULL,
  map_version TEXT NOT NULL, created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS research_ordered_feature_screens (
  event_id BIGINT NOT NULL REFERENCES research_events(event_id),
  feature_version TEXT NOT NULL, input_sha256 TEXT NOT NULL,
  features JSONB NOT NULL, checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(event_id,feature_version)
);
CREATE TABLE IF NOT EXISTS research_ordered_question_runs (
  question_id TEXT NOT NULL REFERENCES research_ordered_question_map(question_id),
  input_sha256 TEXT NOT NULL, result JSONB NOT NULL,
  checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(question_id,input_sha256)
);
CREATE INDEX IF NOT EXISTS idx_ordered_question_runs_latest
  ON research_ordered_question_runs(question_id,checked_at_utc DESC);
ALTER TABLE research_ordered_formula_scopes ADD COLUMN IF NOT EXISTS evaluation_input_sha256 TEXT;
COMMENT ON TABLE research_ordered_question_runs IS
 'Exact preserved Q01-Q72 map; feature screens are PARTIAL, not statistical validation. Every failure, missing field and unchanged-input skip remains auditable.';
