-- Telegram source evidence stays isolated from LIVE events and First Touch.
-- Reimports preserve revisions; a time proposal never becomes a reviewed time.
CREATE TABLE IF NOT EXISTS research_archive_intake_batches (
    intake_batch_key TEXT PRIMARY KEY,
    archive_revision_digest TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    source_chat_key TEXT NOT NULL,
    manifest JSONB NOT NULL,
    expected_source_revision_rows INTEGER NOT NULL CHECK (expected_source_revision_rows >= 0),
    intake_status TEXT NOT NULL DEFAULT 'INGESTING'
        CHECK (intake_status IN ('INGESTING', 'COMPLETE')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at_utc TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research_archive_source_messages (
    source_identity_key TEXT NOT NULL,
    source_revision_sha256 TEXT NOT NULL,
    source_chat_key TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind = 'TELEGRAM_DESKTOP_HTML_ARCHIVE'),
    message_text TEXT NOT NULL,
    raw_time_title TEXT NOT NULL,
    header_message_time_utc TIMESTAMPTZ,
    canonical_message_time_utc TIMESTAMPTZ,
    canonical_time_review_reference TEXT,
    candidate_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (candidate_eligible = FALSE),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_identity_key, source_revision_sha256),
    CHECK ((canonical_message_time_utc IS NULL) = (canonical_time_review_reference IS NULL))
);

CREATE TABLE IF NOT EXISTS research_archive_intake_members (
    intake_batch_key TEXT NOT NULL REFERENCES research_archive_intake_batches(intake_batch_key),
    source_identity_key TEXT NOT NULL,
    source_revision_sha256 TEXT NOT NULL,
    proposed_message_time_utc TIMESTAMPTZ,
    normalization_version TEXT NOT NULL,
    time_status TEXT NOT NULL,
    message_family TEXT NOT NULL,
    period_scope_ids JSONB NOT NULL,
    source_annotation JSONB NOT NULL,
    PRIMARY KEY (intake_batch_key, source_identity_key, source_revision_sha256),
    FOREIGN KEY (source_identity_key, source_revision_sha256)
        REFERENCES research_archive_source_messages(source_identity_key, source_revision_sha256)
);

CREATE INDEX IF NOT EXISTS idx_archive_source_chat_message
    ON research_archive_source_messages(source_chat_key, source_message_id);
CREATE INDEX IF NOT EXISTS idx_archive_intake_time
    ON research_archive_intake_members(intake_batch_key, proposed_message_time_utc);

-- A later export may contain a revised message without its earlier copy.
CREATE OR REPLACE VIEW research_archive_source_identity_conflicts AS
SELECT source_identity_key, COUNT(*) AS source_revision_count
FROM research_archive_source_messages
GROUP BY source_identity_key
HAVING COUNT(*) > 1;
