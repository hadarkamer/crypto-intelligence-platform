-- Explicit migration only. Runtime never creates or alters this table.
-- Apply only with approval for the concrete producer/receiver release.
CREATE TABLE IF NOT EXISTS approved_alert_execution_outbox (
    source_key text NOT NULL,
    occurrence_id text NOT NULL,
    source_position_id text NOT NULL,
    payload_json text,
    source_sequence bigint NOT NULL,
    acknowledged_sequence bigint NOT NULL DEFAULT 0,
    canceled boolean NOT NULL DEFAULT false,
    PRIMARY KEY (source_key, occurrence_id)
);
CREATE INDEX IF NOT EXISTS approved_alert_execution_outbox_pending
    ON approved_alert_execution_outbox(source_key, canceled DESC, source_sequence)
    WHERE acknowledged_sequence < source_sequence;
CREATE INDEX IF NOT EXISTS approved_alert_execution_outbox_active
    ON approved_alert_execution_outbox(source_key)
    WHERE NOT canceled;
CREATE INDEX IF NOT EXISTS approved_alert_execution_outbox_payload_capacity
    ON approved_alert_execution_outbox(source_key)
    WHERE payload_json IS NOT NULL;
