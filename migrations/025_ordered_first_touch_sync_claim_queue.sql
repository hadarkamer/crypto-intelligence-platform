-- One ordered access path for both due retries and expired leases. The
-- separate status-lane indices cannot serve the claim query's global ORDER
-- BY without sorting the entire active queue before FOR UPDATE / LIMIT.
-- This changes no outbox identities, leases, payloads or outcome evidence.
CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_sync_claim_queue
    ON research_ordered_first_touch_sync_outbox (
        destination, next_attempt_at_utc ASC, created_at_utc ASC,
        event_id, window_minutes, threshold_bps
    )
    WHERE sync_status IN ('PENDING', 'RETRY', 'IN_FLIGHT');
