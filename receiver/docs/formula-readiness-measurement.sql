-- Read-only paired price comparison; preserve missing prices and failed
-- observations. Identical minute anchors imply no measurable difference at 1m.
-- Positive early_entry_advantage_pct means a better entry at readiness in the
-- expected direction; this is not realized profit or a success probability.
WITH observed AS (
 SELECT event_id, symbol, direction, alert_time_utc, delivery_status,
        engine_snapshot->>'watch_scan_id' AS watch_scan_id,
        engine_snapshot->'timing_measurement' AS timing,
        CASE WHEN symbol='HYPE' THEN 'BINANCE_HYPE_FUTURES_MARK_1M'
             ELSE 'BINANCE_SPOT_TRADE_1M' END AS route
 FROM research_events
 WHERE event_type='FORMULA_MP65_CVD_SHORT'
 AND engine_snapshot->'timing_measurement'->>'version'='formula-readiness-v1'
), anchors AS (
 SELECT *, CASE WHEN timing->>'status'='VALID'
                THEN (timing->>'ready_entry_minute_utc')::timestamptz END AS ready_minute,
           CASE WHEN timing->>'status'='VALID'
                THEN (timing->>'delivery_entry_minute_utc')::timestamptz END AS delivery_minute
 FROM observed
)
SELECT a.event_id,a.symbol,a.direction,a.watch_scan_id,a.timing,a.route,
       a.ready_minute,a.delivery_minute,r.open AS ready_price,d.open AS delivery_price,
       CASE WHEN r.open>0 AND d.open>0 AND a.direction IN ('LONG','SHORT')
            THEN (d.open/r.open-1)*100*CASE WHEN a.direction='LONG' THEN 1 ELSE -1 END
            END AS early_entry_advantage_pct,
       CASE WHEN a.timing->>'status'<>'VALID' THEN a.timing->>'status'
            WHEN r.open IS NULL OR d.open IS NULL THEN 'MISSING_PRICE'
            WHEN a.ready_minute=a.delivery_minute THEN 'SAME_MINUTE'
            ELSE 'PAIRED' END AS comparison_status
FROM anchors a
LEFT JOIN research_price_archive_bars r ON r.route=a.route AND r.symbol=a.symbol
 AND r.open_time_utc=a.ready_minute AND r.close_time_utc<NOW()
LEFT JOIN research_price_archive_bars d ON d.route=a.route AND d.symbol=a.symbol
 AND d.open_time_utc=a.delivery_minute AND d.close_time_utc<NOW()
ORDER BY a.event_id;
