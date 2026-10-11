-- Continuous coherent Max-Pain Watch archive v1
--
-- Additive only.  This archive deliberately has no foreign key, view, trigger,
-- copy or migration path from any pre-cutover archive.  Pre-cutover cohorts
-- calculated by another method can therefore never enter Formula Discovery
-- through this schema.

CREATE TABLE IF NOT EXISTS research_max_pain_snapshot_sets (
    snapshot_set_id BIGSERIAL PRIMARY KEY,
    snapshot_key CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(snapshot_key) ~ '^[0-9a-f]{64}$'
    ),
    archive_schema_version TEXT NOT NULL CHECK (
        archive_schema_version='research-max-pain-archive-v1'
    ),
    method_version TEXT NOT NULL CHECK (
        method_version='coherent-max-pain-seven-timeframe-v1'
    ),
    cutover_marker TEXT NOT NULL CHECK (
        cutover_marker='POST_LEGACY_METHOD_2026_08_29'
    ),
    cutover_time_utc TIMESTAMPTZ NOT NULL CHECK (
        cutover_time_utc='2026-08-29 00:00:00+00'::timestamptz
    ),
    cycle_id TEXT NOT NULL CHECK (BTRIM(cycle_id) <> ''),
    cycle_time_utc TIMESTAMPTZ NOT NULL,
    collection_started_at_utc TIMESTAMPTZ NOT NULL,
    collection_completed_at_utc TIMESTAMPTZ NOT NULL,
    available_at_utc TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (BTRIM(source) <> ''),
    collector_version TEXT NOT NULL CHECK (BTRIM(collector_version) <> ''),
    expected_timeframes TEXT[] NOT NULL CHECK (
        expected_timeframes=ARRAY['12h','24h','48h','3d','1w','2w','1m']::text[]
    ),
    expected_timeframe_count INTEGER NOT NULL CHECK (expected_timeframe_count=7),
    observed_timeframe_count INTEGER NOT NULL CHECK (
        observed_timeframe_count BETWEEN 0 AND 7
    ),
    observed_symbol_count INTEGER NOT NULL CHECK (observed_symbol_count >= 0),
    complete_symbol_count INTEGER NOT NULL CHECK (complete_symbol_count >= 0),
    incomplete_symbol_count INTEGER NOT NULL CHECK (incomplete_symbol_count >= 0),
    eligible_symbol_count INTEGER NOT NULL CHECK (eligible_symbol_count >= 0),
    ineligible_symbol_count INTEGER NOT NULL CHECK (ineligible_symbol_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    invalid_row_count INTEGER NOT NULL CHECK (
        invalid_row_count BETWEEN 0 AND row_count
    ),
    missing_timeframes TEXT[] NOT NULL DEFAULT '{}'::text[],
    duplicate_pairs TEXT[] NOT NULL DEFAULT '{}'::text[],
    skipped_symbols TEXT[] NOT NULL DEFAULT '{}'::text[],
    collection_status TEXT NOT NULL CHECK (
        collection_status IN ('COMPLETE', 'INCOMPLETE', 'FAILED')
    ),
    validation_status TEXT NOT NULL CHECK (
        validation_status IN ('PASS', 'PARTIAL', 'FAIL')
    ),
    freshness_status TEXT NOT NULL CHECK (
        freshness_status IN ('FRESH', 'PARTIAL', 'STALE', 'UNKNOWN')
    ),
    set_complete_7of7 BOOLEAN NOT NULL,
    research_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    completeness_report JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(completeness_report)='object'
    ),
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(validation_errors)='array'
    ),
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(source_metadata)='object'
    ),
    payload_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cycle_time_utc >= cutover_time_utc),
    CHECK (collection_started_at_utc <= collection_completed_at_utc),
    CHECK (collection_completed_at_utc <= available_at_utc),
    CHECK (
        missing_timeframes <@ ARRAY['12h','24h','48h','3d','1w','2w','1m']::text[]
        AND observed_timeframe_count + CARDINALITY(missing_timeframes)=7
    ),
    CHECK (complete_symbol_count + incomplete_symbol_count=observed_symbol_count),
    CHECK (eligible_symbol_count + ineligible_symbol_count=observed_symbol_count),
    CHECK (complete_symbol_count <= observed_symbol_count),
    CHECK (eligible_symbol_count <= complete_symbol_count),
    CHECK (
        NOT set_complete_7of7 OR (
            observed_timeframe_count=7
            AND CARDINALITY(missing_timeframes)=0
            AND CARDINALITY(duplicate_pairs)=0
        )
    ),
    CHECK (
        validation_status = CASE
            WHEN observed_symbol_count > 0
                 AND eligible_symbol_count=observed_symbol_count THEN 'PASS'
            WHEN eligible_symbol_count > 0 THEN 'PARTIAL'
            ELSE 'FAIL'
        END
    ),
    CHECK (research_eligible=(eligible_symbol_count > 0)),
    CHECK (
        NOT research_eligible OR (
            source IN ('WATCH_SHARED', 'RESEARCH_PASSIVE')
            AND collection_status='COMPLETE'
            AND validation_status IN ('PASS', 'PARTIAL')
            AND freshness_status IN ('FRESH', 'PARTIAL')
            AND set_complete_7of7
            AND observed_timeframe_count=7
            AND observed_symbol_count > 0
            AND eligible_symbol_count > 0
            AND CARDINALITY(missing_timeframes)=0
            AND CARDINALITY(duplicate_pairs)=0
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_research_max_pain_sets_prior_only
    ON research_max_pain_snapshot_sets (
        available_at_utc DESC, snapshot_set_id DESC
    )
    WHERE research_eligible=TRUE;

CREATE INDEX IF NOT EXISTS idx_research_max_pain_sets_cycle
    ON research_max_pain_snapshot_sets (cycle_time_utc DESC, snapshot_set_id DESC);

CREATE TABLE IF NOT EXISTS research_max_pain_snapshot_symbols (
    snapshot_set_id BIGINT NOT NULL REFERENCES research_max_pain_snapshot_sets(
        snapshot_set_id
    ) ON DELETE RESTRICT,
    symbol TEXT NOT NULL CHECK (
        BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
    ),
    observed_timeframe_count INTEGER NOT NULL CHECK (
        observed_timeframe_count BETWEEN 0 AND 7
    ),
    missing_timeframes TEXT[] NOT NULL DEFAULT '{}'::text[],
    duplicate_timeframes TEXT[] NOT NULL DEFAULT '{}'::text[],
    invalid_row_count INTEGER NOT NULL CHECK (invalid_row_count BETWEEN 0 AND 7),
    complete_7of7 BOOLEAN NOT NULL,
    price_overlay_coherent BOOLEAN NOT NULL,
    validation_status TEXT NOT NULL CHECK (
        validation_status IN ('PASS', 'FAIL')
    ),
    freshness_status TEXT NOT NULL CHECK (
        freshness_status IN ('FRESH', 'STALE', 'UNKNOWN')
    ),
    research_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(validation_errors)='array'
    ),
    payload_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_set_id, symbol),
    CHECK (
        missing_timeframes <@ ARRAY['12h','24h','48h','3d','1w','2w','1m']::text[]
        AND duplicate_timeframes <@ ARRAY['12h','24h','48h','3d','1w','2w','1m']::text[]
    ),
    CHECK (observed_timeframe_count + CARDINALITY(missing_timeframes)=7),
    CHECK (
        complete_7of7=(
            observed_timeframe_count=7
            AND CARDINALITY(missing_timeframes)=0
            AND CARDINALITY(duplicate_timeframes)=0
        )
    ),
    CHECK (validation_status = CASE WHEN research_eligible THEN 'PASS' ELSE 'FAIL' END),
    CHECK (
        NOT research_eligible OR (
            observed_timeframe_count=7
            AND CARDINALITY(missing_timeframes)=0
            AND CARDINALITY(duplicate_timeframes)=0
            AND invalid_row_count=0
            AND complete_7of7
            AND price_overlay_coherent
            AND validation_status='PASS'
            AND freshness_status='FRESH'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_research_max_pain_symbols_prior_only
    ON research_max_pain_snapshot_symbols (symbol, snapshot_set_id DESC)
    WHERE research_eligible=TRUE;

CREATE TABLE IF NOT EXISTS research_max_pain_snapshot_rows (
    snapshot_row_id BIGSERIAL PRIMARY KEY,
    snapshot_set_id BIGINT NOT NULL REFERENCES research_max_pain_snapshot_sets(
        snapshot_set_id
    ) ON DELETE RESTRICT,
    symbol TEXT NOT NULL CHECK (
        BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
    ),
    timeframe TEXT NOT NULL CHECK (
        timeframe IN ('12h','24h','48h','3d','1w','2w','1m')
    ),
    rank INTEGER CHECK (rank IS NULL OR rank > 0),
    source_observed_at_utc TIMESTAMPTZ,
    current_price DOUBLE PRECISION CHECK (
        current_price IS NULL OR current_price > 0
    ),
    coinglass_current_price DOUBLE PRECISION CHECK (
        coinglass_current_price IS NULL OR coinglass_current_price > 0
    ),
    price_source TEXT,
    price_exchange TEXT,
    price_market TEXT,
    price_pair TEXT,
    price_instrument TEXT,
    price_fetched_at_utc TIMESTAMPTZ,
    price_source_policy_status TEXT NOT NULL CHECK (
        price_source_policy_status IN ('PASS', 'FAIL', 'UNKNOWN')
    ),
    short_max_pain DOUBLE PRECISION CHECK (
        short_max_pain IS NULL OR short_max_pain > 0
    ),
    long_max_pain DOUBLE PRECISION CHECK (
        long_max_pain IS NULL OR long_max_pain > 0
    ),
    short_liquidation_amount DOUBLE PRECISION CHECK (
        short_liquidation_amount IS NULL OR short_liquidation_amount >= 0
    ),
    long_liquidation_amount DOUBLE PRECISION CHECK (
        long_liquidation_amount IS NULL OR long_liquidation_amount >= 0
    ),
    short_target_signed_distance_pct DOUBLE PRECISION,
    long_target_signed_distance_pct DOUBLE PRECISION,
    short_target_abs_distance_pct DOUBLE PRECISION CHECK (
        short_target_abs_distance_pct IS NULL OR short_target_abs_distance_pct >= 0
    ),
    long_target_abs_distance_pct DOUBLE PRECISION CHECK (
        long_target_abs_distance_pct IS NULL OR long_target_abs_distance_pct >= 0
    ),
    row_valid BOOLEAN NOT NULL,
    freshness_status TEXT NOT NULL CHECK (
        freshness_status IN ('FRESH', 'STALE', 'UNKNOWN')
    ),
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(validation_errors)='array'
    ),
    raw_provenance JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(raw_provenance)='object'
    ),
    payload_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        price_source_policy_status <> 'PASS'
        OR COALESCE(raw_provenance->>'price_interval', '') = '1m'
    ),
    CHECK (
        price_source_policy_status <> 'PASS'
        OR (
            symbol='HYPE'
            AND price_source='hyperliquid'
            AND price_exchange='hyperliquid'
            AND price_market='spot'
            AND price_pair='HYPE/USDT'
            AND price_instrument='@107'
        )
        OR (
            symbol<>'HYPE'
            AND price_source='binance_spot'
            AND price_exchange='binance'
            AND price_market='spot'
            AND REPLACE(price_pair, '/', '')=(symbol || 'USDT')
        )
    ),
    CHECK (
        NOT row_valid OR (
            price_source_policy_status='PASS'
            AND freshness_status='FRESH'
            AND current_price IS NOT NULL
            AND short_max_pain IS NOT NULL
            AND long_max_pain IS NOT NULL
            AND short_liquidation_amount IS NOT NULL
            AND long_liquidation_amount IS NOT NULL
            AND source_observed_at_utc IS NOT NULL
            AND price_fetched_at_utc IS NOT NULL
        )
    ),
    UNIQUE (snapshot_set_id, symbol, timeframe),
    FOREIGN KEY (snapshot_set_id, symbol)
        REFERENCES research_max_pain_snapshot_symbols(snapshot_set_id, symbol)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_research_max_pain_rows_symbol_set
    ON research_max_pain_snapshot_rows (
        symbol, snapshot_set_id, timeframe
    );

CREATE OR REPLACE FUNCTION assert_research_max_pain_snapshot_complete()
RETURNS TRIGGER AS $$
DECLARE
    target_set_id BIGINT;
    declared_rows INTEGER;
    declared_symbols INTEGER;
    declared_eligible_symbols INTEGER;
    declared_complete_symbols INTEGER;
    declared_incomplete_symbols INTEGER;
    declared_invalid_rows INTEGER;
    declared_timeframes INTEGER;
    is_eligible BOOLEAN;
    declared_freshness TEXT;
    available_at TIMESTAMPTZ;
    actual_rows INTEGER;
    actual_symbols INTEGER;
    actual_manifests INTEGER;
    actual_eligible_symbols INTEGER;
    actual_complete_symbols INTEGER;
    actual_incomplete_symbols INTEGER;
    actual_invalid_rows INTEGER;
    actual_timeframes INTEGER;
    actual_fresh_symbols INTEGER;
    actual_stale_symbols INTEGER;
    actual_freshness TEXT;
BEGIN
    target_set_id := COALESCE(NEW.snapshot_set_id, NULL);
    SELECT row_count, observed_symbol_count, eligible_symbol_count,
           complete_symbol_count, incomplete_symbol_count, invalid_row_count,
           observed_timeframe_count, research_eligible, freshness_status,
           available_at_utc
      INTO declared_rows, declared_symbols, declared_eligible_symbols,
           declared_complete_symbols, declared_incomplete_symbols,
           declared_invalid_rows, declared_timeframes, is_eligible,
           declared_freshness, available_at
    FROM research_max_pain_snapshot_sets
    WHERE snapshot_set_id=target_set_id;

    SELECT COUNT(*), COUNT(DISTINCT symbol), COUNT(DISTINCT timeframe)
      INTO actual_rows, actual_symbols, actual_timeframes
    FROM research_max_pain_snapshot_rows
    WHERE snapshot_set_id=target_set_id;

    SELECT COUNT(*), COUNT(*) FILTER (WHERE research_eligible),
           COUNT(*) FILTER (WHERE complete_7of7),
           COUNT(*) FILTER (WHERE NOT complete_7of7),
           COALESCE(SUM(invalid_row_count), 0),
           COUNT(*) FILTER (WHERE freshness_status='FRESH'),
           COUNT(*) FILTER (WHERE freshness_status='STALE')
      INTO actual_manifests, actual_eligible_symbols,
           actual_complete_symbols, actual_incomplete_symbols,
           actual_invalid_rows, actual_fresh_symbols, actual_stale_symbols
    FROM research_max_pain_snapshot_symbols
    WHERE snapshot_set_id=target_set_id;

    IF actual_rows <> declared_rows
       OR actual_symbols <> declared_symbols
       OR actual_manifests <> declared_symbols
       OR actual_eligible_symbols <> declared_eligible_symbols
       OR actual_complete_symbols <> declared_complete_symbols
       OR actual_incomplete_symbols <> declared_incomplete_symbols
       OR actual_invalid_rows <> declared_invalid_rows
       OR actual_timeframes <> declared_timeframes THEN
        RAISE EXCEPTION
            'Max-Pain snapshot set % declared rows/symbols/eligible %/%/% but contains rows/row-symbols/manifests/eligible %/%/%/%',
            target_set_id, declared_rows, declared_symbols,
            declared_eligible_symbols, actual_rows, actual_symbols, actual_manifests,
            actual_eligible_symbols;
    END IF;

    actual_freshness := CASE
        WHEN actual_manifests > 0 AND actual_fresh_symbols=actual_manifests THEN 'FRESH'
        WHEN actual_fresh_symbols > 0 THEN 'PARTIAL'
        WHEN actual_stale_symbols > 0 THEN 'STALE'
        ELSE 'UNKNOWN'
    END;

    IF declared_freshness IS DISTINCT FROM actual_freshness THEN
        RAISE EXCEPTION
            'Max-Pain snapshot set % has an invalid aggregate freshness claim',
            target_set_id;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM research_max_pain_snapshot_rows
        WHERE snapshot_set_id=target_set_id
          AND (
            source_observed_at_utc > available_at
            OR price_fetched_at_utc > available_at
          )
    ) THEN
        RAISE EXCEPTION
            'Max-Pain snapshot set % contains future-dated source evidence',
            target_set_id;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM research_max_pain_snapshot_symbols manifest
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS row_count,
                   COUNT(DISTINCT timeframe) AS timeframe_count,
                   COUNT(*) FILTER (WHERE NOT row_valid) AS invalid_count,
                   COUNT(*) FILTER (WHERE freshness_status='FRESH') AS fresh_count,
                   COUNT(*) FILTER (WHERE freshness_status='STALE') AS stale_count,
                   COUNT(DISTINCT ROW(
                       current_price,
                       COALESCE(price_source, ''),
                       COALESCE(price_exchange, ''),
                       COALESCE(price_market, ''),
                       COALESCE(price_pair, ''),
                       COALESCE(price_instrument, ''),
                       price_fetched_at_utc
                   )) AS price_signature_count
            FROM research_max_pain_snapshot_rows child
            WHERE child.snapshot_set_id=manifest.snapshot_set_id
              AND child.symbol=manifest.symbol
        ) actual ON TRUE
        WHERE manifest.snapshot_set_id=target_set_id
          AND (
            actual.row_count <> manifest.observed_timeframe_count
            OR actual.timeframe_count <> manifest.observed_timeframe_count
            OR actual.invalid_count <> manifest.invalid_row_count
            OR manifest.price_overlay_coherent IS DISTINCT FROM (
                actual.row_count > 0 AND actual.price_signature_count=1
            )
            OR manifest.freshness_status IS DISTINCT FROM CASE
                WHEN actual.row_count > 0 AND actual.fresh_count=actual.row_count
                    THEN 'FRESH'
                WHEN actual.stale_count > 0 THEN 'STALE'
                ELSE 'UNKNOWN'
            END
            OR EXISTS (
                SELECT 1
                FROM UNNEST(
                    ARRAY['12h','24h','48h','3d','1w','2w','1m']::text[]
                ) required(timeframe)
                WHERE (
                    EXISTS (
                        SELECT 1
                        FROM research_max_pain_snapshot_rows child_timeframe
                        WHERE child_timeframe.snapshot_set_id=manifest.snapshot_set_id
                          AND child_timeframe.symbol=manifest.symbol
                          AND child_timeframe.timeframe=required.timeframe
                    )
                ) = (required.timeframe = ANY(manifest.missing_timeframes))
            )
            OR (
                manifest.research_eligible
                AND (
                    actual.row_count <> 7
                    OR actual.timeframe_count <> 7
                    OR actual.invalid_count <> 0
                    OR actual.price_signature_count <> 1
                )
            )
          )
    ) THEN
        RAISE EXCEPTION
            'Max-Pain snapshot set % has a manifest/row coherence violation',
            target_set_id;
    END IF;

    IF is_eligible AND actual_eligible_symbols=0 THEN
        RAISE EXCEPTION
            'research-eligible Max-Pain snapshot set % has no eligible symbol',
            target_set_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_research_max_pain_set_complete
    ON research_max_pain_snapshot_sets;

CREATE CONSTRAINT TRIGGER trg_research_max_pain_set_complete
AFTER INSERT ON research_max_pain_snapshot_sets
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_research_max_pain_snapshot_complete();

DROP TRIGGER IF EXISTS trg_research_max_pain_row_complete
    ON research_max_pain_snapshot_rows;

CREATE CONSTRAINT TRIGGER trg_research_max_pain_row_complete
AFTER INSERT ON research_max_pain_snapshot_rows
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_research_max_pain_snapshot_complete();

DROP TRIGGER IF EXISTS trg_research_max_pain_symbol_complete
    ON research_max_pain_snapshot_symbols;

CREATE CONSTRAINT TRIGGER trg_research_max_pain_symbol_complete
AFTER INSERT ON research_max_pain_snapshot_symbols
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_research_max_pain_snapshot_complete();

CREATE OR REPLACE FUNCTION prevent_research_max_pain_archive_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_research_max_pain_sets_append_only
    ON research_max_pain_snapshot_sets;

CREATE TRIGGER trg_research_max_pain_sets_append_only
BEFORE UPDATE OR DELETE ON research_max_pain_snapshot_sets
FOR EACH ROW EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();

DROP TRIGGER IF EXISTS trg_research_max_pain_rows_append_only
    ON research_max_pain_snapshot_rows;

CREATE TRIGGER trg_research_max_pain_rows_append_only
BEFORE UPDATE OR DELETE ON research_max_pain_snapshot_rows
FOR EACH ROW EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();

DROP TRIGGER IF EXISTS trg_research_max_pain_symbols_append_only
    ON research_max_pain_snapshot_symbols;

CREATE TRIGGER trg_research_max_pain_symbols_append_only
BEFORE UPDATE OR DELETE ON research_max_pain_snapshot_symbols
FOR EACH ROW EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();

DROP TRIGGER IF EXISTS trg_research_max_pain_sets_no_truncate
    ON research_max_pain_snapshot_sets;

CREATE TRIGGER trg_research_max_pain_sets_no_truncate
BEFORE TRUNCATE ON research_max_pain_snapshot_sets
FOR EACH STATEMENT EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();

DROP TRIGGER IF EXISTS trg_research_max_pain_rows_no_truncate
    ON research_max_pain_snapshot_rows;

CREATE TRIGGER trg_research_max_pain_rows_no_truncate
BEFORE TRUNCATE ON research_max_pain_snapshot_rows
FOR EACH STATEMENT EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();

DROP TRIGGER IF EXISTS trg_research_max_pain_symbols_no_truncate
    ON research_max_pain_snapshot_symbols;

CREATE TRIGGER trg_research_max_pain_symbols_no_truncate
BEFORE TRUNCATE ON research_max_pain_snapshot_symbols
FOR EACH STATEMENT EXECUTE FUNCTION prevent_research_max_pain_archive_mutation();
