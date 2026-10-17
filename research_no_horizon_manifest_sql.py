"""Pure bounded SQL builders for future manifest anchors with frozen BTC parents.

Parent JSONB text is retained by the same SELECT that hashes the complete source
row. Later source fetches use that exact pin and must still match the full-row
hash. Other source revisions remain fail-closed. No DB, filesystem or network I/O.
The collector retains SQL/raw results and attaches SHA256(SQL UTF-8) to the
anchor receipt's query_sha256 before passing a manifest to the chunk builders.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
from typing import Mapping

import research_no_horizon_manifest as transport

VERSION = "no-horizon-pinned-parent-manifest-sql-v1"
PARENT_SNAPSHOT_MODE = "ANCHOR_PARENT_JSONB_TEXT_V1"
TRANSPORT_VERSION = transport.MODE
EXPORT_VERSION = transport.EXPORT_VERSION
CONSUMER = "watch-all-scan-intake-v1"
PARENT_POLICY = "btc-parent-close-reversal-200bps-v1"
ROUTE = transport.ROUTE
SOURCE_CAP = transport.MAX_SOURCE_ROWS
CANDLE_CAP = transport.MAX_CANDLES
PAGE_SIZE = transport.MAX_PAGE_ROWS
SOURCE_PAGE_SIZE = 8
SYMBOLS = frozenset(("BTC", "ETH", "SOL", "DOGE", "ZEC", "BNB", "XRP"))


def _parent_pin(text: str) -> None:
    transport._bytes(text, transport.MAX_CHUNK_BYTES)
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate parent JSON key")
            result[key] = value
        return result
    def nonfinite(_):
        raise ValueError("nonfinite parent JSON number")
    parsed = json.loads(text, object_pairs_hook=pairs, parse_float=Decimal, parse_constant=nonfinite)
    if parsed is not None and not isinstance(parsed, dict):
        raise ValueError("parent pin must be a JSON object or null")
    pending = [parsed]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, str):
            item.encode("utf-8", errors="strict")
            if "\x00" in item:
                raise ValueError("NUL in parent JSON string")


def _pinned_manifest(manifest: Mapping) -> dict:
    frozen = transport._manifest(manifest)
    if frozen["receipt"].get("parent_snapshot_mode") != PARENT_SNAPSHOT_MODE:
        raise ValueError("pinned parent anchor required")
    for entry in frozen["source_entries"]:
        if "btc_parent_payload_text" not in entry:
            raise ValueError("missing exact parent pin")
        _parent_pin(entry["btc_parent_payload_text"])
    return frozen


def literal(value: str) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("SQL literal must be a NUL-free string")
    return "'" + value.replace("'", "''") + "'"


def timestamp(value: str) -> str:
    if not isinstance(value, (str, datetime)):
        raise ValueError("explicit timestamp required")
    return transport.contracts.utc(value).isoformat()


def digest_sql(text_expression: str) -> str:
    return f"encode(sha256(convert_to({text_expression},'UTF8')),'hex')"


def row_payload(parent_expression: str = "to_jsonb(p)") -> str:
    return f"""jsonb_build_object(
      'intake',to_jsonb(i),
      'stored_source',jsonb_build_object('snapshot_set_id',s.snapshot_set_id,
        'snapshot_key',s.snapshot_key,'payload_sha256',s.payload_sha256,
        'cycle_id',s.cycle_id,'source',s.source,
        'available_at_utc',s.available_at_utc,'created_at_utc',s.created_at_utc),
      'scores',s.source_metadata#>'{{capture_metadata,operational_scores}}',
      'btc_parent',{parent_expression},'btc_prior_bar',to_jsonb(b))"""


def row_joins(*, include_parent: bool = True) -> str:
    parent = f"""LEFT JOIN LATERAL (
    SELECT p.* FROM public.research_btc_parent_movements p
    WHERE p.episode_policy_version={literal(PARENT_POLICY)}
      AND p.start_time_utc<=i.usable_from_utc
      AND (p.end_time_utc IS NULL OR p.end_time_utc>i.usable_from_utc)
    ORDER BY p.start_time_utc DESC LIMIT 1
  ) p ON TRUE""" if include_parent else ""
    return f"""LEFT JOIN public.research_max_pain_snapshot_sets s USING(snapshot_set_id)
  {parent}
  LEFT JOIN LATERAL (
    SELECT b.* FROM public.research_btc_price_bars b
    WHERE b.open_time_utc+INTERVAL '1 minute'<=i.usable_from_utc
      AND b.close_time_utc>i.usable_from_utc-INTERVAL '1 minute'
    ORDER BY b.close_time_utc DESC LIMIT 1
  ) b ON TRUE"""


def candle_payload(alias: str = "c") -> str:
    return "jsonb_build_object(" + ",".join(
        f"'{key}',{alias}.{key}" for key in (
            "route", "symbol", "open_time_utc", "close_time_utc", "open", "high", "low", "close")) + ")"


def manifest_sql(plan: dict) -> str:
    """Build one anchor statement; byte admission remains with the assembler.

    Optional smaller row/candle/page limits are enforced in this statement.
    A declared source_byte_limit also needs the caller's final export-size check.
    """
    if not isinstance(plan, Mapping):
        raise ValueError("plan object required")
    source_cap = transport._integer(plan.get("source_row_limit", SOURCE_CAP), "source row limit", SOURCE_CAP, 1)
    candle_cap = transport._integer(plan.get("max_candle_rows", CANDLE_CAP), "candle row limit", CANDLE_CAP, 1)
    page_size = transport._integer(plan.get("page_size", PAGE_SIZE), "page size", PAGE_SIZE, 1)
    if "source_byte_limit" in plan:
        transport._integer(plan["source_byte_limit"], "source byte limit", transport.MAX_BYTES, 1)
    if plan.get("route", ROUTE) != ROUTE:
        raise ValueError("unsupported source price route")
    start, end, cutoff = (timestamp(plan[key]) for key in
        ("source_start_utc", "source_end_utc", "cutoff_utc"))
    start_dt, end_dt, cutoff_dt = (datetime.fromisoformat(value) for value in (start, end, cutoff))
    if not start_dt < end_dt <= cutoff_dt or cutoff_dt-start_dt > timedelta(days=31):
        raise ValueError("invalid bounded source/cutoff interval")
    entry = start_dt.replace(second=0, microsecond=0)
    if entry < start_dt:
        entry += timedelta(minutes=1)
    symbol = plan["symbol"]
    if not isinstance(symbol, str) or symbol not in SYMBOLS:
        raise ValueError("only supported canonical Binance Spot symbol allowed")
    return f"""WITH cfg AS MATERIALIZED (
  SELECT {literal(start)}::timestamptz AS source_start,
    {literal(end)}::timestamptz AS source_end,
    {literal(cutoff)}::timestamptz AS cutoff,
    {literal(entry.isoformat())}::timestamptz AS price_start
), source_keys AS MATERIALIZED (
  SELECT i.snapshot_set_id,i.usable_from_utc
  FROM public.research_watch_scan_intakes i CROSS JOIN cfg
  WHERE i.consumer_version={literal(CONSUMER)} AND i.intake_status='ACCEPTED'
    AND i.usable_from_utc>=cfg.source_start AND i.usable_from_utc<cfg.source_end
  ORDER BY i.usable_from_utc,i.snapshot_set_id LIMIT {source_cap+1}
), source_count AS (
  SELECT count(*)::integer AS n FROM source_keys
), source_json AS MATERIALIZED (
  SELECT (row_number() OVER(ORDER BY k.usable_from_utc,k.snapshot_set_id)-1)::integer AS ordinal,
    i.snapshot_set_id,i.usable_from_utc,{row_payload()} AS payload_json
  FROM source_keys k
  JOIN public.research_watch_scan_intakes i USING(snapshot_set_id)
  {row_joins()}
  WHERE i.consumer_version={literal(CONSUMER)} AND i.intake_status='ACCEPTED'
    AND (SELECT n FROM source_count)<={source_cap}
), source_payloads AS MATERIALIZED (
  SELECT ordinal,snapshot_set_id,usable_from_utc,payload_json::text AS payload_text,
    (payload_json->'btc_parent')::text AS btc_parent_payload_text
  FROM source_json
), source_manifest AS (
  SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',ordinal,
      'snapshot_set_id',snapshot_set_id,'usable_from_utc',usable_from_utc,
      'btc_parent_payload_text',btc_parent_payload_text,
      'sha256',{digest_sql('payload_text')},'byte_length',octet_length(convert_to(payload_text,'UTF8')))
      ORDER BY ordinal),'[]'::jsonb) AS entries
  FROM source_payloads
), prices AS MATERIALIZED (
  SELECT c.route,c.symbol,c.open_time_utc,c.close_time_utc,c.open,c.high,c.low,c.close
  FROM public.research_price_archive_bars c CROSS JOIN cfg
  WHERE c.route={literal(ROUTE)} AND c.symbol={literal(symbol)}
    AND c.open_time_utc>=cfg.price_start
    AND c.open_time_utc+INTERVAL '1 minute'<=cfg.cutoff
  ORDER BY c.open_time_utc LIMIT {candle_cap+1}
), candle_count AS (
  SELECT count(*)::integer AS n FROM prices
), numbered_prices AS (
  SELECT prices.*,((row_number() OVER(ORDER BY open_time_utc)-1)/{page_size})::integer AS page_ordinal
  FROM prices WHERE (SELECT n FROM candle_count)<={candle_cap}
), price_payloads AS MATERIALIZED (
  SELECT c.page_ordinal AS ordinal,min(c.open_time_utc) AS first_open_utc,
    max(c.open_time_utc) AS last_open_utc,count(*)::integer AS row_count,
    jsonb_agg({candle_payload()} ORDER BY c.open_time_utc)::text AS payload_text
  FROM numbered_prices c GROUP BY c.page_ordinal
), candle_manifest AS (
  SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',ordinal,
    'first_open_utc',first_open_utc,'last_open_utc',last_open_utc,'row_count',row_count,
    'sha256',{digest_sql('payload_text')},'byte_length',octet_length(convert_to(payload_text,'UTF8')))
    ORDER BY ordinal),'[]'::jsonb) AS pages FROM price_payloads
)
SELECT jsonb_build_object(
  'transport_version',{literal(TRANSPORT_VERSION)},'export_version',{literal(EXPORT_VERSION)},
  'symbol',{literal(symbol)},'route',{literal(ROUTE)},
  'source_start_utc',cfg.source_start,'source_end_utc',cfg.source_end,'cutoff_utc',cfg.cutoff,
  'source_entries',source_manifest.entries,'candle_pages',candle_manifest.pages,
  'receipt',jsonb_build_object(
    'transaction_mode','SINGLE_STATEMENT_READ_ONLY','read_only',current_setting('transaction_read_only'),
    'mvcc_snapshot',pg_current_snapshot()::text,'extracted_at_utc',statement_timestamp(),
    'query_identity','no-horizon-pinned-parent-manifest-select-v2',
    'parent_snapshot_mode',{literal(PARENT_SNAPSHOT_MODE)},
    'serializer','POSTGRESQL_JSONB_TEXT_UTF8_V1','serializer_timezone',current_setting('TimeZone'),
    'source_count',source_count.n,'candle_count',candle_count.n,
    'source_count_capped_plus_one',source_count.n,'candle_count_capped_plus_one',candle_count.n,
    'source_cap',{source_cap},'candle_cap',{candle_cap},'page_size',{page_size},
    'source_overflow',source_count.n>{source_cap},'candle_overflow',candle_count.n>{candle_cap},
    'rows_complete',source_count.n<={source_cap},'candles_complete',candle_count.n<={candle_cap},
    'truncated',source_count.n>{source_cap} OR candle_count.n>{candle_cap},
    'database_writes',false)) AS manifest
FROM cfg CROSS JOIN source_manifest CROSS JOIN candle_manifest
CROSS JOIN source_count CROSS JOIN candle_count;
"""


def source_chunk_sql(manifest: Mapping, ordinals) -> str:
    """Fetch exact source IDs using only the parent fragments in this anchor."""
    frozen = _pinned_manifest(manifest)
    if not isinstance(ordinals, (list, tuple)) or not 1 <= len(ordinals) <= SOURCE_PAGE_SIZE:
        raise ValueError("source query requires 1 through 8 exact manifest ordinals")
    for ordinal in ordinals:
        transport._integer(ordinal, "requested source ordinal", len(frozen["source_entries"])-1)
    if len(set(ordinals)) != len(ordinals):
        raise ValueError("duplicate source request")
    entries = [frozen["source_entries"][ordinal] for ordinal in sorted(ordinals)]
    values = ",".join(
        f"({entry['ordinal']}::integer,{entry['snapshot_set_id']}::bigint,"
        f"convert_from(decode('{entry['btc_parent_payload_text'].encode('utf-8').hex()}',"
        "'hex'),'UTF8')::jsonb)" for entry in entries)
    return f"""WITH requested(ordinal,snapshot_set_id,btc_parent) AS (VALUES {values}),
payloads AS MATERIALIZED (
  SELECT requested.ordinal,i.snapshot_set_id,{row_payload("requested.btc_parent")}::text AS payload_text
  FROM requested JOIN public.research_watch_scan_intakes i USING(snapshot_set_id)
  {row_joins(include_parent=False)}
  WHERE i.consumer_version={literal(CONSUMER)} AND i.intake_status='ACCEPTED'
  ORDER BY requested.ordinal LIMIT {SOURCE_PAGE_SIZE+1}
)
SELECT jsonb_build_object('kind','source','requested_count',{len(entries)},
  'returned_count',(SELECT count(*) FROM payloads),'overflow',(SELECT count(*)>{len(entries)} FROM payloads),
  'entries',coalesce((SELECT jsonb_agg(jsonb_build_object('ordinal',ordinal,
    'snapshot_set_id',snapshot_set_id,'payload_text',payload_text,
    'sha256',{digest_sql('payload_text')},'byte_length',octet_length(convert_to(payload_text,'UTF8')))
    ORDER BY ordinal) FROM payloads),'[]'::jsonb),
  'receipt',jsonb_build_object('read_only',current_setting('transaction_read_only'),
    'mvcc_snapshot',pg_current_snapshot()::text,'fetched_at_utc',statement_timestamp(),
    'serializer_timezone',current_setting('TimeZone'),
    'parent_snapshot_mode',{literal(PARENT_SNAPSHOT_MODE)},
    'query_identity','no-horizon-pinned-parent-source-chunk-select-v2','database_writes',false)) AS chunk;
"""


def candle_chunk_sql(manifest: Mapping, pageordinal: int) -> str:
    """Fetch one existing bounded candle page; the price payload is unchanged."""
    manifest = _pinned_manifest(manifest)
    transport._integer(pageordinal, "requested candle ordinal", len(manifest["candle_pages"])-1)
    page = manifest["candle_pages"][pageordinal]
    page_size = manifest["receipt"]["page_size"]
    first, last, cutoff = (timestamp(value) for value in
        (page["first_open_utc"], page["last_open_utc"], manifest["cutoff_utc"]))
    if not datetime.fromisoformat(first) <= datetime.fromisoformat(last) < datetime.fromisoformat(cutoff):
        raise ValueError("invalid frozen candle bounds")
    if manifest.get("route") != ROUTE or manifest.get("symbol") not in ("BTC", "ETH", "SOL", "DOGE", "ZEC", "BNB", "XRP"):
        raise ValueError("invalid frozen candle route")
    return f"""WITH prices AS MATERIALIZED (
  SELECT c.route,c.symbol,c.open_time_utc,c.close_time_utc,c.open,c.high,c.low,c.close
  FROM public.research_price_archive_bars c
  WHERE c.route={literal(ROUTE)} AND c.symbol={literal(manifest['symbol'])}
    AND c.open_time_utc>={literal(first)}::timestamptz
    AND c.open_time_utc<={literal(last)}::timestamptz
    AND c.open_time_utc+INTERVAL '1 minute'<={literal(cutoff)}::timestamptz
  ORDER BY c.open_time_utc LIMIT {page_size+1}
), payload AS (
  SELECT count(*)::integer AS row_count,
    coalesce(jsonb_agg({candle_payload()} ORDER BY c.open_time_utc),'[]'::jsonb)::text AS payload_text
  FROM prices c
)
SELECT jsonb_build_object('kind','candles','ordinal',{page['ordinal']},
  'row_count',payload.row_count,'overflow',payload.row_count>{page_size},
  'payload_text',payload.payload_text,'sha256',{digest_sql('payload.payload_text')},
  'byte_length',octet_length(convert_to(payload.payload_text,'UTF8')),
  'receipt',jsonb_build_object('read_only',current_setting('transaction_read_only'),
    'mvcc_snapshot',pg_current_snapshot()::text,'fetched_at_utc',statement_timestamp(),
    'serializer_timezone',current_setting('TimeZone'),
    'query_identity','no-horizon-exact-candle-chunk-select-v1','database_writes',false)) AS chunk
FROM payload;
"""
