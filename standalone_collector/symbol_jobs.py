"""Symbol identity and explicit attempts for the standalone collector's existing ledger."""
from __future__ import annotations

from datetime import datetime
import hashlib
import math
import os
import re
from uuid import uuid4

from heatmap_models import schema_version, source_url

MAX_ATTEMPTS = 3
MEASURE_SCHEMA = 'coinglass.right-edge-measure.v1'
SYMBOL = re.compile(r'[A-Z0-9]{1,16}\Z')
RETRYABLE_ERRORS = frozenset({
    'source_capture_failed', 'source_timeout', 'source_not_readable', 'source_login_required',
    'session_observation_failed', 'session_transport_failed', 'session_snapshot_failed',
    'session_conflict', 'analysis_timeout', 'analysis_rate_limited', 'analysis_http_error',
    'scan_timeout', 'interrupted', 'evidence_store_failed', 'worker_crashed', 'scan_failed',
})


def modern_enabled():
    return os.getenv('COINGLASS_MODERN_UI_ENABLED', '').lower() == 'true'


def verified_symbols():
    raw = os.getenv('COINGLASS_VERIFIED_SYMBOLS', 'BTC')
    symbols = [item.strip() for item in raw.split(',')]
    if not symbols or not all(SYMBOL.fullmatch(item) for item in symbols):
        from collection_bridge import BridgeError
        raise BridgeError('invalid_symbol_configuration', 'Invalid configured CoinGlass symbols', 503)
    return frozenset(symbols)


def verify_symbol(value):
    if not isinstance(value, str) or value not in verified_symbols():
        from collection_bridge import BridgeError
        raise BridgeError('unsupported_symbol', 'Unconfigured CoinGlass symbol')
    return value


def verify_attempt(value):
    from collection_bridge import BridgeError
    if type(value) is not int or not 1 <= value <= MAX_ATTEMPTS:
        raise BridgeError('invalid_attempt', 'Attempt must be between 1 and 3')
    return value


def request_dimensions(body):
    from collection_bridge import BridgeError, request_fields
    if not isinstance(body, dict) or set(body) - {'request_id', 'timeframe', 'symbol', 'attempt'}:
        raise BridgeError('invalid_request', 'Invalid CoinGlass job fields')
    rid, timeframe = request_fields({key: body[key] for key in ('request_id', 'timeframe') if key in body})
    if 'symbol' not in body and 'attempt' not in body:
        return (rid, timeframe)
    return (rid, timeframe, verify_symbol(body.get('symbol', 'BTC')), verify_attempt(body.get('attempt', 1)))


def initialize_dimensions(connection):
    connection.execute("""ALTER TABLE public.ai_collection_bridge_jobs
        ADD COLUMN IF NOT EXISTS symbol text NOT NULL DEFAULT 'BTC',
        ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 1 CHECK (attempt BETWEEN 1 AND 3),
        ADD COLUMN IF NOT EXISTS logical_request_id uuid""")
    connection.execute('DROP INDEX IF EXISTS public.ai_collection_bridge_one_active_model_tf')
    # Explicit cycle IDs have separate queued work; the existing global worker still runs just one.
    connection.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_active_shared_map
        ON public.ai_collection_bridge_jobs(symbol,heatmap_model,timeframe)
        WHERE logical_request_id IS NULL AND status IN ('queued','running')""")
    connection.execute("""CREATE INDEX IF NOT EXISTS ai_collection_bridge_ready_map
        ON public.ai_collection_bridge_jobs(symbol,heatmap_model,timeframe,finished_at DESC)
        WHERE status='ready'""")


def _continue_attempt(connection, prior, rid, attempt):
    from collection_bridge import BridgeError
    current = prior['attempt']
    if attempt <= current or prior['status'] == 'ready':
        return prior
    if (attempt != current + 1 or prior['status'] != 'failed'
            or str(prior['logical_request_id']) != rid or prior['error_code'] not in RETRYABLE_ERRORS):
        raise BridgeError('invalid_retry', 'Cannot advance the requested attempt', 409)
    if prior['error_code'] == 'source_login_required' and os.getenv('COINGLASS_SESSION_STORE_ENABLED', '').lower() == 'true':
        confirmed = connection.execute("""SELECT 1 FROM collector_private.coinglass_login_jobs j
            JOIN collector_private.coinglass_login_incidents i ON i.id=j.incident_id
            WHERE j.job_id=%s AND i.resolved_at IS NOT NULL AND i.resolved_revision IS NOT NULL""",
            (prior['id'],)).fetchone()
        if not confirmed:
            raise BridgeError('login_pending', 'The collector is waiting for verified operator sign-in', 409)
    connection.execute('DELETE FROM public.ai_collection_bridge_evidence WHERE job_id=%s', (prior['id'],))
    return connection.execute("""UPDATE public.ai_collection_bridge_jobs
        SET status='queued',attempt=%s,started_at=NULL,finished_at=NULL,
            result=NULL,image_png=NULL,error_code=NULL
        WHERE id=%s AND status='failed' AND attempt=%s RETURNING *""",
        (attempt, prior['id'], current)).fetchone()


def start_job(store, rid, timeframe, symbol=None, attempt=1):
    from collection_bridge import BridgeError, CREATE_LOCK, envelope
    explicit = symbol is not None
    symbol = verify_symbol('BTC' if symbol is None else symbol)
    verify_attempt(attempt)
    if symbol != 'BTC' and not modern_enabled():
        raise BridgeError('unsupported_symbol', 'This symbol requires the modern CoinGlass map')
    with store.connect() as connection:
        connection.execute('SELECT pg_advisory_xact_lock(%s)', (CREATE_LOCK,))
        prior = connection.execute("""SELECT j.* FROM public.ai_collection_bridge_requests r
            JOIN public.ai_collection_bridge_jobs j ON j.id=r.job_id WHERE r.id=%s
            FOR UPDATE OF j""", (rid,)).fetchone()
        if prior:
            if (prior['symbol'], prior['heatmap_model'], prior['timeframe']) != (symbol, store.heatmap_model, timeframe):
                raise BridgeError('idempotency_conflict', 'Request id belongs to another CoinGlass map', 409)
            return envelope(_continue_attempt(connection, prior, rid, attempt))
        if attempt != 1:
            raise BridgeError('invalid_retry', 'A new request must begin at attempt 1', 409)
        connection.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
            error_code='interrupted',finished_at=now() WHERE status='running'
            AND started_at < now()-interval '15 minutes'""")
        connection.execute("""UPDATE public.ai_collection_bridge_jobs SET image_png=NULL
            WHERE COALESCE(finished_at,created_at)<now()-interval '6 hours' AND image_png IS NOT NULL""")
        connection.execute("""DELETE FROM public.ai_collection_bridge_jobs
            WHERE COALESCE(finished_at,created_at)<now()-interval '7 days' AND status IN ('ready','failed')
            AND NOT (logical_request_id IS NOT NULL AND status='failed' AND attempt<3
                     AND error_code='source_login_required')""")
        row = None
        if not explicit:
            # Legacy callers share work and a short cache. They never attach to another cycle's attempt.
            row = connection.execute("""SELECT * FROM public.ai_collection_bridge_jobs
                WHERE symbol=%s AND heatmap_model=%s AND timeframe=%s AND logical_request_id IS NULL
                AND (status IN ('queued','running') OR (status='ready'
                    AND result->>'selection_policy'='prominent_right_edge.v1'
                    AND CASE WHEN %s THEN result->'measure_snapshot'->>'schema'=%s
                             ELSE result->'hover_snapshot'->>'version'='displayed-hover.v1' END
                    AND (result->>'captured_at')::timestamptz BETWEEN now()-interval '15 minutes' AND now()))
                ORDER BY created_at DESC LIMIT 1""",
                (symbol, store.heatmap_model, timeframe, modern_enabled(), MEASURE_SCHEMA)).fetchone()
        if row is None:
            row = connection.execute("""INSERT INTO public.ai_collection_bridge_jobs
                (id,timeframe,status,heatmap_model,symbol,logical_request_id)
                VALUES(%s,%s,'queued',%s,%s,%s) RETURNING *""",
                (str(uuid4()), timeframe, store.heatmap_model, symbol, rid if explicit else None)).fetchone()
        connection.execute("""INSERT INTO public.ai_collection_bridge_requests
            (id,timeframe,job_id,heatmap_model) VALUES(%s,%s,%s,%s)""",
            (rid, timeframe, row['id'], store.heatmap_model))
        return envelope(row)


def latest_job(store, timeframe, symbol='BTC'):
    from collection_bridge import envelope
    with store.connect() as connection:
        row = connection.execute("""SELECT id,timeframe,status,result,error_code,heatmap_model,
                   symbol,attempt,logical_request_id FROM public.ai_collection_bridge_jobs
            WHERE symbol=%s AND heatmap_model=%s AND timeframe=%s AND status='ready'
              AND result IS NOT NULL
              AND (result->>'captured_at')::timestamptz BETWEEN now()-interval '6 hours' AND now()
            ORDER BY (result->>'captured_at')::timestamptz DESC,finished_at DESC LIMIT 1""",
            (verify_symbol(symbol), store.heatmap_model, timeframe)).fetchone()
    return envelope(row) if row else None


def get_job(store, jid, symbol='BTC'):
    from collection_bridge import envelope
    with store.connect() as connection:
        row = connection.execute("""SELECT id,timeframe,status,result,error_code,heatmap_model,
                   symbol,attempt,logical_request_id FROM public.ai_collection_bridge_jobs
            WHERE id=%s AND heatmap_model=%s AND symbol=%s""",
            (jid, store.heatmap_model, verify_symbol(symbol))).fetchone()
    return envelope(row) if row else None


def get_evidence(store, jid, symbol='BTC'):
    with store.connect() as connection:
        row = connection.execute("""SELECT COALESCE(j.image_png,e.image_png) AS image_png
            FROM public.ai_collection_bridge_jobs j
            LEFT JOIN public.ai_collection_bridge_evidence e ON e.job_id=j.id
            WHERE j.id=%s AND j.heatmap_model=%s AND j.symbol=%s AND j.status IN ('ready','failed')
              AND COALESCE(j.finished_at,j.created_at)>now()-interval '6 hours'""",
            (jid, store.heatmap_model, verify_symbol(symbol))).fetchone()
    return bytes(row['image_png']) if row and row['image_png'] else None


def _aware_timestamp(value):
    if not isinstance(value, str):
        return False
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).tzinfo is not None
    except ValueError:
        return False


def valid_result(result, image, jid, timeframe, model, symbol, modern):
    if not isinstance(result, dict):
        return False
    evidence = result.get('evidence')
    digest = hashlib.sha256(image).hexdigest()
    if (not isinstance(evidence, dict) or evidence.get('sha256') != digest
            or evidence.get('artifact_id') != jid or evidence.get('content_type') != 'image/png'
            or result.get('schema_version') != schema_version(model)
            or type(result.get('heatmap_model')) is not int or result['heatmap_model'] != model
            or result.get('symbol') != symbol or result.get('timeframe') != timeframe
            or result.get('run_id') != jid or result.get('selection_policy') != 'prominent_right_edge.v1'):
        return False
    if not modern:
        snapshot = result.get('hover_snapshot')
        return (symbol == 'BTC' and result.get('source_url') == source_url(model)
                and isinstance(snapshot, dict) and snapshot.get('version') == 'displayed-hover.v1'
                and 'measure_snapshot' not in result)
    url = f'https://www.coinglass.com/LiquidationHeatMap/{symbol}?mode=coin'
    measure = result.get('measure_snapshot')
    expected = {'schema': MEASURE_SCHEMA, 'symbol': symbol, 'heatmap_model': model,
                'timeframe': timeframe, 'source_url': url, 'captured_at': result.get('captured_at'),
                'artifact_id': jid, 'sha256': digest, 'mode': 'Coin', 'lens': 'Heat',
                'palette': 'Viridis', 'theme': 'dark', 'intensity': [0, 100], 'count': 'Right edge'}
    return (result.get('source_url') == url and 'hover_snapshot' not in result
            and result.get('level_contract_version') == 'prominent-rows.v1'
            and result.get('price_reference_type') == 'last_price_label'
            and type(result.get('observed_price')) in (float, int)
            and math.isfinite(result['observed_price']) and result['observed_price'] > 0
            and _aware_timestamp(result.get('source_updated_at')) and _aware_timestamp(result.get('captured_at'))
            and isinstance(measure, dict) and type(measure.get('heatmap_model')) is int
            and all(measure.get(key) == val for key, val in expected.items())
            and measure.get('source_time') == result['source_updated_at'] == measure.get('source_time_after')
            and _aware_timestamp(measure.get('column_time')) and measure['column_time'] == measure.get('column_time_after'))
