"""Bounded research-only inverse computation on canonical closed 1m paths."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import time
import uuid
from typing import Any, Callable

import canonical_price_path
import research_ordered_first_touch as calculator
import research_ordered_inverse_store as store


def _connect(url: str):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(url,row_factory=dict_row,connect_timeout=5,
        options='-c statement_timeout=15000 -c lock_timeout=1000')


def _claim(conn: Any, *, now: datetime, token: str) -> dict[str,Any] | None:
    row = conn.execute('''WITH due AS (
        SELECT linked_source_event_id,inverse_version FROM research_ordered_inverse_requests
        WHERE inverse_version=%s AND queue_status IN ('PENDING','RETRY','IN_FLIGHT')
          AND ((queue_status IN ('PENDING','RETRY') AND next_attempt_at_utc<=%s)
            OR (queue_status='IN_FLIGHT' AND lease_expires_at_utc<%s))
        ORDER BY next_attempt_at_utc,linked_source_event_id FOR UPDATE SKIP LOCKED LIMIT 1
    ) UPDATE research_ordered_inverse_requests q SET queue_status='IN_FLIGHT',claim_token=%s::uuid,
        lease_expires_at_utc=%s+INTERVAL '180 seconds',attempts=q.attempts+1
      FROM due WHERE q.linked_source_event_id=due.linked_source_event_id AND q.inverse_version=due.inverse_version
      RETURNING q.*''',(store.VERSION,now,now,token,now)).fetchone()
    return dict(row) if row else None


def _finish(conn: Any, job: dict[str,Any], *, now: datetime, status: str,
            result: dict[str,Any], error: str | None=None) -> None:
    seconds = 60 if result.get('OPEN') else 300
    conn.execute('''UPDATE research_ordered_inverse_requests SET queue_status=%s,
        claim_token=NULL,lease_expires_at_utc=NULL,last_checked_at_utc=%s,
        next_attempt_at_utc=%s,last_error=%s,result=%s::jsonb
        WHERE linked_source_event_id=%s AND inverse_version=%s AND claim_token=%s::uuid''',
        (status,now,now+timedelta(seconds=seconds),error,store.canonical(result),
         job['linked_source_event_id'],store.VERSION,job['claim_token']))


def run_pending(database_url: str, *, now: datetime | None=None, event_limit: int=4,
                max_seconds: float=20, fetch_candles: Callable | None=None,
                connect: Callable | None=None, outcome_writer: Callable | None=None) -> dict[str,int]:
    """Run under the existing ordered-outcome pass; never deliver Telegram.

    Source materialization and job claims commit before external price fetches.
    Each requested source produces 32 distinct canonical v7 measurements; an
    OPEN/missing path retries without rewriting valid terminal labels.
    """
    import research_outcome_worker as native
    import research_common_window_metrics as metrics
    import research_common_window_metrics_store as metric_store
    connect = connect or _connect
    fetch = fetch_candles or canonical_price_path.fetch_closed_candles
    write = outcome_writer or native.ResearchOutcomeWorker._write_ordered_first_touch_outcome
    now = store.utc(now or datetime.now(timezone.utc))
    deadline = time.monotonic()+max(1,min(float(max_seconds),45))
    summary = {'inverse_checked':0,'inverse_written':0,'inverse_rejected':0,
        'inverse_path_failures':0,'inverse_schema_missing':0,'inverse_common_window_written':0}
    with connect(database_url) as conn:
        if not store.available(conn):
            summary['inverse_schema_missing']=1
            return summary
        metrics_available = metric_store.available(conn)
    for _ in range(max(1,min(int(event_limit),16))):
        if time.monotonic()>=deadline:
            break
        with connect(database_url) as conn:
            job = _claim(conn,now=now,token=str(uuid.uuid4()))
            if not job:
                break
            source = conn.execute('SELECT * FROM research_events WHERE event_id=%s',
                                  (job['linked_source_event_id'],)).fetchone()
            try:
                if source is None:
                    raise ValueError('Original inverse source is missing')
                event = store.materialize(conn,job,dict(source))
            except ValueError as exc:
                _finish(conn,job,now=now,status='REJECTED',result={},error=str(exc))
                summary['inverse_rejected']+=1
                continue
        summary['inverse_checked']+=1
        try:
            start = store.utc(event['alert_time_utc'])
            latest = native._latest_closed_candle_cutoff(now)
            cutoff = min(start+timedelta(minutes=1440),latest)
            path_result = fetch(event['symbol'],start,cutoff)
            error = native._canonical_path_provenance_error(event['symbol'],path_result)
            if error:
                raise ValueError(error)
            full_path = list(path_result.get('candles') or ())
            outcomes = []
            common = []
            for window in store.WINDOWS:
                observed_cutoff = min(start+timedelta(minutes=window),latest)
                candles = native._candles_for_horizon(full_path,observed_cutoff)
                expected = native._expected_candles(start,observed_cutoff)
                for outcome in calculator.calculate_all_ordered_first_touch_outcomes(
                    reference_price=event['current_price'],direction=event['direction'],event_time=start,
                    candles=candles,observation_closed=now>=start+timedelta(minutes=window),
                    path_complete=len(candles)==expected):
                    outcomes.append((window,expected,outcome))
                if metrics_available:
                    common.append(metrics.calculate_common_window_metrics(symbol=event['symbol'],
                        reference_price=event['current_price'],direction=event['direction'],event_time=start,
                        window_minutes=window,candles=candles,observed_at=now,path_result=path_result))
            written = 0
            common_written = 0
            with connect(database_url) as conn:
                owned = conn.execute('''SELECT claim_token FROM research_ordered_inverse_requests
                    WHERE linked_source_event_id=%s AND inverse_version=%s AND claim_token=%s::uuid FOR UPDATE''',
                    (job['linked_source_event_id'],store.VERSION,job['claim_token'])).fetchone()
                if not owned:
                    continue
                for window,expected,outcome in outcomes:
                    if write(conn,event=event,window_minutes=window,
                        reference_source=native._snapshot_price_source(event['engine_snapshot']),
                        path_result=path_result,outcome=outcome,expected_candles=expected):
                        written+=1
                if metrics_available:
                    metric_store.seed_event(conn,event)
                    for metric in common:
                        if metric_store.write_metrics(conn,event_id=event['event_id'],metrics=metric):
                            common_written+=1
                # The canonical writer preserves valid terminal evidence even
                # when a later fetch is partial. Queue status follows persisted
                # measurements, never a downgraded recalculation attempt.
                persisted = conn.execute('''SELECT status,COUNT(*) AS count
                    FROM research_ordered_first_touch_outcomes WHERE event_id=%s
                      AND method_version='ordered-first-touch-v7' GROUP BY status''',
                    (event['event_id'],)).fetchall()
                counts = {row['status']:int(row['count']) for row in persisted}
                pending = counts.get('OPEN',0)+counts.get('DATA_MISSING',0)+max(0,32-sum(counts.values()))
                _finish(conn,job,now=now,status='RETRY' if pending else 'COMPLETE',result=counts)
            summary['inverse_written']+=written
            summary['inverse_common_window_written']+=common_written
        except Exception as exc:
            summary['inverse_path_failures']+=1
            with connect(database_url) as conn:
                _finish(conn,job,now=now,status='RETRY',result={'DATA_MISSING':32},error=str(exc)[:500])
    return summary
