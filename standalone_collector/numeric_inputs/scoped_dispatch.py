"""Durable, bounded dispatch for numerical cycles on their existing queue rows."""
import json
from uuid import uuid4

from psycopg import errors
from psycopg.types.json import Jsonb

DELAY_SECONDS = 10
MAX_LAUNCHES = 2
WORK_LOCK = 748230102

# q is a numerical job. This test never reads the evidence table: that table
# can itself be the reason for the hold. The numerical child already owns its
# reservation, so the transient running state is deliberately eligible here.
def not_stopped(alias):
    # alias is a query-local identifier, supplied only by the fixed SQL statements below and their callers.
    return f"""NOT EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs guard
        WHERE guard.fixed_cycle_id={alias}.fixed_cycle_id AND guard.fixed_worker_guard->>'state'='stopped')"""

SCOPE_OPEN = """NOT EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs guard
    WHERE guard.fixed_cycle_id=q.fixed_cycle_id AND (guard.fixed_worker_guard->>'state'='stopped'
        OR guard.fixed_worker_guard->>'state'='backoff'
        AND (guard.fixed_worker_guard->>'due')::numeric>extract(epoch FROM clock_timestamp())))"""

PICK = """SELECT a.id,a.fixed_cycle_id,a.fixed_worker_guard AS guard,
        extract(epoch FROM clock_timestamp())::double precision AS now
    FROM public.ai_collection_bridge_jobs a
    WHERE a.fixed_cycle_id IS NOT NULL AND a.id=(SELECT h.id FROM public.ai_collection_bridge_jobs h
        WHERE h.fixed_cycle_id=a.fixed_cycle_id ORDER BY h.created_at,h.id LIMIT 1)
    AND coalesce(a.fixed_worker_guard->>'state','')<>'stopped'
    AND coalesce((a.fixed_worker_guard->>'due')::numeric,0)<=extract(epoch FROM clock_timestamp())
    AND NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
        AND mode='ExclusiveLock' AND classid=0 AND objid=%s AND objsubid=1
        AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))
    AND EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs q WHERE q.fixed_cycle_id=a.fixed_cycle_id
        AND q.status IN ('queued','running')
        AND (q.status='running' OR q.symbol='BTC' AND q.heatmap_model=1 AND q.timeframe='12H'
            OR q.created_at<clock_timestamp()-interval '350 seconds' OR EXISTS (
                SELECT 1 FROM public.ai_collection_bridge_jobs h WHERE h.fixed_cycle_id=q.fixed_cycle_id
                AND h.symbol='BTC' AND h.heatmap_model=1 AND h.timeframe='12H' AND h.status IN ('ready','failed')))
        AND NOT EXISTS (SELECT 1 FROM public.ai_collection_bridge_jobs legacy
            WHERE legacy.fixed_cycle_id IS NULL AND legacy.status='queued'
            AND (legacy.created_at,legacy.id)<(q.created_at,q.id)))
    ORDER BY a.created_at,a.id LIMIT 1"""


def evidence_available(connection):
    row = connection.execute("""SELECT coalesce(has_table_privilege(
        to_regclass('public.ai_collection_bridge_evidence'),'SELECT'),false) AS available""").fetchone()
    return row['available']


def progress(connection, group):
    # Ignore enqueue-only traffic and re-created start timestamps. Persist
    # independent, monotone milestones instead of treating another lease as new work.
    jobs = connection.execute("""SELECT id::text,status FROM public.ai_collection_bridge_jobs
        WHERE fixed_cycle_id=%s AND status<>'queued'""", (group,)).fetchall()
    ranks = {r['id']: (4 if r['status'] in ('ready','failed') else 1) for r in jobs}
    if evidence_available(connection):
        try:
            with connection.transaction():
                receipts = connection.execute("""SELECT j.id::text,
                    CASE WHEN e.image_png IS NOT NULL AND e.diagnostic ? 'measured_btc_v1' THEN 3 ELSE 2 END AS rank
                    FROM public.ai_collection_bridge_jobs j JOIN public.ai_collection_bridge_evidence e ON e.job_id=j.id
                    WHERE j.fixed_cycle_id=%s AND (e.image_png IS NOT NULL AND e.diagnostic ? 'measured_btc_v1'
                        OR e.diagnostic ? 'measured_btc_partial_v1')""",(group,)).fetchall()
        except (errors.UndefinedTable, errors.UndefinedColumn, errors.InsufficientPrivilege):
            receipts = []
        for row in receipts:
            ranks[row['id']] = max(ranks.get(row['id'],0),row['rank'])
    return ranks


def advanced(before, after):
    return any(rank>before.get(job,0) for job,rank in after.items())


def store_guard(connection, anchor, guard):
    connection.execute("UPDATE public.ai_collection_bridge_jobs SET fixed_worker_guard=%s WHERE id=%s",
                       (Jsonb(guard) if guard is not None else None,anchor))


def reserve(store):
    with store.connect() as connection:
        anchor = connection.execute(PICK+' FOR UPDATE OF a SKIP LOCKED',(WORK_LOCK,)).fetchone()
        if not anchor:
            return None
        group = str(anchor['fixed_cycle_id'])
        guard = anchor['guard'] or {}
        before = progress(connection,group)
        attempts = guard.get('attempts',0)
        if advanced(guard.get('milestones',{}),before):
            attempts = 0
        if attempts>=MAX_LAUNCHES:
            # A supervisor may have exited after reserving its last launch.
            # Never discard the reservation just because its process vanished.
            reason = {'reason':'supervisor_exit_before_receipt','last_completed_stage':'dispatch_reserved'}
            guard.update(state='stopped',stopped_at=anchor['now'],due=0,last=reason)
            store_guard(connection,anchor['id'],guard)
            print_event(group,guard)
            return None
        # Keep high-water marks if an evidence table is temporarily unreadable.
        milestone=dict(guard.get('milestones',{}))
        for job,value in before.items():
            milestone[job]=max(value,milestone.get(job,0))
        token=str(uuid4())
        guard.update(state='running',anchor_job_id=str(anchor['id']),attempts=attempts+1,due=anchor['now']+DELAY_SECONDS,
                     token=token,milestones=milestone)
        store_guard(connection,anchor['id'],guard)
        return str(anchor['id']),group,token


def print_event(group,guard):
    # last is bounded by the launch protocol. Never include exception messages or DB statements.
    print('COINGLASS_FIXED_DISPATCH '+json.dumps(dict(cycle_id=str(group),anchor_job_id=guard['anchor_job_id'],state=guard['state'],
          attempts=guard['attempts'],**guard.get('last',{})),separators=(',',':')),flush=True)


def settle(store,reservation,receipt):
    anchor,group,token=reservation
    with store.connect() as connection:
        row=connection.execute("""SELECT fixed_worker_guard AS guard,
            extract(epoch FROM clock_timestamp())::double precision AS now
            FROM public.ai_collection_bridge_jobs WHERE id=%s FOR UPDATE""",(anchor,)).fetchone()
        if not row or not row['guard'] or row['guard'].get('token')!=token:
            return
        guard=row['guard']
        observed=progress(connection,group)
        complete=not connection.execute(
                "SELECT 1 FROM public.ai_collection_bridge_jobs WHERE fixed_cycle_id=%s AND status IN ('queued','running') LIMIT 1",
                (group,)).fetchone()
        if complete or advanced(guard['milestones'],observed):
            credit=guard.get('paused_seconds',0)
            store_guard(connection,anchor,{'state':'eligible','paused_seconds':credit} if credit and not complete else None)
            if receipt.get('returncode',0)!=0:
                print_event(group,dict(state='advanced',anchor_job_id=anchor,attempts=guard['attempts'],last=receipt))
            return
        if receipt.get('returncode')==0 and receipt['reason']=='owner_busy':
            guard['attempts']-=1
        guard.update(last=receipt,state='stopped' if guard['attempts']>=MAX_LAUNCHES else 'backoff')
        if guard['state']=='stopped':
            guard['stopped_at']=row['now']
        guard['due']=0 if guard['state']=='stopped' else row['now']+DELAY_SECONDS
        store_guard(connection,anchor,guard)
        print_event(group,guard)


def resume(connection,group):
    # This is an operator action on one stopped cycle, independent of receipt access.
    # It does not launch, requeue terminal jobs, or change any upstream account hold.
    row=connection.execute("""UPDATE public.ai_collection_bridge_jobs SET fixed_worker_guard=jsonb_build_object(
            'state','eligible','paused_seconds',
            coalesce((fixed_worker_guard->>'paused_seconds')::numeric,0)+
            greatest(0,extract(epoch FROM clock_timestamp())-(fixed_worker_guard->>'stopped_at')::numeric))
            WHERE fixed_cycle_id=%s AND fixed_worker_guard->>'state'='stopped' RETURNING id""",(group,)).fetchone()
    return bool(row)
