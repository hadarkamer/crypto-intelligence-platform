"""Private entry for the frozen numerical runtime. It never starts an HTTP server."""
import asyncio
import json
import os
import signal
import sys
from uuid import UUID, uuid4


def canonical_status(connection, cycle_id=None):
    """Read the canonical slot's metadata. Do not acquire a lease or retrieve account material."""
    from dedicated_auth_state import incident_status
    from session_runtime import ManagedSession
    from session_state import WORK_LOCK
    managed = ManagedSession.from_environment(connection,str(uuid4()))
    with connection.transaction():
        row = connection.execute("""
            SELECT s.status,s.encrypted_payload IS NOT NULL AS snapshot,
                EXISTS(SELECT 1 FROM collector_private.coinglass_dedicated_auth a
                    WHERE a.slot=s.slot AND a.phase='verified' AND a.verified_at IS NOT NULL) AS proved,
                EXISTS(SELECT 1 FROM public.ai_collection_bridge_jobs j
                    WHERE j.id=s.run_id AND j.fixed_cycle_id=%s::uuid) AS same_cycle,
                EXISTS(SELECT 1 FROM public.ai_collection_bridge_jobs h
                    JOIN pg_stat_activity a ON a.application_name='coinglass-fixed-'||h.id::text
                    JOIN pg_locks l ON l.pid=a.pid AND l.locktype='advisory' AND l.granted
                    AND l.mode='ExclusiveLock' AND l.classid=0 AND l.objid=%s AND l.objsubid=1
                    AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
                    WHERE h.id=s.run_id AND h.fixed_cycle_id IS NOT NULL) AS active_fixed,
                EXISTS(SELECT 1 FROM collector_private.coinglass_dedicated_auth next
                    JOIN collector_private.coinglass_dedicated_auth previous
                    ON previous.superseded_by=next.id AND previous.slot=next.slot
                    WHERE next.slot=s.slot AND previous.phase='superseded'
                    AND next.phase='retry' AND next.attempts=0
                    AND (next.retry_after IS NULL OR next.retry_after<=clock_timestamp())) AS acknowledged,
                CASE WHEN s.map_retry_after='infinity'::timestamptz THEN 86400
                    ELSE greatest(0,ceil(extract(epoch FROM s.map_retry_after-clock_timestamp())))::bigint END AS cooldown
            FROM collector_private.coinglass_dedicated_sessions s WHERE s.slot=%s""",
            (cycle_id,WORK_LOCK,managed.store.slot)).fetchone()
    if row and row['cooldown']:
        return {'code':'source_rate_limited','retry_after_seconds':row['cooldown']}
    incident = incident_status(connection,managed.store.slot)
    if incident['status']=='waiting' and incident['reason']!='auth_retry_pending':
        return {'code':'source_login_required'}
    if row and row['status']=='in_use' and not row['same_cycle']:
        return {'code':'map_in_progress','active_fixed_cycle':bool(row['active_fixed'])}
    if (row and row['acknowledged'] and incident['reason']=='auth_retry_pending'
            and os.environ.get('COINGLASS_LOGIN_PASSWORD')):
        # Only the official operator action can create this successor. Admission
        # grants no form reservation: auth_start/credentials/submit remain authoritative.
        return {'code':'ready'}
    if row and row['same_cycle'] and incident['status']=='in_progress':
        return {'code':'ready'}
    if incident['status'] in ('in_progress','waiting'):
        return {'code':'source_login_required'}
    if row and row['snapshot'] and row['proved'] and (row['status']=='ready' or row['same_cycle']):
        return {'code':'ready'}
    return {'code':'source_login_required'}


async def main(args):
    from collection_bridge import JobStore
    store = JobStore(os.environ['DATABASE_URL'])
    if args and args[0]=='status' and len(args) <= 2:
        group = str(UUID(args[1])) if len(args)==2 else None
        with store.connect() as connection:
            with connection.transaction():
                connection.execute('SET TRANSACTION READ ONLY')
                status = canonical_status(connection,group)
        print(json.dumps(status,separators=(',',':')),flush=True)
        return
    if args != ['run']:
        raise ValueError('invalid_operation')
    from modern_map_cycle import run_scoped_cycle
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    await run_scoped_cycle(store)


if __name__ == '__main__':
    try:
        asyncio.run(main(sys.argv[1:]))
    except (KeyboardInterrupt,asyncio.CancelledError):
        pass
    except Exception:
        # Connection strings, raw provider replies and account details are never printed.
        sys.exit(1)
