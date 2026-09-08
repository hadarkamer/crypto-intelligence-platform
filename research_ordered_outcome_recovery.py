"""Review/enqueue exact native alert IDs, or run one bounded locked catch-up.

No source event, price provenance, threshold, or terminal outcome is edited.
--apply is required for a mutation. The normal deployed worker also discovers
Max Pain score>=65 alerts automatically; this helper makes an incident cohort
reviewable and allows faster one-pass recovery without changing service config.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json

import research_ordered_outcome_recovery_store as store


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event-ids',help='Comma-separated immutable source IDs, at most1000')
    parser.add_argument('--as-of',help='Explicit timezone-qualified request cutoff')
    parser.add_argument('--request-key',default='manual-outcome-recovery')
    parser.add_argument('--priority',type=int,choices=(0,1,2),default=1)
    parser.add_argument('--run-once',action='store_true')
    parser.add_argument('--event-limit',type=int,default=8,choices=range(1,65))
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args(argv)
    if not args.event_ids and not args.run_once:
        parser.error('Provide --event-ids or --run-once')
    if args.event_ids and not args.as_of:
        parser.error('--event-ids requires --as-of')
    import research_outcome_worker as worker
    import psycopg
    from psycopg.rows import dict_row
    url=worker._database_url()
    if not url:
        raise RuntimeError('Research database is not configured')
    result={'apply':args.apply,'run_once':args.run_once,'event_limit':args.event_limit}
    if args.event_ids:
        ids=store.normalize_ids(args.event_ids.split(','))
        cutoff=store.utc(args.as_of)
        if cutoff>datetime.now(timezone.utc):
            parser.error('--as-of cannot be in the future')
        with psycopg.connect(url,row_factory=dict_row,connect_timeout=5,
                options='-c statement_timeout=15000 -c lock_timeout=1000') as conn:
            result['sources']=conn.execute("""SELECT event_id,symbol,direction,event_type,score,
                event_kind,delivery_status,alert_time_utc FROM research_events
                WHERE event_id=ANY(%s::bigint[]) ORDER BY event_id""",(ids,)).fetchall()
            result['requested_through_utc']=cutoff
            if args.apply:
                result['enqueued']=store.enqueue(conn,ids,as_of=cutoff,request_key=args.request_key,priority=args.priority)
    if args.run_once and args.apply:
        result['pass']=worker.ResearchOutcomeWorker()._run_ordered_first_touch_once(
            url,event_limit=args.event_limit)
    print(json.dumps(result,default=str,ensure_ascii=False))


if __name__=='__main__':
    main()
