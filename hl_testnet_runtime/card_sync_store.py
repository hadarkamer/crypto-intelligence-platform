"""Persistent read-only observation worker state beside existing lifecycle tables.

All writes are to shadow observation tables, never trading journals or alert cards.
Per-bucket expiring claims fence deployments; evidence and heartbeat commit together.
Unchanged facts update a small heartbeat, not an ever-growing full history copy.
"""
from copy import deepcopy
import uuid
from . import card_lifecycle as life
from .card_lifecycle_store import LifecycleStore, LifecycleStorageError, SCHEMA, _continues
from .card_sync_evidence import facts

TABLE = SCHEMA+'.sync_state'


class SyncStore:
    def __init__(self,journal): self.journal=journal

    def initialize(self):
        LifecycleStore(self.journal).initialize()
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)',(1729048230,))
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
                bucket text PRIMARY KEY REFERENCES {SCHEMA}.heads(bucket),
                owner text, lease_until timestamptz, next_check timestamptz,
                last_attempt timestamptz, last_success timestamptz, cursor_ms bigint,
                status text NOT NULL DEFAULT 'NOT_CHECKED', failures integer NOT NULL DEFAULT 0,
                report jsonb, checked_revision bigint)''')
            conn.execute(f'REVOKE ALL ON {TABLE} FROM PUBLIC')

    def claim(self):
        """Oldest-due first. One bucket per pass is batching, never a trade/card cap."""
        token=uuid.uuid4().hex
        with self.journal._transaction() as conn:
            LifecycleStore(self.journal).ready(conn)
            conn.execute(f'''INSERT INTO {TABLE}(bucket) SELECT bucket FROM {SCHEMA}.heads
                ON CONFLICT DO NOTHING''')
            row=conn.execute(f'''SELECT s.bucket,h.revision,h.evidence,h.evidence_hash,s.cursor_ms
                FROM {TABLE} s JOIN {SCHEMA}.heads h ON h.bucket=s.bucket
                WHERE (s.lease_until IS NULL OR s.lease_until<clock_timestamp())
                  AND (s.next_check IS NULL OR s.next_check<=clock_timestamp())
                ORDER BY s.last_attempt NULLS FIRST,s.bucket
                FOR UPDATE OF s SKIP LOCKED LIMIT 1''').fetchone()
            if not row: return None
            bucket,revision,evidence,checksum,cursor=row
            LifecycleStore.verify(evidence,checksum)
            conn.execute(f'''UPDATE {TABLE} SET owner=%s,lease_until=clock_timestamp()+interval '60 seconds',
                status='CHECKING',last_attempt=clock_timestamp() WHERE bucket=%s''',(token,bucket))
        return dict(bucket=bucket,token=token,revision=revision,evidence=evidence,
                    cursor_ms=max(cursor or 0,evidence['snapshot']['at_ms']))

    def save(self,claim,observation,*,now_ms):
        evidence=dict(bindings=deepcopy(observation['bindings']),snapshot=deepcopy(observation['snapshot']))
        report=life.review(evidence['bindings'],evidence['snapshot'],now_ms=now_ms)
        if life.digest(evidence['bindings'])!=life.digest(claim['evidence']['bindings']):
            raise LifecycleStorageError('BINDING_CHANGED_DURING_READ')
        if observation['cursor_ms']!=evidence['snapshot']['at_ms']:
            raise LifecycleStorageError('CHECKPOINT_NOT_BOUND_TO_OBSERVATION')
        bucket=LifecycleStore.bucket(evidence['snapshot']['account'],evidence['snapshot']['symbol'])
        if bucket!=claim['bucket']: raise LifecycleStorageError('BUCKET_MISMATCH')
        good=not report['needs_review']
        with self.journal._transaction() as conn:
            owned=conn.execute(f'''SELECT owner,lease_until>clock_timestamp() FROM {TABLE}
                WHERE bucket=%s FOR UPDATE''',(bucket,)).fetchone()
            if owned!=(claim['token'],True): raise LifecycleStorageError('OBSERVATION_LEASE_LOST')
            old=conn.execute(f'''SELECT revision,evidence,evidence_hash FROM {SCHEMA}.heads
                WHERE bucket=%s FOR UPDATE''',(bucket,)).fetchone()
            if not old or old[0]!=claim['revision']:
                raise LifecycleStorageError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
            LifecycleStore.verify(old[1],old[2])
            _continues(old[1],evidence)
            changed=good and facts(old[1]['snapshot'])!=facts(evidence['snapshot'])
            revision=old[0]
            if changed:
                revision+=1
                payload,checksum=life.encoded(evidence),life.digest(evidence)
                conn.execute(f'''UPDATE {SCHEMA}.heads SET revision=%s,evidence=%s::jsonb,evidence_hash=%s
                    WHERE bucket=%s''',(revision,payload,checksum,bucket))
                conn.execute(f'''INSERT INTO {SCHEMA}.history(bucket,revision,evidence,evidence_hash)
                    VALUES(%s,%s,%s::jsonb,%s)''',(bucket,revision,payload,checksum))
            # A problem never advances the checkpoint or replaces last verified evidence.
            conn.execute(f'''UPDATE {TABLE} SET owner=NULL,lease_until=NULL,
                next_check=clock_timestamp()+interval '30 seconds',last_attempt=clock_timestamp(),
                last_success=CASE WHEN %s THEN clock_timestamp() ELSE last_success END,
                cursor_ms=CASE WHEN %s THEN %s ELSE cursor_ms END,
                status=%s,failures=CASE WHEN %s THEN 0 ELSE failures+1 END,
                report=%s::jsonb,checked_revision=%s WHERE bucket=%s''',
                (good,good,observation['cursor_ms'],'VERIFIED' if good else 'NEEDS_REVIEW',
                 good,life.encoded(report),revision,bucket))
        return dict(changed=changed,revision=revision,status='VERIFIED' if good else 'NEEDS_REVIEW',report=report)

    def fail(self,claim,code):
        life.ident(code,r'[A-Z_]{1,100}')
        with self.journal._transaction() as conn:
            conn.execute(f'''UPDATE {TABLE} SET owner=NULL,lease_until=NULL,status=%s,report=NULL,
                failures=failures+1,last_attempt=clock_timestamp(),
                next_check=clock_timestamp()+LEAST(300,30*power(2,LEAST(failures,3)))*interval '1 second'
                WHERE bucket=%s AND owner=%s''',(code,claim['bucket'],claim['token']))

    def summary(self):
        with self.journal._transaction() as conn:
            row=conn.execute(f'''SELECT count(*),
                count(*) FILTER(WHERE s.status='VERIFIED' AND s.last_success>clock_timestamp()-interval '120 seconds'
                    AND s.checked_revision=h.revision),
                count(*) FILTER(WHERE s.status NOT IN ('VERIFIED','CHECKING','NOT_CHECKED')),
                count(*) FILTER(WHERE s.last_success IS NULL OR s.last_success<=clock_timestamp()-interval '120 seconds'
                    OR s.checked_revision IS DISTINCT FROM h.revision)
                FROM {SCHEMA}.heads h LEFT JOIN {TABLE} s ON s.bucket=h.bucket''').fetchone()
        return dict(registered_buckets=row[0],fresh_verified_buckets=row[1],problem_buckets=row[2],stale_buckets=row[3])
