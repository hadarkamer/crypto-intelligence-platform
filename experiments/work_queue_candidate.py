"""Isolated extension of the generated collector's PostgreSQL job ledger.

TEST DATABASE ONLY. Not imported by prepare.py, app, routes, or live workers.
Reuses the existing JobStore connection and jobs table. No source/AI calls.
This is a queue contract candidate, not a production migration or scheduler.
"""
from dataclasses import dataclass
from uuid import UUID

from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

from work_identity import WorkSpec


@dataclass(frozen=True)
class Claim:
    command: WorkSpec
    owner: str
    generation: int


class WorkQueue:
    def __init__(self, existing_store):
        info = conninfo_to_dict(existing_store.dsn)
        # Explicit local CI fixture only; never fall back to DATABASE_URL.
        if (info.get('host') != '127.0.0.1'
                or info.get('dbname') != 'coinglass_queue_test'
                or info.get('user') != 'queue_test'
                or info.get('port') != '5432'):
            raise ValueError('Only the explicit isolated queue fixture is allowed')
        self.store = existing_store

    def connect(self):
        return self.store.connect()

    def initialize(self):
        self.store.initialize()
        with self.connect() as c:
            c.execute('''ALTER TABLE public.ai_collection_bridge_jobs
                ADD COLUMN IF NOT EXISTS work_command jsonb,
                ADD COLUMN IF NOT EXISTS owner_id uuid,
                ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS lease_until timestamptz,
                ADD COLUMN IF NOT EXISTS external_started boolean NOT NULL DEFAULT false''')
            # Preserve the old rule for legacy rows; new jobs use full identity.
            c.execute('DROP INDEX IF EXISTS public.ai_collection_bridge_one_active_model_tf')
            c.execute('''CREATE UNIQUE INDEX ai_collection_bridge_one_active_model_tf
                ON public.ai_collection_bridge_jobs(heatmap_model,timeframe)
                WHERE work_command IS NULL AND status IN ('queued','running')''')
            c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_work_identity
                ON public.ai_collection_bridge_jobs
                ((work_command->>'tenant_id'), (work_command->>'source_scope_id'),
                 (work_command->>'batch_id'), (work_command->>'coin'),
                 (work_command->>'heatmap_model'), (work_command->>'timeframe'))
                WHERE work_command IS NOT NULL''')

    def enqueue(self, command: WorkSpec):
        command = WorkSpec.from_record(command.to_record())
        with self.connect() as c:
            c.execute('''INSERT INTO public.ai_collection_bridge_jobs
                (id,timeframe,heatmap_model,status,work_command)
                VALUES (%s,%s,%s,'queued',%s) ON CONFLICT (id) DO NOTHING''',
                (command.work_id, command.timeframe, command.heatmap_model,
                 Jsonb(command.to_record())))
            # Separate statement gets a fresh READ COMMITTED snapshot after
            # waiting for the winning INSERT. Never reset a completed job.
            row = c.execute('''SELECT * FROM public.ai_collection_bridge_jobs
                WHERE id=%s''', (command.work_id,)).fetchone()
            command.require_same_command(row['work_command'])
            return row

    @staticmethod
    def _lease(seconds):
        if type(seconds) is not int or not 1 <= seconds <= 900:
            raise ValueError('Lease must be 1..900 integer seconds')
        return seconds

    def claim(self, owner: str, lease_seconds=60):
        owner = str(UUID(owner))
        seconds = self._lease(lease_seconds)
        with self.connect() as c:
            row = c.execute('''WITH candidate AS (
                SELECT id FROM public.ai_collection_bridge_jobs
                WHERE work_command IS NOT NULL AND
                    (status='queued' OR (status='running'
                     AND lease_until <= clock_timestamp() AND NOT external_started))
                ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
            ) UPDATE public.ai_collection_bridge_jobs AS j
              SET status='running', owner_id=%s, generation=j.generation+1,
                  started_at=clock_timestamp(),
                  lease_until=clock_timestamp()+%s*interval '1 second'
              FROM candidate WHERE j.id=candidate.id RETURNING j.*''',
                (owner, seconds)).fetchone()
            if row is None:
                return None
            command = WorkSpec.from_record(row['work_command'])
            if (command.work_id != str(row['id']) or command.timeframe != row['timeframe']
                    or command.heatmap_model != row['heatmap_model']):
                raise ValueError('Stored command mismatch')  # transaction rolls back
            return Claim(command, owner, row['generation'])

    @staticmethod
    def _fence(claim):
        return (claim.command.work_id, claim.owner, claim.generation)

    def renew(self, claim, lease_seconds=60):
        with self.connect() as c:
            return c.execute('''UPDATE public.ai_collection_bridge_jobs
                SET lease_until=clock_timestamp()+%s*interval '1 second'
                WHERE id=%s AND owner_id=%s AND generation=%s
                  AND status='running' AND lease_until > clock_timestamp()''',
                (self._lease(lease_seconds), *self._fence(claim))).rowcount == 1

    def begin_external(self, claim):
        """Reserve once, before an external side effect; does not call a provider.

        Expiry after this boundary requires reconciliation, never auto-reclaim.
        A crash between this write and sending is conservatively ambiguous.
        """
        with self.connect() as c:
            return c.execute('''UPDATE public.ai_collection_bridge_jobs
                SET external_started=true
                WHERE id=%s AND owner_id=%s AND generation=%s
                  AND status='running' AND lease_until > clock_timestamp()
                  AND NOT external_started''', self._fence(claim)).rowcount == 1

    def finish(self, claim, result=None, image=None, error=None):
        with self.connect() as c:
            return c.execute('''UPDATE public.ai_collection_bridge_jobs
                SET status=%s,result=%s,image_png=%s,error_code=%s,
                    finished_at=clock_timestamp(),lease_until=NULL
                WHERE id=%s AND owner_id=%s AND generation=%s
                  AND status='running' AND lease_until > clock_timestamp()''',
                ('failed' if error else 'ready', Jsonb(result), image, error,
                 *self._fence(claim))).rowcount == 1

    def get(self, work_id):
        with self.connect() as c:
            return c.execute('SELECT * FROM public.ai_collection_bridge_jobs WHERE id=%s',
                             (work_id,)).fetchone()
