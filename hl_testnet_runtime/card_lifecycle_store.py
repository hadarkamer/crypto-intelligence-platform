"""Append-only shadow observations in the EXISTING isolated staging database.

No initialization on import/startup, no exchange or app connection. This is not an
execution queue. Source cards and the legacy order journal are never overwritten.
"""
from . import card_lifecycle as life

SCHEMA = 'hl_testnet_card_lifecycle_v1'


def _continues(previous, current):
    """New facts/IDs may be appended; never forget fills or silently reassign IDs."""
    old_b = {b['card_id']: b for b in previous['bindings']}
    new_b = {b['card_id']: b for b in current['bindings']}
    if not set(old_b) <= set(new_b):
        raise life.LifecycleError('PREVIOUS_CARD_REMOVED')
    for cid, old in old_b.items():
        new = new_b[cid]
        if {k:v for k,v in old.items() if k != 'orders'} != {k:v for k,v in new.items() if k != 'orders'}:
            raise life.LifecycleError('IMMUTABLE_CARD_BINDING_CHANGED')
        if any(not set(old['orders'][leg]) <= set(new['orders'][leg]) for leg in life.LEGS):
            raise life.LifecycleError('PREVIOUS_ORDER_BINDING_REMOVED')
    before, after = previous['snapshot'], current['snapshot']
    if before['account'].lower() != after['account'].lower() or before['symbol'] != after['symbol']:
        raise life.LifecycleError('BUCKET_CHANGED')
    if after['at_ms'] <= before['at_ms']:
        raise life.LifecycleError('STALE_OR_CONFLICTING_SNAPSHOT')
    for collection, key in (('fills', 'fill_id'), ('terminal_orders', 'oid')):
        new = {row[key]: row for row in after[collection]}
        for row in before[collection]:
            if new.get(row[key]) != row:
                raise life.LifecycleError('PREVIOUS_EXCHANGE_FACT_CHANGED_OR_MISSING')


class LifecycleStore:
    def __init__(self, journal):
        self.journal = journal

    def initialize(self):
        """Explicit migration only; currently exercised on disposable CI storage."""
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (1729048210,))
            conn.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {SCHEMA}.versions
                (singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL)''')
            conn.execute(f'''INSERT INTO {SCHEMA}.versions VALUES(true,%s)
                ON CONFLICT DO NOTHING''', (life.VERSION,))
            self.ready(conn)
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {SCHEMA}.heads
                (bucket text PRIMARY KEY, revision bigint NOT NULL CHECK(revision>0),
                 evidence jsonb NOT NULL, evidence_hash text NOT NULL)''')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {SCHEMA}.history
                (bucket text NOT NULL REFERENCES {SCHEMA}.heads(bucket),
                 revision bigint NOT NULL CHECK(revision>0), evidence jsonb NOT NULL,
                 evidence_hash text NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                 PRIMARY KEY(bucket,revision))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for table in ('versions', 'heads', 'history'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC')

    def ready(self, conn):
        if conn.execute(f'SELECT version FROM {SCHEMA}.versions WHERE singleton=true').fetchone() != (life.VERSION,):
            raise life.LifecycleError('LIFECYCLE_SCHEMA_REQUIRES_REVIEW')

    @staticmethod
    def bucket(account, symbol):
        return life.digest(['testnet', life.address(account), life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')])

    @staticmethod
    def verify(evidence, checksum):
        if life.digest(evidence) != checksum:
            raise life.LifecycleError('OBSERVATION_CHECKSUM_MISMATCH')

    def save(self, bindings, snapshot, *, expected_revision, now_ms):
        report = life.review(bindings, snapshot, now_ms=now_ms)
        if type(expected_revision) is not int or expected_revision < 0:
            raise life.LifecycleError('REVISION_REQUIRED')
        evidence = dict(bindings=bindings, snapshot=snapshot)
        checksum, payload = life.digest(evidence), life.encoded(evidence)
        bucket = self.bucket(snapshot['account'], snapshot['symbol'])
        # Serialize both the first insert and updates, independently per bucket.
        lock = int(bucket[:16], 16) % (2**63)
        with self.journal._transaction() as conn:
            self.ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (lock,))
            old = conn.execute(f'SELECT revision,evidence,evidence_hash FROM {SCHEMA}.heads WHERE bucket=%s', (bucket,)).fetchone()
            revision = 0 if old is None else old[0]
            if old:
                self.verify(old[1], old[2])
                if old[2] == checksum:
                    return dict(revision=revision, duplicate=True, report=report)
            if expected_revision != revision:
                raise life.LifecycleError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
            if old:
                _continues(old[1], evidence)
            revision += 1
            conn.execute(f'''INSERT INTO {SCHEMA}.heads VALUES(%s,%s,%s::jsonb,%s)
                ON CONFLICT(bucket) DO UPDATE SET revision=EXCLUDED.revision,
                evidence=EXCLUDED.evidence,evidence_hash=EXCLUDED.evidence_hash''',
                (bucket, revision, payload, checksum))
            conn.execute(f'INSERT INTO {SCHEMA}.history(bucket,revision,evidence,evidence_hash) VALUES(%s,%s,%s::jsonb,%s)', (bucket, revision, payload, checksum))
        return dict(revision=revision, duplicate=False, report=report)

    def load(self, account, symbol, *, now_ms):
        with self.journal._transaction() as conn:
            self.ready(conn)
            row = conn.execute(f'SELECT revision,evidence,evidence_hash FROM {SCHEMA}.heads WHERE bucket=%s', (self.bucket(account,symbol),)).fetchone()
        if not row:
            raise life.LifecycleError('NO_LIFECYCLE_OBSERVATION')
        self.verify(row[1], row[2])
        # Recheck freshness; a saved green report is not current authority.
        return dict(revision=row[0], report=life.review(row[1]['bindings'],row[1]['snapshot'],now_ms=now_ms))
