"""Additive record-only tables on the verified staging DB; no exchange access."""
from . import trade_cards as cards
from .postgres_journal import JournalError

SCHEMA = 'hl_testnet_cards_v1'
LOCK = 1729048190


class CardStore:
    def __init__(self, journal):
        self.journal = journal

    def ready(self, conn):
        row = conn.execute(f'SELECT version FROM {SCHEMA}.metadata WHERE singleton=true').fetchone()
        if row != (cards.VERSION,):
            raise JournalError('CARD_SCHEMA_REQUIRES_REVIEW')
        if conn.execute('SELECT to_regclass(%s)', (SCHEMA + '.cards',)).fetchone()[0] is None:
            raise JournalError('CARD_SCHEMA_REQUIRES_REVIEW')

    def initialize(self):
        """Startup only, never in the alert hot path. Existing trading tables untouched."""
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK,))
            if conn.execute('SELECT to_regnamespace(%s)', (SCHEMA,)).fetchone()[0] is not None:
                self.ready(conn)
                return False
            conn.execute(f'CREATE SCHEMA {SCHEMA}')
            conn.execute(f'''CREATE TABLE {SCHEMA}.metadata (
                singleton boolean PRIMARY KEY CHECK(singleton), version text NOT NULL,
                visits bigint NOT NULL DEFAULT 0)''')
            conn.execute(f'INSERT INTO {SCHEMA}.metadata VALUES(true,%s,0)', (cards.VERSION,))
            conn.execute(f'''CREATE TABLE {SCHEMA}.cards (
                card_id text PRIMARY KEY CHECK(length(card_id)=64),
                source_stream text NOT NULL, event_id text NOT NULL,
                digest text NOT NULL CHECK(length(digest)=64), manifest jsonb NOT NULL,
                environment text NOT NULL DEFAULT 'testnet' CHECK(environment='testnet'),
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                UNIQUE(source_stream,event_id))''')
            conn.execute(f'REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC')
            for table in ('metadata', 'cards'):
                conn.execute(f'REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC')
        return True

    def probe(self):
        with self.journal._transaction() as conn:
            self.ready(conn)
            visits = conn.execute(f'''UPDATE {SCHEMA}.metadata SET visits=visits+1
                WHERE singleton=true RETURNING visits''').fetchone()[0]
        with self.journal._transaction() as conn:
            self.ready(conn)
            observed = conn.execute(f'SELECT visits FROM {SCHEMA}.metadata WHERE singleton=true').fetchone()[0]
            if observed < visits:
                raise JournalError('CARD_STORAGE_READBACK_FAILED')
        return dict(separate_connection_verified=True, previous_probe_seen=visits > 1)

    def record(self, card):
        card = cards.validate_card(card)
        payload, digest = cards.canonical(card), cards.checksum(card)
        with self.journal._transaction() as conn:
            self.ready(conn)
            # Unique index arbitrates concurrent/restarted deliveries. No overwrite.
            inserted = conn.execute(f'''INSERT INTO {SCHEMA}.cards
                (card_id,source_stream,event_id,digest,manifest) VALUES(%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT(source_stream,event_id) DO NOTHING RETURNING card_id''',
                (card['card_id'],card['source_stream'],card['event_id'],digest,payload)).fetchone()
            row = conn.execute(f'''SELECT card_id,digest FROM {SCHEMA}.cards
                WHERE source_stream=%s AND event_id=%s''',
                (card['source_stream'],card['event_id'])).fetchone()
            if row != (card['card_id'],digest):
                raise JournalError('SOURCE_OR_PLAN_CHANGED_NO_OVERWRITE')
        return dict(card_id=card['card_id'], created=inserted is not None,
                    duplicate=inserted is None, dispatch_enabled=False)

    def load(self, card_id):
        if not isinstance(card_id, str) or not cards.re.fullmatch(r'[0-9a-f]{64}', card_id):
            raise JournalError('INVALID_CARD_ID')
        with self.journal._transaction() as conn:
            self.ready(conn)
            row = conn.execute(f'SELECT manifest,digest FROM {SCHEMA}.cards WHERE card_id=%s', (card_id,)).fetchone()
            if row is None:
                raise JournalError('UNKNOWN_CARD')
            if cards.checksum(row[0]) != row[1]:
                raise JournalError('STORED_CARD_CHECKSUM_MISMATCH')
            return cards.validate_card(row[0])

    def import_legacy_reviews(self):
        """At most 32 archived source reviews per invocation, NOT a trading cap.

        Only sources already bound to saved formula rules, not reconstructed
        alerts. Preserve original times. Do not import historical fills as new
        pending orders. Does not create/modify any execution reservation.
        """
        with self.journal._transaction() as conn:
            self.journal._ready(conn)
            exists = conn.execute('SELECT to_regclass(%s)',
                ('hl_testnet_execution_v1.limit_cancel_rules',)).fetchone()[0]
            if exists is None:
                return []
            rows = conn.execute('''SELECT p.manifest,r.policy
                FROM hl_testnet_execution_v1.prepared p
                JOIN hl_testnet_execution_v1.limit_cancel_rules r ON r.plan_key=p.plan_key
                ORDER BY p.created_at,p.plan_key LIMIT 32''').fetchall()
        reports = []
        for prepared, rule in rows:
            source = prepared['source']
            if (rule.get('source_digest') != cards.checksum(source)
                    or rule.get('event_id') != source['event_id']):
                raise JournalError('ARCHIVED_FORMULA_SOURCE_MISMATCH')
            meta = {'universe':[{'name':source['symbol'], 'szDecimals':prepared['audit']['sz_decimals']}]}
            card = cards.prepare_card(source, meta, rule_id=rule['rule_id'],
                threshold_pct=rule['threshold_pct'], record_kind='historical_review',
                source_stream='legacy_testnet_review')
            first = self.record(card)
            second = self.record(card)
            if second['created'] or self.load(first['card_id']) != card:
                raise JournalError('CARD_REPLAY_CHECK_FAILED')
            reports.append(dict(created=first['created'], replay_verified=True,
                account_role=card['account_role'], record_kind='historical_review'))
        return reports
