"""Testnet state-domain and SQL safety; real PG tests explicitly gated.

Fake SQL unit tests are not claimed as PostgreSQL evidence. The optional tests
use only the established disposable loopback CI database.
"""
from copy import deepcopy
from contextlib import nullcontext
import json
import os
import unittest
from unittest.mock import patch

from . import card_lifecycle as life
from . import experimental_live_state as state
from .experimental_execution_state import initial as software_initial, StateError
from .filled_dispatch_store import DispatchStore, SCHEMA as LEGACY_SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .test_experimental_execution_runtime import ROUTES, T

LIVE_ROUTES = {r: dict(**v, agent='0x'+str(i+3)*40) for i,(r,v) in enumerate(ROUTES.items())}
CI = os.environ.get('HL_JOURNAL_CI_URL')


class Connection:
    def __init__(self, value=None, *, commit_failure=False, nonce=T+10000):
        self.value = deepcopy(value if value is not None else state.initial(LIVE_ROUTES, T-60000))
        self.commit_failure, self.nonce = commit_failure, nonce
        self.queries = []; self.rollbacks = 0
    def __enter__(self): return self
    def __exit__(self, typ, _value, _traceback):
        if typ: self.rollbacks += 1
        elif self.commit_failure: raise OSError('PRIVATE_COMMIT_DETAILS')
    def close(self): pass
    def transaction(self): return self
    def execute(self, query, params=None):
        self.queries.append((query,params))
        class Cursor:
            def __init__(self,row): self.row=row
            def fetchone(self): return self.row
        if query == 'SELECT current_database()': return Cursor(('hl_journal_ci',))
        if query.startswith('SELECT pg_advisory_xact_lock'): return Cursor((None,))
        if query.startswith('SELECT version,domain FROM '+state.PG_SCHEMA): return Cursor((state.VERSION,'testnet'))
        if query.startswith('SELECT version,domain FROM '+LEGACY_SCHEMA):
            from .filled_dispatch_store import VERSION
            return Cursor((VERSION,'software'))
        if query.startswith('SELECT value,checksum'): return Cursor((deepcopy(self.value),life.digest(self.value)))
        if query.startswith('INSERT INTO '+LEGACY_SCHEMA+'.nonces'):
            self.nonce=max(self.nonce+1,params[1]);return Cursor((self.nonce,))
        if query.startswith('UPDATE '+state.PG_SCHEMA+'.state'):
            self.value=json.loads(params[0]);return Cursor(None)
        raise AssertionError('UNEXPECTED_QUERY')


class StateUnitTests(unittest.TestCase):
    def setUp(self):
        self.journal=PostgresJournal.for_ci('postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci')
        self.store=state.TestnetExecutionState.for_ci(self.journal)

    def test_software_state_cannot_be_reinterpreted_as_testnet(self):
        value=software_initial(ROUTES,T-60000)
        with self.assertRaisesRegex(StateError,'TESTNET_STATE_INTEGRITY_FAILURE'):
            state.checked(value,life.digest(value))

    def test_r2732_software_and_testnet_views_cannot_cross_domains(self):
        from . import r2732_entry
        software=r2732_entry.initial(LIVE_ROUTES,not_before_ms=T)
        live=r2732_entry.initial_testnet(LIVE_ROUTES,not_before_ms=T)
        self.assertEqual(r2732_entry._copy(software),software)
        self.assertEqual(r2732_entry._copy(live,domain='testnet'),live)
        with self.assertRaisesRegex(r2732_entry.EntryError,'STATE_INVALID'):
            r2732_entry._copy(live)
        with self.assertRaisesRegex(r2732_entry.EntryError,'STATE_INVALID'):
            r2732_entry._copy(software,domain='testnet')

    def test_nested_software_request_or_source_rejected_even_with_matching_checksum(self):
        for key in ('sources','requests'):
            value=state.initial(LIVE_ROUTES,T-60000);value[key]['x']={'domain':'software'}
            with self.subTest(key=key),self.assertRaisesRegex(StateError,'INTEGRITY'):
                state.checked(value,life.digest(value))

    def test_constructor_requires_explicit_ci_method(self):
        with self.assertRaisesRegex(StateError,'EXPLICIT_TESTNET_POSTGRES_JOURNAL_REQUIRED'):
            state.TestnetExecutionState(self.journal)

    def test_distinct_routes_required_and_agents_pinned(self):
        bad=deepcopy(LIVE_ROUTES);bad['short_account']['account']=bad['long_account']['account']
        with self.assertRaisesRegex(StateError,'INDEPENDENT_TESTNET'):
            state.initial(bad,T)
        conn=Connection()
        with patch.object(self.journal,'_connect',return_value=conn),self.assertRaisesRegex(StateError,'IDENTITY_CHANGED'):
            self.store.mutate(lambda value: value['agents'].update(short_account='0x'+'f'*40))
        self.assertEqual(conn.rollbacks,1)

    def test_hot_path_never_initializes_schema(self):
        conn=Connection()
        with patch.object(self.journal,'_connect',return_value=conn):
            self.store.load();self.store.mutate(lambda value:value['events'].append({'test':'ok'}))
        self.assertEqual(conn.value['revision'],1)
        self.assertFalse(any(any(word in q for word in ('CREATE ','ALTER ','DROP ')) for q,_ in conn.queries))

    def test_nonce_allocated_from_existing_agent_table_inside_state_transaction(self):
        conn=Connection(nonce=T+10010)
        with patch.object(self.journal,'_connect',return_value=conn):
            result=self.store.commit_attempt(lambda value,nonce:dict(nonce=nonce),role='short_account',now_ms=T+10000)
        self.assertEqual(result,dict(nonce=T+10011))
        query,args=next((q,a) for q,a in conn.queries if q.startswith('INSERT INTO '+LEGACY_SCHEMA+'.nonces'))
        self.assertEqual(args,(LIVE_ROUTES['short_account']['agent'],T+10000))
        self.assertIn('GREATEST',query)
        self.assertEqual(conn.value['revision'],1)

    def test_nonce_future_bound_rolls_back(self):
        conn=Connection(nonce=T+11000)
        with patch.object(self.journal,'_connect',return_value=conn),self.assertRaisesRegex(StateError,'NONCE_WINDOW'):
            self.store.commit_attempt(lambda value,nonce:nonce,role='short_account',now_ms=T+10000)
        self.assertEqual(conn.rollbacks,1)
        self.assertFalse(any(q.startswith('UPDATE ') for q,_ in conn.queries))

    def test_commit_ambiguity_cannot_return_request_in_either_connection_mode(self):
        for leased in (False,True):
            conn=Connection(commit_failure=True)
            with self.subTest(leased=leased),patch.object(self.journal,'_connect',return_value=conn):
                with self.journal.reuse_connection() if leased else nullcontext():
                    with self.assertRaisesRegex(JournalError,'PERSISTENCE_UNAVAILABLE_NO_SEND'):
                        self.store.commit_attempt(lambda value,nonce:dict(nonce=nonce),role='short_account',now_ms=T+10000)

    def test_controlled_revision_error_is_not_masked_as_database_error(self):
        conn=Connection();failure=StateError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
        def fail(_state):raise failure
        with patch.object(self.journal,'_connect',return_value=conn),self.assertRaises(StateError) as caught:
            self.store.mutate(fail)
        self.assertIs(caught.exception,failure)
        self.assertEqual(conn.rollbacks,1)


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL; mock SQL is not PostgreSQL evidence')
class StatePostgresTests(unittest.TestCase):
    def setUp(self):
        self.journal=PostgresJournal.for_ci(CI);self.journal.bootstrap()
        DispatchStore(self.journal).initialize()
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {state.PG_SCHEMA} CASCADE')
        self.store=state.TestnetExecutionState.for_ci(self.journal)
        self.store.initialize(LIVE_ROUTES,not_before_ms=T-60000)

    def test_testnet_domain_persists_and_reopens_without_software_translation(self):
        self.store.mutate(lambda value:value['events'].append(dict(kind='CI_REOPEN')))
        other=state.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        self.assertEqual(other.load()['domain'],'testnet')
        self.assertEqual(other.load()['events'],[dict(kind='CI_REOPEN')])

    def test_reinitialization_cannot_change_agent_route_or_activation(self):
        bad=deepcopy(LIVE_ROUTES);bad['short_account']['agent']='0x'+'f'*40
        with self.assertRaisesRegex(StateError,'ROUTE_OR_ACTIVATION_CHANGED'):
            self.store.initialize(bad,not_before_ms=T-60000)
        self.assertEqual(self.store.load()['agents']['short_account'],LIVE_ROUTES['short_account']['agent'])

    def test_failed_transition_rolls_back_shared_nonce_too(self):
        def fail(value,nonce): raise StateError('EXPECTED_CI_ROLLBACK')
        role='short_account'
        with self.journal._transaction() as conn:
            conn.execute(f'DELETE FROM {LEGACY_SCHEMA}.nonces WHERE agent=%s',(LIVE_ROUTES[role]['agent'],))
        with self.assertRaisesRegex(StateError,'EXPECTED_CI_ROLLBACK'):
            self.store.commit_attempt(fail,role=role,now_ms=T)
        with self.journal._transaction() as conn:
            value=conn.execute(f'SELECT nonce FROM {LEGACY_SCHEMA}.nonces WHERE agent=%s',(LIVE_ROUTES[role]['agent'],)).fetchone()
        self.assertIsNone(value)
        self.assertEqual(self.store.load()['revision'],0)

    def test_shared_nonce_sequence_crosses_connections(self):
        role='long_account'
        with self.journal._transaction() as conn:
            conn.execute(f'DELETE FROM {LEGACY_SCHEMA}.nonces WHERE agent=%s',(LIVE_ROUTES[role]['agent'],))
        other=state.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        a=self.store.commit_attempt(lambda value,nonce:nonce,role=role,now_ms=T)
        b=other.commit_attempt(lambda value,nonce:nonce,role=role,now_ms=T)
        self.assertEqual((a,b),(T,T+1))
