"""Private diagnostics must never become a public control or venue consumer."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import json
import unittest
from unittest.mock import patch

from . import app, testnet_diagnostics as m, card_lifecycle as life
from .test_filled_quantity_dispatch import state_from_case
from .test_card_lifecycle import A, B, T

SECRET = 'private-diagnostics-' + 'a1234567890bcdef' * 2
ENV = {'HL_TESTNET_DIAGNOSTICS_TOKEN':SECRET,'RENDER_SERVICE_ID':m.SERVICE,
       'HL_TESTNET_RUNTIME_MODE':'long_stream_testnet_v1',
       'HL_TESTNET_LONG_ACCOUNT_ADDRESS':A,
       'HL_TESTNET_LONG_AGENT_ADDRESS':'0x'+'3'*40,
       'HL_TESTNET_SHORT_ACCOUNT_ADDRESS':B,
       'HL_TESTNET_SHORT_AGENT_ADDRESS':'0x'+'4'*40,
       'HL_TESTNET_LONG_ENTRY_ENABLED':'false','HL_TESTNET_SHORT_ENTRY_ENABLED':'false',
       'RENDER_GIT_COMMIT':'e'*40,'HL_TESTNET_AGENT_KEY':'DO_NOT_DISCLOSE',
       'HL_TESTNET_DATABASE_URL':'DO_NOT_DISCLOSE_DATABASE_URL'}


class Cursor:
    def __init__(self, rows): self.rows=rows
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class ReadOnlyFixture:
    """The fake connection rejects ANY statement other than trusted SELECT."""
    def __init__(self, state=None):
        self.state=state or state_from_case(q='40',stop='40',take='40',expiry_seconds=60)
        self._parameters=dict(host='selected-internal-host',dbname='selected_testnet_db',
                              user='DO_NOT_DISCLOSE_USER',password='DO_NOT_DISCLOSE_PASSWORD')
        self.statements=[]
    @contextmanager
    def _transaction(self): yield self
    def execute(self, sql, params=()):
        self.statements.append((sql,params))
        if not sql.lstrip().startswith('SELECT'):
            raise AssertionError('DIAGNOSTICS_MUST_ONLY_SELECT')
        if sql=='SELECT current_database()': return Cursor([('selected_testnet_db',)])
        if sql=='SELECT to_regclass(%s)':
            return Cursor([(params[0] if params[0] in (m.BUCKETS,m.EVENTS) else None,)])
        if 'SELECT count(*)' in sql: return Cursor([(1,)])
        if 'GROUP BY event' in sql: return Cursor([('OBSERVE',1)])
        if 'SELECT value,digest,revision' in sql:
            return Cursor([(self.state,life.digest(self.state),1)])
        if 'SELECT min(at_ms)' in sql: return Cursor([(T,T)])
        if 'SELECT bucket,revision,event' in sql: return Cursor([(self.state['bucket'],1,'OBSERVE',None,T)])
        raise AssertionError('UNEXPECTED_DIAGNOSTICS_QUERY')


class EndpointTests(unittest.TestCase):
    def request(self, **overrides):
        response=[]
        env={'REQUEST_METHOD':'GET','PATH_INFO':'/internal/testnet-diagnostics/v1',
             'QUERY_STRING':'','HTTP_X_TESTNET_DIAGNOSTICS_TOKEN':SECRET,
             'wsgi.input':BytesIO()}
        env.update(overrides)
        body=b''.join(app.application(env,lambda status,headers:response.append((status,dict(headers)))))
        return response[0][0],response[0][1],body
    def test_auth_is_header_only_and_rejects_body_query_and_other_methods(self):
        with patch.dict('os.environ',ENV,clear=True),patch.object(m,'load_diagnostics') as read:
            for change in ({'HTTP_X_TESTNET_DIAGNOSTICS_TOKEN':''},
                           {'HTTP_X_TESTNET_DIAGNOSTICS_TOKEN':'wrong'},
                           {'HTTP_X_TESTNET_DIAGNOSTICS_TOKEN':'é'*40},
                           {'REQUEST_METHOD':'POST'},{'REQUEST_METHOD':'HEAD'},
                           {'QUERY_STRING':'token='+SECRET},{'CONTENT_LENGTH':'1'},
                           {'HTTP_TRANSFER_ENCODING':'chunked'},
                           {'HTTP_X_TRADE_REPORT_TOKEN':SECRET,'HTTP_X_TESTNET_DIAGNOSTICS_TOKEN':''}):
                status,headers,body=self.request(**change)
                self.assertEqual((status,body),('404 Not Found',b'Not found\n'))
                self.assertEqual(headers['Cache-Control'],'no-store')
            read.assert_not_called()
    def test_token_must_be_separate_and_testnet_service_must_match(self):
        for settings in ({'HL_TESTNET_DIAGNOSTICS_TOKEN':'short'},
                         {'HL_TESTNET_CARDS_INTAKE_SECRET':SECRET},
                         {'HL_TESTNET_REPORT_API_TOKEN':SECRET},
                         {'RENDER_SERVICE_ID':'some_other_service'},
                         {'HL_TESTNET_RUNTIME_MODE':'mainnet'}):
            with patch.dict('os.environ',{**ENV,**settings},clear=True), \
                 patch.object(m,'load_diagnostics') as read:
                self.assertEqual(self.request()[0],'404 Not Found')
                read.assert_not_called()
    def test_authorized_response_has_absolute_size_cap_and_no_private_error(self):
        with patch.dict('os.environ',ENV,clear=True):
            with patch.object(m,'load_diagnostics',return_value={'status':'READ_ONLY'}):
                status,headers,body=self.request()
                self.assertEqual(status,'200 OK')
                self.assertEqual(json.loads(body),{'status':'READ_ONLY'})
                self.assertEqual(int(headers['Content-Length']),len(body))
            for output in ({'payload':'x'*(m.MAX_BYTES+1)},{'invalid':float('nan')}):
                with patch.object(m,'load_diagnostics',return_value=output):
                    status,_,body=self.request()
                    self.assertEqual((status,body),('503 Service Unavailable',b'Diagnostics unavailable\n'))
            with patch.object(m,'load_diagnostics',side_effect=RuntimeError('DO_NOT_DISCLOSE')):
                self.assertNotIn(b'DO_NOT_DISCLOSE',self.request()[2])


class JournalDiagnosticTests(unittest.TestCase):
    def test_readonly_journal_returns_partial_fill_and_reduce_only_proofs(self):
        fixture=ReadOnlyFixture();before=deepcopy(fixture.state)
        for target in ('http.client.HTTPSConnection','socket.create_connection',
                       'hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body',
                       'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard=patch(target,side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            guard.start();self.addCleanup(guard.stop)
        with patch.object(m.PostgresJournal,'from_env',return_value=fixture):
            report=m.load_diagnostics(ENV)
        self.assertEqual(fixture.state,before)
        self.assertTrue(all(sql.lstrip().startswith('SELECT') for sql,_ in fixture.statements))
        card=report['buckets']['recent'][0]['cards'][0]
        self.assertEqual(card['entry_quantity'],'40')
        self.assertEqual(card['exit_quantity'],'0')
        self.assertEqual(card['actual_entry_price'],'10')
        self.assertEqual(card['source_event_id'],'filled-policy-1')
        self.assertTrue(card['protection_verified_at_snapshot'])
        self.assertEqual({o['leg']:o['reduce_only'] for o in card['open_orders']},
                         {'ENTRY':False,'STOP':True,'TAKE_PROFIT':True})
        self.assertGreater(report['buckets']['recent'][0]['evidence_age_ms'],0)
        self.assertEqual(report['exchange_calls'],0)
        self.assertEqual(report['order_requests_sent'],0)
        serialized=json.dumps(report)
        for secret in (A,B,ENV['HL_TESTNET_LONG_AGENT_ADDRESS'],ENV['HL_TESTNET_AGENT_KEY'],
                       ENV['HL_TESTNET_DATABASE_URL'],SECRET,'DO_NOT_DISCLOSE_USER','DO_NOT_DISCLOSE_PASSWORD'):
            self.assertNotIn(secret,serialized)
        self.assertEqual(report['database']['dbname'],'selected_testnet_db')
        self.assertEqual(report['config']['RENDER_GIT_COMMIT'],'e'*40)
    def test_short_fill_and_closure_are_real_saved_facts(self):
        from .test_card_lifecycle import fill,terminal
        state=state_from_case(q='100',side='SHORT',stop='100',take='100',expiry_seconds=60)
        b=state['bindings'][0];snap=state['evidence']['snapshot']
        snap['fills'].append(fill(b,'STOP',qty='100'))
        snap['terminal_orders'] += [terminal(b,'STOP','100'),terminal(b,'TAKE_PROFIT','0')]
        snap['open_orders']=[];snap['position_quantity']='0'
        report=m._bucket(state,life.digest(state),1,{B:'short_account'},T+60000)
        row=report['cards'][0]
        self.assertEqual(row['account_role'],'short_account')
        self.assertEqual((row['entry_quantity'],row['exit_quantity']),('100','100'))
        self.assertTrue(row['closure_verified'])
        self.assertFalse(row['protection_verified_at_snapshot'])
        self.assertEqual(row['state'],'CLOSED')
        self.assertEqual(row['open_orders'],[])
    def test_duplicate_saved_fill_does_not_inflate_weighted_price_or_quantity(self):
        state=ReadOnlyFixture().state
        state['evidence']['snapshot']['fills']*=2
        row=m._bucket(state,life.digest(state),1,{A:'long_account'},T)['cards'][0]
        self.assertEqual(row['entry_quantity'],'40')
        self.assertEqual(len(row['fills']),1)
    def test_changed_bucket_or_original_source_is_rejected(self):
        state=ReadOnlyFixture().state
        with self.assertRaises(ValueError):m._bucket(state,'f'*64,1,{},T)
        changed=deepcopy(state)
        cid=changed['bindings'][0]['card_id']
        changed['originals'][cid]['card']['prepared']['source']['entry']='999'
        with self.assertRaises(ValueError):m._bucket(changed,life.digest(changed),1,{},T)
    def test_configuration_values_are_allowlisted_not_arbitrary_environment(self):
        report=m._config({**ENV,'HL_TESTNET_FILLED_AFTER_EXIT_POLICY':'cancel_remainder_after_exit_v1',
                          'HL_TESTNET_APP_DELIVERY':'DO_NOT_DISCLOSE',
                          'HL_TESTNET_SAFETY_PIPELINE':'DO_NOT_DISCLOSE'})
        self.assertEqual(report['HL_TESTNET_FILLED_AFTER_EXIT_POLICY'],'cancel_remainder_after_exit_v1')
        self.assertEqual(report['HL_TESTNET_APP_DELIVERY'],'UNRECOGNIZED')
        self.assertTrue(report['safety_pipeline_configured'])
        self.assertNotIn('DO_NOT_DISCLOSE',json.dumps(report))


if __name__=='__main__':unittest.main()
