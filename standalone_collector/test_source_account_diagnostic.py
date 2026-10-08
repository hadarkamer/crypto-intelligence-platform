"""Offline synthetic sessions and mocked HTTP only; never CoinGlass/OpenAI."""
import ast
import contextlib
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock,patch

import source_account_diagnostic as diagnostic

SESSION='synthetic-session-never-a-real-credential'
PRIVATE='PRIVATE_ACCOUNT_PROFILE_AND_ERROR'


class Response:
    def __init__(self,status=200,body=None):
        self.status_code=status
        self.body=json.dumps({'success':True,'data':PRIVATE}).encode() if body is None else body
        self.closed=False;self.reads=0
    @property
    def headers(self):raise AssertionError('Response headers must not be read')
    def __enter__(self):return self
    def __exit__(self,*args):self.closed=True
    def iter_content(self,chunk_size):
        for index in range(0,len(self.body),chunk_size):
            self.reads+=1
            yield self.body[index:index+chunk_size]


class AccountDiagnosticTests(unittest.TestCase):
    def check(self,response=None,env=None):
        get=Mock(return_value=Response() if response is None else response)
        environment={'COINGLASS_COOKIE_HEADER':'theme=dark; obe='+SESSION} if env is None else env
        result=diagnostic.check_account_response(env=environment,http_get=get)
        self.assertNotIn(SESSION,json.dumps(result));self.assertNotIn(PRIVATE,json.dumps(result))
        self.assertEqual((result['source_attempts'],result['ai_calls'],result['sheet_writes']),(0,0,0))
        return result,get
    def test_exact_frontend_request_and_encrypted_data_is_ignored(self):
        response=Response();result,get=self.check(response)
        self.assertEqual(result['code'],'source_account_api_accepted')
        self.assertIs(result['account_api_success'],True);self.assertEqual(result['http_status'],200)
        self.assertEqual(result['source_account_requests'],1);get.assert_called_once()
        self.assertEqual(get.call_args.args,('https://capi.coinglass.com/coin-community/api/userapi/info',))
        kwargs=get.call_args.kwargs;headers=kwargs['headers']
        self.assertEqual(headers['obe'],SESSION);self.assertEqual(headers['Accept'],'application/json')
        self.assertEqual(headers['language'],'en');self.assertEqual(headers['encryption'],'true')
        self.assertTrue(headers['cache-ts-v2'].isdigit())
        self.assertEqual(headers['Origin'],'https://www.coinglass.com')
        self.assertEqual(headers['Referer'],'https://www.coinglass.com/')
        self.assertEqual(headers['User-Agent'],'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36')
        self.assertEqual(set(headers),{'Accept','language','encryption','cache-ts-v2','obe','Origin','Referer','User-Agent'})
        self.assertFalse(kwargs['allow_redirects']);self.assertTrue(kwargs['stream'])
        self.assertEqual(kwargs['timeout'],(4,8));self.assertTrue(response.closed)
        self.assertNotIn('account_recognized',result)
    def test_success_does_not_require_or_expose_profile_data(self):
        for data in (PRIVATE,{'name':PRIVATE,'email':PRIVATE},None,[]):
            with self.subTest(data_type=type(data).__name__):
                result,_=self.check(Response(body=json.dumps({'success':True,'data':data}).encode()))
                self.assertIs(result['account_api_success'],True)
                self.assertEqual(result['code'],'source_account_api_accepted')
    def test_http_200_unsuccessful_is_not_reported_as_expired(self):
        result,_=self.check(Response(body=json.dumps({'success':False,'msg':PRIVATE,'data':PRIVATE}).encode()))
        self.assertIs(result['account_api_success'],False)
        self.assertEqual(result['code'],'source_account_response_unsuccessful')
        self.assertNotIn('expired',json.dumps(result));self.assertNotIn('invalid_session',json.dumps(result))
    def test_missing_or_tracking_only_session_opens_nothing(self):
        for environment in ({},{'COINGLASS_COOKIE_HEADER':' '},{'COINGLASS_COOKIE_HEADER':'_ga=synthetic'}):
            with self.subTest(environment=tuple(environment)):
                result,get=self.check(env=environment);get.assert_not_called()
                self.assertEqual(result['source_account_requests'],0);self.assertIsNone(result['account_api_success'])
    def test_header_preserves_obe_and_last_cookie_like_capture(self):
        result,get=self.check(env={'COINGLASS_COOKIE_HEADER':'obe=old; obe= '+SESSION+' ; preference=x'})
        self.assertEqual(get.call_args.kwargs['headers']['obe'],SESSION)
        self.assertEqual(result['code'],'source_account_api_accepted')
    def test_percent_decoding_matches_frontend_once_and_preserves_plus(self):
        for raw,expected in (('token%3D%3D','token=='),('token+with+plus','token+with+plus'),
                             ('token%2B','token+'),('token%253D','token%3D'),
                             ('%41%Q0','%41%Q0'),('%FF','%FF')):
            with self.subTest(raw=raw):
                _,get=self.check(env={'COINGLASS_COOKIE_HEADER':'obe='+raw})
                self.assertEqual(get.call_args.kwargs['headers']['obe'],expected)
    def test_header_precedes_storage_state(self):
        state=json.dumps({'cookies':[{'name':'obe','value':'wrong-synthetic','domain':'.coinglass.com','path':'/'}]})
        _,get=self.check(env={'COINGLASS_COOKIE_HEADER':'obe='+SESSION,'COINGLASS_STORAGE_STATE_JSON':state})
        self.assertEqual(get.call_args.kwargs['headers']['obe'],SESSION)
    def test_storage_state_fallback_matches_coinglass_only(self):
        cookies=[{'name':'obe','value':SESSION,'domain':'.coinglass.com','path':'/'},
                 {'name':'obe','value':'wrong-synthetic','domain':'.example.org','path':'/'},
                 {'name':'obe','value':'wrong-synthetic','domain':'.coinglass.com','path':'/unrelated'}]
        _,get=self.check(env={'COINGLASS_COOKIE_HEADER':' ','COINGLASS_STORAGE_STATE_JSON':json.dumps({'cookies':cookies,'origins':[{'localStorage':PRIVATE}]})})
        self.assertEqual(get.call_args.kwargs['headers']['obe'],SESSION)
    def test_invalid_header_cannot_fall_back_or_send_control_characters(self):
        state=json.dumps({'cookies':[{'name':'obe','value':SESSION,'domain':'.coinglass.com','path':'/'}]})
        for header in ('Cookie: obe='+SESSION,'obe='+SESSION+'\r\nX-Test: secret','obe='+SESSION+'\x00',
                       'obe='+SESSION+'%0D%0AX-Test: secret','obe='+SESSION+'%00','obe=','obe='+('X'*65536)):
            with self.subTest(header_length=len(header)):
                result,get=self.check(env={'COINGLASS_COOKIE_HEADER':header,'COINGLASS_STORAGE_STATE_JSON':state})
                get.assert_not_called();self.assertEqual(result['source_account_requests'],0)
    def test_total_header_limit_is_enforced_before_request(self):
        _,get=self.check(env={'COINGLASS_COOKIE_HEADER':'obe='+('X'*(65536-4))})
        get.assert_not_called()
    def test_malformed_or_foreign_storage_state_does_not_request(self):
        for state in ('{', '[]', '{}',json.dumps({'cookies':[{'name':'obe','value':SESSION,'domain':'.example.org','path':'/'}]}),
                      json.dumps({'cookies':[{'name':'obe','value':SESSION,'domain':{'private':PRIVATE},'path':'/'}]}),
                      json.dumps({'cookies':[{'name':'obe','value':SESSION,'domain':'.coinglass.com','path':[]}]}),
                      '\ud800','X'*65537):
            with self.subTest(length=len(state)):
                _,get=self.check(env={'COINGLASS_STORAGE_STATE_JSON':state});get.assert_not_called()
    def test_denied_limited_and_other_statuses_do_not_read_private_body(self):
        for status,code in ((401,'source_account_request_denied'),(403,'source_account_request_denied'),
                            (429,'source_account_rate_limited'),(500,'source_account_check_unavailable')):
            with self.subTest(status=status):
                response=Response(status,PRIVATE.encode());result,get=self.check(response)
                self.assertEqual(result['code'],code);self.assertEqual(result['http_status'],status)
                self.assertIsNone(result['account_api_success']);self.assertEqual(response.reads,0)
                self.assertTrue(response.closed);get.assert_called_once()
    def test_redirect_is_never_followed_or_read(self):
        for status in (301,302,307,308):
            with self.subTest(status=status):
                response=Response(status,PRIVATE.encode());result,get=self.check(response)
                self.assertEqual(result['code'],'source_account_redirect_rejected')
                self.assertFalse(get.call_args.kwargs['allow_redirects']);self.assertEqual(response.reads,0)
                get.assert_called_once();self.assertTrue(response.closed)
    def test_invalid_http_status_is_never_exposed(self):
        for status in (PRIVATE,True,0,700):
            with self.subTest(status_type=type(status).__name__):
                result,_=self.check(Response(status,PRIVATE.encode()))
                self.assertIsNone(result['http_status']);self.assertEqual(result['code'],'source_account_check_unavailable')
    def test_malformed_or_non_boolean_success_stays_unknown(self):
        for body in (PRIVATE.encode(),b'[]',b'{}',b'{"success":1}',b'{"success":"true"}',b'{"success":null}'):
            with self.subTest(length=len(body)):
                response=Response(body=body);result,_=self.check(response)
                self.assertIsNone(result['account_api_success']);self.assertEqual(result['code'],'source_account_check_unavailable')
                self.assertTrue(response.closed)
    def test_oversized_body_stops_stream_and_closes(self):
        response=Response(body=b'X'*65536);result,_=self.check(response)
        self.assertEqual(result['code'],'source_account_check_unavailable');self.assertIsNone(result['account_api_success'])
        self.assertEqual(response.reads,9);self.assertTrue(response.closed)
    def test_exact_body_limit_accepts_outer_success_only(self):
        body=b'{"success":true}'+b' '*(32768-len(b'{"success":true}'))
        result,_=self.check(Response(body=body));self.assertEqual(result['code'],'source_account_api_accepted')
    def test_transport_errors_are_redacted_without_retry_or_output(self):
        get=Mock(side_effect=RuntimeError(PRIVATE+' '+SESSION));output=io.StringIO()
        with contextlib.redirect_stdout(output),contextlib.redirect_stderr(output):
            result=diagnostic.check_account_response(env={'COINGLASS_COOKIE_HEADER':'obe='+SESSION},http_get=get)
        get.assert_called_once();self.assertEqual(result['code'],'source_account_check_unavailable')
        self.assertEqual(output.getvalue(),'');self.assertNotIn(PRIVATE,json.dumps(result));self.assertNotIn(SESSION,json.dumps(result))
    def test_main_prints_only_fixed_report_not_private_response(self):
        get=Mock(return_value=Response(body=json.dumps({'success':True,'data':{'name':PRIVATE,'token':SESSION}}).encode()))
        fake_requests=types.SimpleNamespace(get=get);output=io.StringIO()
        with patch.dict('os.environ',{'COINGLASS_COOKIE_HEADER':'obe='+SESSION},clear=True),patch.dict(sys.modules,{'requests':fake_requests}),contextlib.redirect_stdout(output),contextlib.redirect_stderr(output):
            diagnostic.main()
        text=output.getvalue();self.assertTrue(text.startswith('COINGLASS_ACCOUNT_DIAGNOSTIC '))
        self.assertEqual(len(text.splitlines()),1);self.assertNotIn(PRIVATE,text);self.assertNotIn(SESSION,text)
        report=json.loads(text.split(' ',1)[1]);self.assertEqual(report['code'],'source_account_api_accepted')
        self.assertEqual(set(report),{'checked_at','source_account_requests','source_attempts','ai_calls','sheet_writes','http_status','account_api_success','code'})


class StartupFlagTests(unittest.TestCase):
    def startup(self,flag=None,argv=None):
        # Execute only the real app entrypoint with fake service/provider modules.
        # No database, model, browser or session is imported by the test.
        source=Path(__file__).with_name('app.py').read_text(encoding='utf-8')
        entrypoint=ast.parse(source).body[-1];events=[]
        environment={} if flag is None else {'COINGLASS_ACCOUNT_DIAGNOSTIC_ON_START':flag}
        fake_diagnostic=types.SimpleNamespace(main=lambda:events.append('account'))
        fake_web=types.SimpleNamespace(run_app=lambda *args,**kwargs:events.append('serve'))
        namespace={'__name__':'__main__','sys':types.SimpleNamespace(argv=argv or ['app.py']),
            'os':types.SimpleNamespace(getenv=environment.get),
            'create_app':lambda:events.append('create') or object(),
            'deployment_check':lambda:events.append('config'),
            'probe':lambda:events.append('probe')}
        with patch.dict(sys.modules,{'source_account_diagnostic':fake_diagnostic,'aiohttp':types.SimpleNamespace(web=fake_web)}):
            exec(compile(ast.Module(body=[entrypoint],type_ignores=[]),'app-startup','exec'),namespace)
        return events
    def test_flag_defaults_off_and_other_values_are_off(self):
        for value in (None,'','false','1','yes'):
            with self.subTest(value=value):self.assertEqual(self.startup(value),['create','serve'])
    def test_explicit_flag_runs_once_before_application_creation(self):
        for value in ('true','TRUE',' true '):
            with self.subTest(value=value):self.assertEqual(self.startup(value),['account','create','serve'])
    def test_other_cli_modes_do_not_trigger_account_request(self):
        self.assertEqual(self.startup('true',['app.py','--source-probe']),['probe'])
        for mode in ('--probe','--check-config'):
            with self.subTest(mode=mode):self.assertEqual(self.startup('true',['app.py',mode]),['config'])


if __name__=='__main__':unittest.main()
