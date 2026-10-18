"""Isolation gates for the test-only Render build entrypoint."""
import os
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import render_hover_check as check


class IsolationTests(unittest.TestCase):
    def test_focused_repairs_only_two_cases_and_never_receive_ai_or_db(self):
        import model3_hover_diagnostic as diagnostic
        self.assertEqual(diagnostic.FOCUSED_CASES,((2,'12H'),(3,'48H')))
        with patch.dict(os.environ, {'HOVER_CHECK_MODE':'focused','COINGLASS_COOKIE_HEADER':'synthetic=session',
                'OPENAI_API_KEY':'synthetic-ai','DATABASE_URL':'synthetic-db'},clear=True), \
                patch.object(check.subprocess,'run') as run,patch.object(check,'publish_status') as publish:
            check.main()
            self.assertIn('--focused',run.call_args.args[0])
            self.assertEqual(run.call_args.kwargs['env']['OPENAI_API_KEY'],'')
            self.assertEqual(run.call_args.kwargs['env']['DATABASE_URL'],'')
            publish.assert_called_once_with(focused=True)

    def test_ai_key_reaches_only_explicit_ai_child(self):
        with patch.dict(os.environ, {'HOVER_CHECK_MODE':'gpt-check',
                'COINGLASS_COOKIE_HEADER':'synthetic=session','OPENAI_API_KEY':'synthetic-ai',
                'OPENAI_MARKET_SCANNER_MODEL':'test-model','DATABASE_URL':'synthetic-db',
                'SUPABASE_SERVICE_ROLE_KEY':'synthetic-secret'}, clear=True), \
                patch.object(check.subprocess,'run') as run, patch.object(check,'publish_status') as publish:
            check.main()
            self.assertEqual(run.call_count,3)
            for call in run.call_args_list[:2]:
                self.assertEqual(call.kwargs['env']['OPENAI_API_KEY'],'')
                self.assertNotIn('COINGLASS_COOKIE_HEADER',call.kwargs['env'])
            env=run.call_args.kwargs['env']
            self.assertEqual(env['OPENAI_API_KEY'],'synthetic-ai')
            self.assertEqual(env['OPENAI_MARKET_SCANNER_MODEL'],'test-model')
            self.assertEqual(env['DATABASE_URL'],'')
            self.assertNotIn('SUPABASE_SERVICE_ROLE_KEY',env)
            self.assertIn('hover_ai_check.py',str(run.call_args.args[0]))
            publish.assert_called_once_with(ai_check=True)

    def test_ai_missing_key_stops_before_source_or_build(self):
        with patch.dict(os.environ, {'HOVER_CHECK_MODE':'gpt-check','COINGLASS_COOKIE_HEADER':'x=y'}, clear=True), \
                patch.object(check.subprocess,'run') as run:
            with self.assertRaisesRegex(RuntimeError,'key missing'):check.main()
            run.assert_not_called()

    def test_api_transport_blocks_other_destinations_redirects_and_retries(self):
        from hover_ai_check import OneRequest, ENDPOINT
        post=Mock(return_value=Mock(status_code=200));guard=OneRequest(post)
        with self.assertRaises(RuntimeError):guard('https://example.invalid',allow_redirects=False)
        with self.assertRaises(RuntimeError):guard(ENDPOINT)
        self.assertEqual(guard.calls,0)
        guard(ENDPOINT,allow_redirects=False)
        with self.assertRaises(RuntimeError):guard(ENDPOINT,allow_redirects=False)
        self.assertEqual(guard.calls,1);post.assert_called_once()

    def test_transport_failure_still_consumes_one_request_budget(self):
        from hover_ai_check import OneRequest, ENDPOINT
        post=Mock(side_effect=OSError('synthetic'));guard=OneRequest(post)
        with self.assertRaises(OSError):guard(ENDPOINT,allow_redirects=False)
        with self.assertRaises(RuntimeError):guard(ENDPOINT,allow_redirects=False)
        post.assert_called_once()

    def test_ai_runner_exact_cases_continues_after_timeout_without_retry(self):
        import hover_ai_check as ai
        calls=[]
        def probe(command,env,timeout):
            index=command.index('--single');calls.append((int(command[index+1]),command[index+2]))
            self.assertEqual(timeout,300);self.assertEqual(env['DATABASE_URL'],'')
            if len(calls)==1:raise subprocess.TimeoutExpired(command,timeout)
            dest=Path(command[-1]);dest.mkdir(parents=True)
            (dest/'report.json').write_text(json.dumps({'status':'validated','ai_calls':1}))
        with tempfile.TemporaryDirectory() as temp, \
                patch('sys.argv',['check','--output',temp+'/cases']), \
                patch('hover_source_check.run_probe',side_effect=probe),patch.object(ai,'emit'):
            self.assertEqual(ai.main(),1)
        self.assertEqual(calls,[(1,'12H'),(2,'24H'),(3,'48H')])

    def test_ai_runner_stops_on_auth_error(self):
        import hover_ai_check as ai
        def probe(command,env,timeout):
            dest=Path(command[-1]);dest.mkdir(parents=True)
            (dest/'report.json').write_text(json.dumps({'status':'failed','code':'analysis_unauthorized'}))
        with tempfile.TemporaryDirectory() as temp, \
                patch('sys.argv',['check','--output',temp+'/cases']), \
                patch('hover_source_check.run_probe',side_effect=probe) as run,patch.object(ai,'emit'):
            self.assertEqual(ai.main(),1);run.assert_called_once()

    def test_ai_public_status_is_accurate_and_contains_no_results(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(check,'PUBLIC',Path(temp)/'public'):
            check.publish_status(ai_check=True)
            page=(check.PUBLIC/'index.html').read_text()
            self.assertIn('At most three AI requests',page)
            self.assertNotIn('No application database, AI calls',page)
            self.assertEqual([p.name for p in check.PUBLIC.iterdir()],['index.html'])

    def test_models12_runs_six_distinct_cases_and_continues_after_one_failure(self):
        import model3_hover_diagnostic as diagnostic
        calls=[]
        def probe(command,env):
            model=int(command[command.index('--model')+1]);tf=command[command.index('--single')+1]
            calls.append((model,tf))
            if len(calls)==1:raise subprocess.TimeoutExpired(command,180)
            dest=Path(command[-1]);dest.mkdir(parents=True)
            (dest/'report.json').write_text(json.dumps({'model':model,'timeframe':tf,'status':'captured'}))
        with tempfile.TemporaryDirectory() as temp, \
                patch('sys.argv',['check','--output',temp+'/cases','--models12']), \
                patch('hover_source_check.run_probe',side_effect=probe),patch.object(diagnostic,'emit'):
            self.assertEqual(diagnostic.main(),1)
        self.assertEqual(calls,[(m,t) for m in (1,2) for t in ('12H','24H','48H')])
    def test_models12_selects_only_pending_cases_without_ai_or_database(self):
        with patch.dict(os.environ, {'HOVER_CHECK_MODE':'models12','COINGLASS_COOKIE_HEADER':'synthetic=session','OPENAI_API_KEY':'synthetic-ai'},clear=True), \
                patch.object(check.subprocess,'run') as run, patch.object(check,'publish_status') as publish:
            check.main()
            command=run.call_args.args[0]
            self.assertIn('--models12',command)
            self.assertEqual(run.call_args.kwargs['env']['OPENAI_API_KEY'],'')
            self.assertEqual(run.call_args.kwargs['env']['DATABASE_URL'],'')
            publish.assert_called_once_with(diagnostic_only=True,models12=True)
    def test_model3_diagnostic_uses_only_its_scoped_runner(self):
        with patch.dict(os.environ, {'HOVER_CHECK_MODE': 'model3-diagnostic',
                    'COINGLASS_COOKIE_HEADER': 'synthetic=session'}, clear=True), \
                patch.object(check.subprocess, 'run') as run, \
                patch.object(check, 'publish_status') as publish:
            check.main()
            self.assertEqual(run.call_count, 3)
            calls = str(run.call_args_list)
            self.assertIn('model3_hover_diagnostic.py', calls)
            self.assertNotIn('hover_source_check.py', calls)
            publish.assert_called_once_with(diagnostic_only=True)

    def test_only_source_session_reaches_source_child(self):
        with patch.dict(os.environ, {'PATH': '/bin', 'DATABASE_URL': 'synthetic-db',
                'OPENAI_API_KEY': 'synthetic-ai', 'SUPABASE_SERVICE_ROLE_KEY': 'synthetic-db',
                'COINGLASS_COOKIE_HEADER': 'synthetic=session',
                'COLLECTION_BRIDGE_ENABLED': '1'}, clear=True):
            build = check.child_environment()
            source = check.child_environment(source=True)
        self.assertNotIn('COINGLASS_COOKIE_HEADER', build)
        self.assertEqual(source['COINGLASS_COOKIE_HEADER'], 'synthetic=session')
        for env in (build, source):
            self.assertEqual(env['DATABASE_URL'], '')
            self.assertEqual(env['OPENAI_API_KEY'], '')
            self.assertEqual(env['COLLECTION_BRIDGE_ENABLED'], '0')
            self.assertNotIn('SUPABASE_SERVICE_ROLE_KEY', env)

    def test_missing_source_session_stops_before_subprocess(self):
        with patch.dict(os.environ, {'HOVER_CHECK_MODE': 'source'}, clear=True), \
                patch.object(check.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'session missing'):
                check.main()
            run.assert_not_called()

    def test_prepare_never_invokes_source_or_service(self):
        with patch.dict(os.environ, {}, clear=True), \
                patch.object(check.subprocess, 'run') as run, \
                patch.object(check, 'publish_status') as publish:
            check.main()
            self.assertEqual(run.call_count, 2)
            calls = str(run.call_args_list)
            self.assertNotIn('hover_source_check.py', calls)
            self.assertNotIn('app.py', calls)
            publish.assert_called_once_with(source_passed=False)

    def test_source_failure_does_not_publish_success(self):
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(check, 'PRIVATE', Path(temp) / 'new-run'), \
                patch.dict(os.environ, {'HOVER_CHECK_MODE': 'source',
                    'COINGLASS_COOKIE_HEADER': 'synthetic=session'}, clear=True), \
                patch.object(check.subprocess, 'run', side_effect=[None, None,
                    subprocess.CalledProcessError(1, ['source-check'])]) as run, \
                patch.object(check, 'publish_status') as publish:
            with self.assertRaises(subprocess.CalledProcessError):
                check.main()
            self.assertEqual(run.call_count, 3)
            publish.assert_not_called()

    def test_existing_output_blocks_duplicate(self):
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(check, 'PRIVATE', Path(temp)), \
                patch.dict(os.environ, {'HOVER_CHECK_MODE': 'source',
                    'COINGLASS_COOKIE_HEADER': 'synthetic=session'}, clear=True), \
                patch.object(check.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'duplicate'):
                check.main()
            run.assert_not_called()

    def test_public_output_contains_no_private_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            public = Path(temp) / 'public'
            public.mkdir()
            (public / 'stale-report.json').write_text('synthetic-private-data')
            private = Path(temp) / 'private'
            private.mkdir()
            (private / 'report.json').write_text('synthetic-private-data')
            with patch.object(check, 'PUBLIC', public), patch.object(check, 'PRIVATE', private):
                check.publish_status()
            self.assertEqual([p.name for p in public.iterdir()], ['index.html'])
            self.assertNotIn('synthetic-private-data', (public / 'index.html').read_text())
            self.assertTrue((private / 'report.json').exists())


if __name__ == '__main__':
    unittest.main()
