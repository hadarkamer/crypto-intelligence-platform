"""Isolation gates for the test-only Render build entrypoint."""
import os
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import render_hover_check as check


class IsolationTests(unittest.TestCase):
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
