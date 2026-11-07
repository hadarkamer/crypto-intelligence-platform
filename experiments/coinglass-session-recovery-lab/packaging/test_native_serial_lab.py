"""Targeted adapter tests: no PostgreSQL, Chromium, source or external network."""
import ast
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import native_serial_lab as runner


def frozen_source():
    candidates = [Path('/lab/app/.github/labs/btc-full-flow/run.py'),
                  HERE.parents[1] / 'nov7_recovery_preparation/lab/app/.github/labs/btc-full-flow/run.py']
    return next(p for p in candidates if p.is_file()).read_bytes()


class AdapterTests(unittest.TestCase):
    def test_all_original_acceptance_assertions_are_preserved(self):
        data = frozen_source()
        before = ast.parse(data)
        after = ast.parse(runner.adapt_harness(data, runner.local_base(55432), 12345))
        assertions = lambda tree: [ast.dump(n, include_attributes=False) for n in ast.walk(tree) if isinstance(n, ast.Assert)]
        old = assertions(before)
        docker_only = [value for value in old if 'containers' in value]
        self.assertEqual(len(docker_only), 1)
        self.assertEqual([value for value in old if value not in docker_only], assertions(after))
        self.assertIn("len(rows)==19", runner.adapt_harness(data, runner.local_base(55432), 12345))

    def test_changed_source_cannot_be_adapted(self):
        with self.assertRaises(ValueError):
            runner.adapt_harness(frozen_source() + b'\n', runner.local_base(55432), 12345)

    def test_remote_dsn_and_invalid_ports_refused(self):
        for port in (0, 543, 65536, True, '5432'):
            with self.subTest(port=port), self.assertRaises(ValueError):
                runner.local_base(port)
        with self.assertRaises(ValueError):
            runner.adapt_harness(frozen_source(), 'postgresql://production.invalid/db', 12345)

    def test_child_environment_never_inherits_credentials_or_remote_config(self):
        supplied = {key: 'private-sentinel' for key in (
            'DATABASE_URL', 'PGHOST', 'COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON',
            'OPENAI_API_KEY', 'SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY', 'NODE_OPTIONS',
            'PYTHONPATH', 'QUEUE_LAB_NODE', 'COINGLASS_COLLECTOR_URL')}
        supplied['PATH'] = '/safe/bin'
        env = runner.clean_environment(Path('/lab'), Path('/lab/evidence/test'), supplied)
        self.assertNotIn('private-sentinel', env.values())
        self.assertEqual(env['PATH'], '/safe/bin')
        self.assertEqual(env['QUEUE_LAB_NODE'], '/opt/lab-node')
        self.assertNotIn('DATABASE_URL', env)

    def test_evidence_path_cannot_escape_owned_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'owned').mkdir()
            (root/'owned'/'escape').symlink_to(root)
            with self.assertRaises(ValueError):
                runner.beneath(root/'owned'/'escape'/'result', root/'owned')

    def test_pg_start_timeout_still_attempts_owned_stop_and_identity_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            pg = runner.NativePostgres(Path('/fake/bin'), Path(tmp), {}, 55432)
            pg.initialized = True
            pg.data.mkdir()
            (pg.data/'postmaster.pid').write_text('fixture')
            def identity():
                pg.pid, pg.created = 12345, 1.0
            pg.read_identity = Mock(side_effect=identity)
            with patch.object(runner.subprocess, 'run', side_effect=subprocess.TimeoutExpired('pg_ctl', 25)), \
                 patch.object(runner, 'matching_process', return_value=None):
                receipt = pg.cleanup()
            pg.read_identity.assert_called_once()
            self.assertEqual(receipt['stop_error_type'], 'TimeoutExpired')
            self.assertTrue(receipt['stopped'])

    def test_guard_receipts_require_all_source_contexts_and_reject_missing_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp); (evidence/'guards').mkdir()
            for n in range(20):
                (evidence/'guards'/f'python-{n}.json').write_text(json.dumps({'canary_blocked':1,'source_contexts_offline':int(n<19)}))
            with self.assertRaises(RuntimeError):
                runner.verify_guard_receipts(evidence)
            (evidence/'guards'/'node-1.json').write_text('{"canary_blocked":1}')
            self.assertEqual(runner.verify_guard_receipts(evidence)['source_contexts_offline'], 19)
            (evidence/'guards'/'node-1.json').write_text('{"canary_blocked":1,"denied_network":1}')
            with self.assertRaises(RuntimeError):
                runner.verify_guard_receipts(evidence)

    def test_setup_failure_cannot_skip_postgres_or_final_census_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab = Path(tmp); evidence = lab/'evidence'/'test'; evidence.mkdir(parents=True)
            fake_pg = Mock(pid=None)
            fake_pg.start.side_effect = RuntimeError('controlled startup failure')
            fake_pg.cleanup.return_value = {'stopped':True}
            lifecycle = types.SimpleNamespace(establish_subreaper=Mock(), cleanup=Mock(return_value=(True,0,False)))
            with patch.dict(sys.modules, {'collector_matrix':lifecycle}), \
                 patch.object(runner,'verify_inputs',return_value={}), \
                 patch.object(runner,'product_fingerprint',return_value={}), \
                 patch.object(runner,'check_fixed_ports'), patch.object(runner,'reserve_port',return_value=55432), \
                 patch.object(runner,'NativePostgres',return_value=fake_pg), \
                 patch.object(runner.OwnedProcesses,'cleanup',side_effect=RuntimeError('controlled cleanup fault')):
                code = runner.supervise(lab, evidence, Path('/fake/bin'), 60)
            self.assertEqual(code, 1)
            fake_pg.cleanup.assert_called_once()
            lifecycle.cleanup.assert_called_once()
            cleanup = json.loads((evidence/'native-cleanup.json').read_text())
            self.assertTrue(cleanup['final_owned_descendant_census']['verified_empty'])
            self.assertTrue(cleanup['owned_cluster_directory_removed'])
            self.assertTrue(cleanup['errors'])

    def test_node_guard_rejects_external_hosts_before_network(self):
        node = os.environ.get('CODEX_PRIMARY_RUNTIME_NODE', 'node')
        script = """const assert=require('node:assert/strict'); const g=require(process.argv[1]);
for(const host of ['example.invalid','10.0.0.1','169.254.169.254']) assert.throws(()=>g.checkHost(host));
assert.throws(()=>g.checkConnect(['443','example.invalid']));
assert.throws(()=>g.checkConnect([{host:'example.invalid',port:443}]));
assert.throws(()=>g.checkConnect([[{host:'example.invalid',port:443}]]));
assert.throws(()=>g.checkUrl('https://example.invalid/path'));
assert.throws(()=>g.checkUrl('wss://example.invalid/path',true));
g.checkConnect([{host:'127.0.0.1',port:55432}]); g.checkUrl('http://127.0.0.1:21879/health');
assert.equal(g.counts.canary_blocked,1); console.log('node guard canaries passed');"""
        result = subprocess.run([node,'-e',script,str(HERE/'serial_node_guard.cjs')],
                                env={'PATH':os.environ.get('PATH','/usr/bin:/bin')},
                                capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('canaries passed',result.stdout)

    def test_unstarted_monitor_and_directory_error_still_finish_process_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab=Path(tmp); evidence=lab/'evidence'/'test'; evidence.mkdir(parents=True)
            original=lab/runner.HARNESS/'run.py'; original.parent.mkdir(parents=True)
            original.write_bytes(frozen_source())
            fake_pg=types.SimpleNamespace(pid=12345,port=55432,start=Mock(),cleanup=Mock(return_value={'stopped':True}))
            child=Mock(pid=12346); child.poll.return_value=0
            monitor=Mock(); monitor.start.side_effect=RuntimeError('controlled thread start failure')
            monitor.join.side_effect=RuntimeError('cannot join unstarted thread')
            lifecycle=types.SimpleNamespace(establish_subreaper=Mock(),cleanup=Mock(return_value=(True,0,False)))
            with patch.dict(sys.modules,{'collector_matrix':lifecycle}), \
                 patch.object(runner,'verify_inputs',return_value={}), \
                 patch.object(runner,'product_fingerprint',return_value={}), \
                 patch.object(runner,'check_fixed_ports'),patch.object(runner,'reserve_port',return_value=55432), \
                 patch.object(runner,'NativePostgres',return_value=fake_pg), \
                 patch.object(runner.subprocess,'check_output',return_value='postgres (PostgreSQL) 16.10'), \
                 patch.object(runner.subprocess,'Popen',return_value=child), \
                 patch.object(runner.threading,'Thread',return_value=monitor), \
                 patch.object(runner.OwnedProcesses,'capture',return_value=[]), \
                 patch.object(runner.OwnedProcesses,'cleanup',return_value={'remaining_owned_pids':[]}), \
                 patch.object(runner.shutil,'rmtree',side_effect=OSError('controlled directory failure')):
                self.assertEqual(runner.supervise(lab,evidence,Path('/fake/bin'),60),1)
            fake_pg.cleanup.assert_called_once(); lifecycle.cleanup.assert_called_once()
            receipt=json.loads((evidence/'native-cleanup.json').read_text())
            self.assertTrue(receipt['final_owned_descendant_census']['verified_empty'])
            self.assertIn('monitor_join:RuntimeError',receipt['errors'])
            self.assertIn('cluster_directory:OSError',receipt['errors'])
            self.assertEqual(json.loads((evidence/'native-result.json').read_text())['status'],'failed')


if __name__ == '__main__':
    unittest.main(verbosity=2)
