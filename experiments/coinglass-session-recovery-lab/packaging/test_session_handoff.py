"""New bounded-TTY and synthetic multi-cookie tests; never starts a browser."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import pty
import select
import signal
import subprocess
import sys
import termios
import time
import unittest
import types
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('session_handoff', HERE/'session_handoff.py')
h = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(h)
RUNTIME = h.DEFAULT_RUNTIME
CANARY = 'PUBLIC_SYNTHETIC_AMBIENT_CANARY'


def tty_client(args, *, live_validation=False):
    runtime, max_bytes, timeout = Path(args[0]), int(args[1]), float(args[2])
    total_read = 0
    original_read = os.read
    def observed_read(fd, size):
        nonlocal total_read
        result = original_read(fd, size)
        total_read += len(result)
        return result
    started = time.monotonic()
    try:
        os.read = observed_read
        try: raw = h.read_tty(max_bytes=max_bytes, timeout=timeout, live_candidate=live_validation)
        finally: os.read = original_read
        report = ({'status':'passed','candidate_syntax_valid':True,'live_enabled':False,
                   'source_requests':0,'browser_launches':0,'ai_calls':0}
                  if live_validation else h.run_handoff(raw, runtime))
        raw = None
    except h.Rejected as exc: report = {'status':'blocked','reason':exc.code,'live_enabled':False}
    except BaseException: report = {'status':'blocked','reason':'test_client_failed','live_enabled':False}
    report.update(tty_bytes_read=total_read, elapsed_seconds=round(time.monotonic()-started,3))
    print(json.dumps(report,separators=(',',':')),flush=True)


def tty_case(payload, *, limit=h.MAX_TTY_BYTES, timeout=.5, echo=True, live_validation=False):
    master, slave = pty.openpty()
    initial = termios.tcgetattr(slave)
    if not echo:
        initial[3] &= ~termios.ECHO
        termios.tcsetattr(slave, termios.TCSANOW, initial)
    initial = termios.tcgetattr(slave)
    command = ['/usr/bin/setsid','--ctty',sys.executable,'-I','-B',str(Path(__file__).resolve()),
               '--tty-live-validation-client' if live_validation else '--tty-client',str(RUNTIME),str(limit),str(timeout)]
    proc = subprocess.Popen(command,stdin=slave,stdout=slave,stderr=slave,close_fds=True)
    output = b''
    sent = False
    echo_disabled = False
    started = time.monotonic()
    try:
        while proc.poll() is None:
            if time.monotonic()-started > 5: raise AssertionError('test_client_deadline')
            if select.select([master],[],[],.02)[0]:
                try: output += os.read(master,8192)
                except OSError: break
            if not sent and (h.LIVE_PROMPT if live_validation else h.PROMPT) in output:
                echo_disabled = not bool(termios.tcgetattr(slave)[3] & (termios.ECHO|termios.ECHONL))
                if payload is not None: os.write(master,payload)
                sent = True
        proc.wait(timeout=2)
        while select.select([master],[],[],.02)[0]:
            try:
                chunk = os.read(master,8192)
                if not chunk: break
                output += chunk
            except OSError: break
        restored = termios.tcgetattr(slave) == initial
        start = output.find(b'{')
        return json.loads(output[start:].decode().strip()),output,echo_disabled,restored
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid,signal.SIGKILL)
            proc.wait(timeout=2)
        os.close(master)
        os.close(slave)


class HandoffTests(unittest.TestCase):
    def assert_safe(self,report,output):
        self.assertFalse(report['live_enabled'])
        self.assertFalse(any(value.encode() in output for value in h.FIXTURES.values()),'payload leaked')
        self.assertNotIn(CANARY.encode(),output)
        self.assertEqual(report.get('source_requests',0),0)
        self.assertEqual(report.get('ai_calls',0),0)
        self.assertEqual(report.get('browser_launches',0),0)

    def test_single_and_full_headers_use_installed_conversion_in_isolated_child(self):
        for fixture_id,count in (('single_obe',1),('full_header',4)):
            with self.subTest(fixture=fixture_id):
                before=dict(os.environ)
                with mock.patch.dict(os.environ,{'OPENAI_API_KEY':CANARY,'DATABASE_URL':CANARY,
                       'COINGLASS_STORAGE_STATE_JSON':CANARY,'COINGLASS_COOKIE_HEADER':CANARY,
                       'HTTPS_PROXY':CANARY,'PYTHONPATH':CANARY}):
                    report=h.run_handoff(h.FIXTURES[fixture_id],RUNTIME)
                self.assertEqual(dict(os.environ),before)
                self.assertEqual(report['cookie_count'],count)
                for flag in ('conversion_verified','environment_isolated','argv_safe','child_reaped'):
                    self.assertTrue(report[flag])
                self.assert_safe(report,json.dumps(report).encode())

    def test_structure_and_all_nonfixture_values_rejected(self):
        cases=[('', 'input_missing'),('Cookie: obe=PUBLIC_ONLY','field_prefix_rejected'),
               ('Set-Cookie: obe=PUBLIC_ONLY','field_prefix_rejected'),
               ('obe=PUBLIC_ONLY; obe=OTHER_PUBLIC','duplicate_cookie_name'),
               ('obe=', 'input_malformed'),('obe=PUBLIC_ONLY\n','input_malformed'),
               ('broken','input_malformed'),('fixture_language=en','session_cookie_missing'),
               ('obe=UNRECOGNIZED_SYNTHETIC_VALUE','synthetic_fixture_required'),
               ('x'*h.MAX_TTY_BYTES,'input_too_large')]
        for raw,code in cases:
            with self.subTest(code=code):
                with self.assertRaises(h.Rejected) as caught: h.validate_fixture(raw)
                self.assertEqual(caught.exception.code,code)

    def test_valid_tty_full_header_noecho_and_restore(self):
        report,output,noecho,restored=tty_case(h.FIXTURES['full_header'].encode()+b'\n')
        self.assertEqual(report['status'],'passed')
        self.assertEqual(report['cookie_count'],4)
        self.assertTrue(noecho and restored)
        self.assert_safe(report,output)

    def test_exact_read_budget_accepts_newline_but_does_not_overread(self):
        raw=h.FIXTURES['single_obe'].encode()
        for limit,expected in ((len(raw)+1,'passed'),(len(raw),'blocked')):
            with self.subTest(limit=limit):
                report,output,noecho,restored=tty_case(raw+b'\n',limit=limit)
                self.assertEqual(report['status'],expected)
                self.assertLessEqual(report['tty_bytes_read'],limit)
                if expected=='blocked': self.assertEqual(report['reason'],'input_too_large')
                self.assertTrue(noecho and restored)
                self.assert_safe(report,output)

    def test_oversized_tty_stops_at_budget_and_flushes_pending_input(self):
        report,output,noecho,restored=tty_case(b'x'*256+b'\n',limit=32)
        self.assertEqual(report['reason'],'input_too_large')
        self.assertEqual(report['tty_bytes_read'],32)
        self.assertTrue(noecho and restored)
        self.assert_safe(report,output)

    def test_tty_timeout_and_original_echo_false(self):
        report,output,noecho,restored=tty_case(None,timeout=.12,echo=False)
        self.assertEqual(report['reason'],'input_timeout')
        self.assertEqual(report['tty_bytes_read'],0)
        self.assertLess(report['elapsed_seconds'],1)
        self.assertTrue(noecho and restored)
        self.assert_safe(report,output)

    def test_missing_eof_interrupt_and_controls_restore_tty(self):
        for payload,code in ((b'\n','input_missing'),(b'\x04','input_missing'),
                             (b'\x03','input_interrupted'),(b'obe=x\t','input_malformed')):
            with self.subTest(code=code):
                report,output,noecho,restored=tty_case(payload)
                self.assertEqual(report['reason'],code)
                self.assertTrue(noecho and restored)
                self.assert_safe(report,output)

    def test_no_controlling_tty_has_no_stdin_fallback(self):
        result=subprocess.run([sys.executable,'-I','-B',str(HERE/'session_handoff.py'),'--tty',
                    '--runtime',str(RUNTIME)],input=b'obe=PUBLIC_PIPE_CANARY\n',capture_output=True,
                    start_new_session=True,timeout=3)
        report=json.loads(result.stdout)
        self.assertEqual(report['reason'],'secure_tty_required')
        self.assertEqual(result.stderr,b'')
        self.assert_safe(report,result.stdout+result.stderr)

    def test_environment_constructor_does_not_copy_ambient_keys(self):
        environment=h.fixture_environment('full_header',1)
        self.assertEqual(set(environment),{'LANG','COLLECTION_HEATMAP_MODEL','COINGLASS_COOKIE_HEADER'})
        self.assertEqual(environment['COLLECTION_HEATMAP_MODEL'],'1')

    def test_cli_cannot_enable_real_credentials_or_log_unknown_value(self):
        for args in (['--live'],['--cookie',CANARY],['--fixture-id',CANARY]):
            result=subprocess.run([sys.executable,'-I','-B',str(HERE/'session_handoff.py'),*args],
                                   capture_output=True,timeout=3)
            report=json.loads(result.stdout)
            self.assertEqual(report['reason'],'invalid_arguments')
            self.assertEqual(result.stderr,b'')
            self.assert_safe(report,result.stdout)


class SafeResult(unittest.TextTestResult):
    def _exc_info_to_string(self,err,test): return 'Failure details redacted; inspect the named synthetic test.'


class LiveInputTests(unittest.TestCase):
    """Changed-input boundary only, always PUBLIC fixtures and mocked supervisor."""
    PUBLIC_HEADER = 'fixture_session=PUBLIC_SYNTHETIC_LIVE_INPUT; fixture_empty=; fixture_equals=a=b; fixture_session=PUBLIC_REPEAT'

    def test_header_syntax_accepts_loader_compatible_pairs_without_obe(self):
        self.assertIsNone(h.validate_header(self.PUBLIC_HEADER))
        self.assertIsNone(h.validate_header('fixture_empty='))
        with self.assertRaises(h.Rejected) as error:
            h.validate_fixture(self.PUBLIC_HEADER)
        self.assertIn(error.exception.code, {'input_malformed','duplicate_cookie_name','session_cookie_missing'})
        self.assertEqual(h.validate_fixture(h.FIXTURES['full_header']), 'full_header')

    def test_header_syntax_rejects_fields_controls_and_malformed_pairs_without_echo(self):
        for raw in ('', 'Cookie: '+self.PUBLIC_HEADER, 'Set-Cookie: '+self.PUBLIC_HEADER,
                    'fixture=PUBLIC\r\nother=PUBLIC', 'fixture key=PUBLIC', 'fixture', '=PUBLIC',
                    'fixture=PUBLIC; ', 'x'*h.MAX_TTY_BYTES):
            with self.subTest(case_length=len(raw)), self.assertRaises(h.Rejected) as error:
                h.validate_header(raw)
            self.assertNotIn('PUBLIC', str(error.exception))

    def test_live_validation_tty_is_noecho_restored_and_never_calls_source(self):
        report,output,noecho,restored=tty_case(self.PUBLIC_HEADER.encode()+b'\n',live_validation=True)
        self.assertEqual(report['status'],'passed')
        self.assertTrue(report['candidate_syntax_valid'])
        self.assertTrue(noecho and restored)
        self.assertNotIn(self.PUBLIC_HEADER.encode(),output)
        self.assertNotIn(b'PUBLIC_SYNTHETIC_LIVE_INPUT',output)
        self.assertEqual(report['source_requests'],0)
        self.assertEqual(report['browser_launches'],0)

    def test_live_validation_tty_timeout_still_restores_without_source(self):
        report,output,noecho,restored=tty_case(None,timeout=.12,live_validation=True)
        self.assertEqual(report['reason'],'input_timeout')
        self.assertTrue(noecho and restored)
        self.assertEqual(report['tty_bytes_read'],0)
        self.assertNotIn(self.PUBLIC_HEADER.encode(),output)

    def test_live_flag_cannot_accept_cookie_arguments_or_runtime_override(self):
        self.assertEqual(h.arguments(['--live-tty']), (h.DEFAULT_RUNTIME,'live-tty',False))
        for argv in (['--live-tty','--runtime','/other'], ['--live-tty','--cookie',self.PUBLIC_HEADER],
                     ['--live-tty','--child'], ['--live-tty','--fixture-id','full_header'],
                     ['--live-tty','--live-tty']):
            with self.assertRaises(h.Rejected): h.arguments(argv)

    def test_live_handoff_starts_deadline_before_input_and_calls_supervisor_once(self):
        events=[]
        def read(**kwargs):
            events.append('tty'); self.assertEqual(kwargs,{'live_candidate':True})
            return self.PUBLIC_HEADER
        report={'format':'collector-live-supervised.v1','status':'stopped','reason':'public_fixture_test'}
        def supervise(raw,started):
            events.append('supervisor'); self.assertEqual(started,123.5)
            self.assertEqual(raw,self.PUBLIC_HEADER); self.assertNotEqual(os.environ.get('COINGLASS_COOKIE_HEADER'),raw)
            return report
        fake=types.SimpleNamespace(live_single=mock.Mock(side_effect=supervise),
                                   sanitize_live_supervisor_report=mock.Mock(side_effect=lambda value:value))
        with mock.patch.dict(sys.modules,{'collector_matrix':fake}), \
             mock.patch.object(h.time,'monotonic',side_effect=lambda:events.append('clock') or 123.5), \
             mock.patch.object(h,'disable_core_dumps',side_effect=lambda:events.append('core_guard')), \
             mock.patch.object(h,'read_tty',side_effect=read):
            self.assertEqual(h.live_input_trial(),report)
        self.assertEqual(events,['clock','core_guard','tty','supervisor'])
        fake.live_single.assert_called_once(); fake.sanitize_live_supervisor_report.assert_called_once()

    def test_core_guard_lowers_soft_and_hard_and_failure_prevents_input(self):
        import resource
        with mock.patch.object(resource,'setrlimit') as set_limit, \
             mock.patch.object(resource,'getrlimit',return_value=(0,0)):
            h.disable_core_dumps()
        set_limit.assert_called_once_with(resource.RLIMIT_CORE,(0,0))
        with mock.patch.object(h,'disable_core_dumps',side_effect=h.Rejected('core_dump_guard_unavailable')), \
             mock.patch.object(h,'read_tty') as read:
            with self.assertRaises(h.Rejected): h.live_input_trial()
        read.assert_not_called()

    def test_supervisor_failure_and_schema_rejection_never_print_candidate(self):
        for failure in (RuntimeError(self.PUBLIC_HEADER), h.Rejected('live_output_invalid')):
            fake=types.SimpleNamespace(live_single=mock.Mock(return_value={'raw':self.PUBLIC_HEADER}),
                                       sanitize_live_supervisor_report=mock.Mock(side_effect=failure))
            output=io.StringIO()
            with mock.patch.dict(sys.modules,{'collector_matrix':fake}), \
                 mock.patch.object(h,'disable_core_dumps'),mock.patch.object(h,'read_tty',return_value=self.PUBLIC_HEADER), \
                 mock.patch.object(h.sys,'addaudithook'), contextlib.redirect_stdout(output):
                self.assertEqual(h.main(['--live-tty']),2)
            self.assertNotIn('PUBLIC',output.getvalue())
            report=json.loads(output.getvalue())
            self.assertFalse(report['trial_result_available'])
            fake.live_single.assert_called_once()


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--tty-client':
        tty_client(sys.argv[2:]); raise SystemExit(0)
    if len(sys.argv)>1 and sys.argv[1]=='--tty-live-validation-client':
        tty_client(sys.argv[2:],live_validation=True); raise SystemExit(0)
    selected = HandoffTests
    if sys.argv[1:] == ['--live-input-tests']:
        selected = LiveInputTests
        sys.argv = sys.argv[:1]
    if len(sys.argv)==3 and sys.argv[1]=='--runtime': RUNTIME=Path(sys.argv[2]).resolve()
    elif len(sys.argv)!=1: raise SystemExit('invalid_test_arguments')
    result=unittest.TextTestRunner(verbosity=1,resultclass=SafeResult).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(selected))
    print(json.dumps({'suite':'session_handoff_live_input' if selected is LiveInputTests else 'session_handoff','tests':result.testsRun,'failures':len(result.failures),
                      'errors':len(result.errors),'passed':result.wasSuccessful(),'dummy_only':True,
                      'source_requests':0,'browser_launches':0,'ai_calls':0},separators=(',',':')))
    raise SystemExit(0 if result.wasSuccessful() else 1)
