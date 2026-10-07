"""Real SQLite file/process durability evidence, explicitly not PostgreSQL tests.

Every operation drives unchanged PlanStore methods and the real source contract.
SQLite's stronger writer serialization does not validate PG advisory locks/DDL.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from .experimental_plan_store import PlanStoreError
from .experimental_plan_sqlite_test_support import (
    SoftwareSQLiteJournal, SQLiteTestError, open_software_store,
)
from .test_experimental_plan_store import maxpain_plan, source_update, NOW, BEFORE, ARM

ROOT = Path(__file__).resolve().parents[1]


def child_environment():
    return dict(PATH=os.defpath, LANG='C.UTF-8', TZ='UTC', PYTHONUNBUFFERED='1',
        PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0', PYTHONPATH=str(ROOT))


WORKER = r'''
import json, sys, time
from pathlib import Path
from hl_testnet_runtime.experimental_plan_sqlite_test_support import open_software_store
from hl_testnet_runtime.experimental_plan_store import PlanStoreError
from hl_testnet_runtime.test_experimental_plan_store import maxpain_plan, source_update, NOW, BEFORE, ARM
path, gate, ready, operation, index = sys.argv[1:]
Path(ready).write_text('ready')
deadline=time.monotonic()+10
while not Path(gate).exists():
    if time.monotonic()>=deadline: raise RuntimeError('SOFTWARE_TEST_GATE_TIMEOUT')
    time.sleep(.005)
store=open_software_store(path)
message=maxpain_plan()
try:
    if operation=='ingest':
        result=store.ingest(message,now=NOW,not_before=BEFORE)
    elif operation=='cas':
        state=store.change_strategy(message['occurrence_id'],1,{'worker':int(index)},now=NOW)
        result={'status':'SAVED','revision':state['revision']}
    elif operation=='cancel':
        cancel=source_update(message,ARM,kind='CANCEL',reason='TARGET_TOUCHED_FIRST')
        result=store.ingest(cancel,now=ARM,not_before=BEFORE)
    elif operation=='update_owned':
        state=store.change_strategy(message['occurrence_id'],2,{'filled_quantity':'2','protection_required':True},now=ARM)
        result={'status':'SAVED','revision':state['revision']}
    else: raise RuntimeError('SOFTWARE_TEST_UNKNOWN_OPERATION')
except PlanStoreError as error:
    result={'status':str(error)}
print(json.dumps(result))
'''


class SQLitePlanStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='experimental-plan-software-')
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.path = self.directory / 'software-planstore.sqlite3'
        self.store = open_software_store(self.path, create=True)
        self.journal = self.store.journal
        self.message = maxpain_plan()
        self.identity = self.message['occurrence_id']

    def ingest(self):
        return self.store.ingest(self.message, now=NOW, not_before=BEFORE)

    def counts(self):
        with sqlite3.connect(self.path) as connection:
            return tuple(connection.execute('SELECT (SELECT count(*) FROM plans),(SELECT count(*) FROM events)').fetchone())

    def run_child(self, program, *args, expected=0):
        result = subprocess.run([sys.executable, '-c', program, *map(str, args)],
            cwd=ROOT, env=child_environment(), capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, expected, result.stderr)
        return result.stdout

    def race(self, operations):
        gate = self.directory / 'race.gate'
        processes, readies = [], []
        try:
            for index, operation in enumerate(operations):
                ready = self.directory / ('race-' + str(index) + '.ready')
                readies.append(ready)
                processes.append(subprocess.Popen([sys.executable, '-c', WORKER,
                    str(self.path), str(gate), str(ready), operation, str(index)],
                    cwd=ROOT, env=child_environment(), stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True))
            deadline = time.monotonic() + 10
            while not all(path.exists() for path in readies):
                if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
                    self.fail('SOFTWARE_TEST_CHILD_START_FAILED')
                time.sleep(.005)
            gate.write_text('go')
            replies = []
            for process in processes:
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, stderr)
                replies.append(json.loads(stdout))
            return replies
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)

    def test_software_fixture_is_explicit_and_cannot_select_testnet_domain(self):
        self.assertTrue(self.journal._ci)
        with self.assertRaises(AttributeError):
            self.journal._ci = False
        self.assertEqual(self.store.domain, 'software')
        self.assertFalse(SoftwareSQLiteJournal(self.path).initialize())
        with self.assertRaisesRegex(SQLiteTestError, 'CANNOT_VERIFY_POSTGRES_BOOTSTRAP'):
            self.store.initialize()
        for path in (':memory:', 'file:/tmp/unrelated.sqlite3', 'relative.sqlite3'):
            with self.subTest(path=path), self.assertRaises(SQLiteTestError):
                SoftwareSQLiteJournal(path)

    def test_uninitialized_foreign_and_symlink_files_are_refused(self):
        nonexistent = self.directory / 'absent.sqlite3'
        with self.assertRaisesRegex(SQLiteTestError, 'REGULAR_INITIALIZED_FILE'):
            open_software_store(nonexistent).load(self.identity)
        self.assertFalse(nonexistent.exists())
        unrelated = self.directory / 'foreign.sqlite3'
        with sqlite3.connect(unrelated) as connection:
            connection.execute('CREATE TABLE unrelated(value TEXT)')
        with self.assertRaisesRegex(SQLiteTestError, 'INITIALIZED_SOFTWARE_SCHEMA'):
            SoftwareSQLiteJournal(unrelated).initialize()
        symlink = self.directory / 'alias.sqlite3'
        symlink.symlink_to(self.path)
        with self.assertRaisesRegex(SQLiteTestError, 'REGULAR_INITIALIZED_FILE'):
            open_software_store(symlink).load(self.identity)

    def test_unknown_sql_is_refused_instead_of_imitating_postgres(self):
        with self.journal._transaction() as connection:
            with self.assertRaisesRegex(SQLiteTestError, 'UNSUPPORTED_POSTGRES_STATEMENT'):
                connection.execute('SELECT 1')
        self.assertEqual(self.counts(), (0, 0))

    def test_record_and_full_source_survive_new_python_process(self):
        self.ingest()
        program = '''import json,sys
from hl_testnet_runtime.experimental_plan_sqlite_test_support import open_software_store
state=open_software_store(sys.argv[1]).load(sys.argv[2])
print(json.dumps(state))'''
        restored = json.loads(self.run_child(program, self.path, self.identity))
        self.assertEqual(restored, self.store.load(self.identity))
        self.assertEqual(restored['initial_source'], self.message)
        self.assertEqual(self.counts(), (1, 1))

    def test_separate_process_receipts_have_one_durable_winner(self):
        replies = self.race(['ingest'] * 4)
        self.assertEqual(sum(reply['status'] == 'RECORDED' for reply in replies), 1)
        self.assertEqual(sum(reply['status'] == 'DUPLICATE' for reply in replies), 3)
        self.assertEqual(self.counts(), (1, 1))
        self.assertEqual(open_software_store(self.path).load(self.identity)['revision'], 1)

    def test_separate_process_cas_has_one_winner(self):
        self.ingest()
        replies = self.race(['cas'] * 4)
        self.assertEqual(sum(reply['status'] == 'SAVED' for reply in replies), 1)
        self.assertEqual(sum(reply['status'] == 'EXPERIMENTAL_CONCURRENT_RELOAD_REQUIRED' for reply in replies), 3)
        self.assertEqual(self.counts(), (1, 2))
        self.assertEqual(open_software_store(self.path).load(self.identity)['revision'], 2)

    def test_event_failure_rolls_back_real_state_write_then_retry_records_once(self):
        self.journal.fail_event_once = True
        with self.assertRaisesRegex(SQLiteTestError, 'EVENT_WRITE_FAILURE'):
            self.ingest()
        self.assertIsNone(open_software_store(self.path).load(self.identity))
        self.assertEqual(self.counts(), (0, 0))
        self.assertEqual(self.ingest()['revision'], 1)
        self.assertEqual(self.counts(), (1, 1))

    def test_strategy_event_failure_rolls_back_existing_revision_and_value(self):
        self.ingest()
        prior = self.store.load(self.identity)
        self.journal.fail_event_once = True
        with self.assertRaisesRegex(SQLiteTestError, 'EVENT_WRITE_FAILURE'):
            self.store.change_strategy(self.identity, 1, {'consumed_attempt': True}, now=NOW)
        self.assertEqual(open_software_store(self.path).load(self.identity), prior)
        self.assertEqual(self.counts(), (1, 1))

    def test_process_exit_after_writes_before_commit_recovers_no_partial_state(self):
        program = '''import os,sys
from contextlib import contextmanager
from hl_testnet_runtime.experimental_plan_sqlite_test_support import SoftwareSQLiteJournal
from hl_testnet_runtime.experimental_plan_store import PlanStore
from hl_testnet_runtime.test_experimental_plan_store import maxpain_plan,NOW,BEFORE
class CrashingJournal(SoftwareSQLiteJournal):
    @contextmanager
    def _transaction(self):
        with super()._transaction() as connection:
            yield connection
            os._exit(73)
PlanStore(CrashingJournal(sys.argv[1])).ingest(maxpain_plan(),now=NOW,not_before=BEFORE)
'''
        self.run_child(program, self.path, expected=73)
        self.assertIsNone(open_software_store(self.path).load(self.identity))
        self.assertEqual(self.counts(), (0, 0))
        self.assertEqual(self.ingest()['status'], 'RECORDED')

    def test_lost_commit_ack_survives_process_exit_and_retry_is_duplicate(self):
        program = '''import sys
from hl_testnet_runtime.experimental_plan_sqlite_test_support import open_software_store,SQLiteTestError
from hl_testnet_runtime.test_experimental_plan_store import maxpain_plan,NOW,BEFORE
store=open_software_store(sys.argv[1]);store.journal.lose_commit_ack_once=True
try:store.ingest(maxpain_plan(),now=NOW,not_before=BEFORE)
except SQLiteTestError as error:
    assert str(error)=='SQLITE_TEST_COMMIT_ACK_UNKNOWN'
else:raise AssertionError('NO_SUCCESS_ACK_EXPECTED')
'''
        self.run_child(program, self.path)
        result = open_software_store(self.path).ingest(self.message, now=NOW, not_before=BEFORE)
        self.assertEqual((result['status'], result['revision']), ('DUPLICATE', 1))
        self.assertEqual(self.counts(), (1, 1))

    def test_lost_strategy_commit_ack_cannot_replay_old_revision_after_reopen(self):
        self.ingest()
        self.journal.lose_commit_ack_once = True
        with self.assertRaisesRegex(SQLiteTestError, 'COMMIT_ACK_UNKNOWN'):
            self.store.change_strategy(self.identity, 1, {'consumed_attempt': True}, now=NOW)
        reopened = open_software_store(self.path)
        self.assertEqual(reopened.load(self.identity)['strategy'], {'consumed_attempt': True})
        with self.assertRaisesRegex(PlanStoreError, 'CONCURRENT_RELOAD_REQUIRED'):
            reopened.change_strategy(self.identity, 1, {'consumed_attempt': True}, now=NOW)
        self.assertEqual(self.counts(), (1, 2))

    def test_cancel_before_plan_is_durable_tombstone(self):
        cancel = source_update(self.message, NOW, kind='CANCEL', reason='TARGET_TOUCHED_FIRST')
        self.store.ingest(cancel, now=NOW, not_before=BEFORE)
        reopened = open_software_store(self.path)
        self.assertEqual(reopened.ingest(self.message, now=NOW, not_before=BEFORE)['status'], 'DUPLICATE')
        state = reopened.load(self.identity)
        self.assertEqual(state['entry_permission'], 'RETIRED')
        self.assertEqual(state['cancellation'], cancel)
        self.assertEqual(self.counts(), (1, 1))

    def test_cancel_and_strategy_process_race_preserves_owned_state(self):
        self.ingest()
        self.store.change_strategy(self.identity, 1, {'filled_quantity': '1', 'protection_required': True}, now=NOW)
        replies = self.race(['cancel', 'update_owned'])
        self.assertEqual(replies[0]['status'], 'RECORDED')
        self.assertIn(replies[1]['status'], ('SAVED', 'EXPERIMENTAL_CONCURRENT_RELOAD_REQUIRED'))
        state = open_software_store(self.path).load(self.identity)
        self.assertEqual(state['entry_permission'], 'RETIRED')
        self.assertTrue(state['strategy']['protection_required'])
        self.assertEqual(state['strategy']['filled_quantity'], '2' if replies[1]['status'] == 'SAVED' else '1')
        self.assertEqual(self.counts(), (1, 4 if replies[1]['status'] == 'SAVED' else 3))

    def test_durable_checksum_corruption_is_detected_after_reopen(self):
        self.ingest()
        with sqlite3.connect(self.path) as connection:
            connection.execute('UPDATE plans SET digest=? WHERE occurrence_id=?', ('0' * 64, self.identity))
        with self.assertRaisesRegex(PlanStoreError, 'STATE_INTEGRITY_FAILURE'):
            open_software_store(self.path).load(self.identity)

    def test_changed_domain_marker_cannot_be_reopened(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute('UPDATE metadata SET marker=?', ('not-a-software-fixture',))
        with self.assertRaisesRegex(SQLiteTestError, 'INITIALIZED_SOFTWARE_SCHEMA'):
            open_software_store(self.path).load(self.identity)


if __name__ == '__main__':
    unittest.main()
