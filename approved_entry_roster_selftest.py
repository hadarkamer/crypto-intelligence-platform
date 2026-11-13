"""Entry allowlist at the real source, durable outbox and transport boundary."""
from copy import deepcopy
import unittest
from unittest.mock import patch,Mock
import approved_alert_contract as contract
import approved_alert_producer as producer
import experimental_execution_transport as transport
from approved_alert_formula_selftest import formula_fixture
from approved_alert_producer_selftest import approved_fixture,projected
from maxpain_experimental_specs import SPECS
from experimental_execution_transport_selftest import config,ack
from experimental_execution_fixtures import sol_g65_message,hype_row71205_message,r2732_message

class EntryRosterTests(unittest.TestCase):
    def test_real_formula_source_only_retains_two_approved_entries(self):
        for family in ('r2732','hype_row71205','sol_g65'):
            f=formula_fixture(family);before=deepcopy(f['state'])
            values=producer.approved_messages(f['state'],now_ms=f['now_ms'],fence_ms=f['fence_ms'])
            self.assertEqual(len(values),0 if family=='sol_g65' else 1)
            self.assertEqual(len(f['database'].outbox),len(values))
            self.assertEqual(before,f['state'])

    def test_all_old_maxpain_sources_stop_execution_approvals_keep_history(self):
        for spec in SPECS.values():
            f=approved_fixture(spec);before=deepcopy(f['state'])
            self.assertEqual(projected(f),[]);self.assertEqual(f['database'].outbox,{})
            self.assertEqual(f['state'],before);self.assertTrue(f['state']['intents'])

    def test_retained_legacy_transport_queue_blocks_entry_but_sends_cancel(self):
        value=sol_g65_message();now=contract.moment_ms(value['source_as_of'])
        sender=transport.Sender();post=Mock(side_effect=lambda v,*a,**kw:ack(v))
        sender.tick([],config(),now_ms=now,reader=lambda *a,**kw:[value],post=post)
        post.assert_not_called();self.assertIn(value['occurrence_id'],sender.pending)
        cancel=sol_g65_message(kind='CANCEL',cancel_reason='SOURCE_EXIT')
        sender.tick([],config(),now_ms=now,reader=lambda *a,**kw:[cancel],post=post)
        self.assertEqual(post.call_count,1);self.assertEqual(post.call_args.args[0]['kind'],'CANCEL')

    def test_retained_two_identities_are_unchanged(self):
        for factory in (r2732_message,hype_row71205_message):
            value=factory();before=deepcopy(value)
            self.assertTrue(contract.new_entry_allowed(value));self.assertEqual(value,before)

import os
import approved_alert_outbox as outbox
from approved_alert_producer_selftest import ApprovedOutboxPostgreSQLTests
@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'),'Disposable PostgreSQL required')
class EntryRosterPostgresTests(unittest.TestCase):
    setUpClass=classmethod(ApprovedOutboxPostgreSQLTests.setUpClass.__func__)
    tearDownClass=classmethod(ApprovedOutboxPostgreSQLTests.tearDownClass.__func__)

    def test_retired_outbox_survives_restart_and_cancellation_without_new_entries(self):
        # A pre-withdrawal committed source row; never synthesize live signals.
        with patch.object(contract,'new_entry_allowed',return_value=True):
            f=formula_fixture('sol_g65')
            with self.connect() as conn:outbox.synchronize(conn,f['key'],f['state'],f['now_ms'])
        with self.connect() as conn:
            saved=outbox.read(conn,[f['key']]);self.assertEqual(len(saved),1)
            outbox.synchronize(conn,f['key'],f['state'],f['now_ms'])
        with self.connect() as conn:self.assertEqual(outbox.read(conn,[f['key']]),saved)
        ended=deepcopy(f['state']);ended.update(active=None,history=[],intents=[],last_exit_at=f['now'].isoformat())
        with self.connect() as conn:outbox.synchronize(conn,f['key'],ended,f['now_ms']+60000)
        with self.connect() as conn:
            canceled,=outbox.read(conn,[f['key']]);self.assertEqual(canceled['kind'],'CANCEL')
            self.assertEqual(contract.plan_digest(canceled),contract.plan_digest(saved[0]))
        for family in ('r2732','hype_row71205'):
            f=formula_fixture(family)
            with self.connect() as conn:outbox.synchronize(conn,f['key'],f['state'],f['now_ms'])
            with self.connect() as conn:self.assertEqual(len(outbox.read(conn,[f['key']])),1)
