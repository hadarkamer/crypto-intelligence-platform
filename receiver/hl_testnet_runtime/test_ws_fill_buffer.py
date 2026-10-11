"""Offline delivery, validation and commit-boundary tests for WS fill facts."""
from dataclasses import replace
import json
import threading
import unittest
from unittest.mock import patch

from . import fill_wakeups as wake
from .test_fill_wakeups import A, B, ROUTES, Clock, FakeSocket, fill, fills


def economic_fill(tid=1, *, symbol='SOL', quantity='10', oid=7, at=1000):
    return dict(coin=symbol, tid=tid, oid=oid, time=at, side='B',
                sz=quantity, px='150.25', fee='0.05', feeToken='USDC')


class FillBufferTests(unittest.TestCase):
    def setUp(self):
        self.feed = wake.FillWakeups(ROUTES, clock=Clock())
        self.ready(A)
        self.ready(B)

    def send(self, rows, account=A, *, snapshot=False, generation=None):
        generation = (self.feed._states[account]['generation']
                      if generation is None else generation)
        return self.feed._receive(account, generation,
            json.dumps(fills(account, rows=rows, snapshot=snapshot)))

    def ready(self, account):
        generation = self.feed._opened(account)
        for channel in wake.CHANNELS:
            self.assertTrue(self.feed._receive(account, generation, json.dumps(dict(
                channel='subscriptionResponse', data=dict(method='subscribe',
                subscription=dict(type=channel, user=account))))))
        self.assertTrue(self.send([], account, snapshot=True))
        return generation

    def reconcile(self, account=A):
        self.assertTrue(self.feed.finish_reconciliation(
            self.feed.begin_reconciliation(account), complete=True))

    def test_canonical_fill_is_peeked_nondestructively_and_ack_does_not_clear_gate(self):
        self.reconcile()
        self.assertTrue(self.send([economic_fill()]))
        batch = self.feed.pending_fills(A)
        self.assertEqual(batch.fills, (dict(account=A, symbol='SOL', oid='7',
            fill_id='hl:1', quantity='10', price='150.25', fee='0.05',
            fee_token='USDC', side='B', at_ms=1000),))
        self.assertEqual(self.feed.pending_fills(), (batch,))
        self.assertEqual(self.feed.pending_fills(A), batch)
        self.assertTrue(self.feed.acknowledge_fills(batch))
        self.assertEqual(self.feed.pending_fills(), ())
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertTrue(self.feed.acknowledge_fills(batch))

    def test_detached_samples_cannot_mutate_buffer_or_ack_changed_content(self):
        self.send([economic_fill()])
        original = self.feed.pending_fills(A)
        original.fills[0]['quantity'] = '900'
        self.assertEqual(original.items[0][1]['quantity'], '10')
        original.items[0][1]['quantity'] = '999'
        self.assertEqual(self.feed.pending_fills(A).fills[0]['quantity'], '10')
        self.assertTrue(self.feed.acknowledge_fills(original))
        self.assertEqual(len(self.feed.pending_fills(A).fills), 1)

    def test_account_buffers_are_independent_even_for_same_fill_identifier(self):
        self.send([economic_fill()], A)
        self.send([economic_fill(quantity='3')], B)
        batches = self.feed.pending_fills()
        self.assertEqual([batch.account for batch in batches], [A, B])
        self.assertTrue(self.feed.acknowledge_fills(batches[0]))
        self.assertEqual(self.feed.pending_fills(A).fills, ())
        self.assertEqual(self.feed.pending_fills(B).fills[0]['quantity'], '3')
        self.assertFalse(self.feed.acknowledge_fills(replace(batches[1], account='missing')))

    def test_duplicate_rows_and_messages_enqueue_one_fill(self):
        row = economic_fill()
        self.assertTrue(self.send([row, row]))
        original = self.feed.pending_fills(A)
        self.assertEqual(len(original.items), 1)
        self.reconcile()
        self.feed.wake_event.clear()
        self.assertTrue(self.send([row]))
        self.assertEqual(self.feed.pending_fills(A), original)
        self.assertTrue(self.feed.entry_allowed(A))
        self.assertFalse(self.feed.wake_event.is_set())

    def test_identical_fill_after_ack_does_not_restage_or_rewake_current_account(self):
        row = economic_fill()
        self.send([row])
        self.assertTrue(self.feed.acknowledge_fills(self.feed.pending_fills(A)))
        self.reconcile()
        self.feed.wake_event.clear()
        before = self.feed.health()
        self.assertTrue(self.send([row, row]))
        self.assertEqual(self.feed.pending_fills(), ())
        self.assertEqual(self.feed.health(), before)
        self.assertFalse(self.feed.wake_event.is_set())
        self.assertTrue(self.feed.entry_allowed(A))

    def test_changed_fact_after_ack_fails_closed_while_same_identity_is_retained(self):
        self.send([economic_fill()])
        self.assertTrue(self.feed.acknowledge_fills(self.feed.pending_fills(A)))
        self.reconcile()
        self.assertFalse(self.send([economic_fill(quantity='11')]))
        self.assertEqual(self.feed.pending_fills(), ())
        self.assertEqual(self.feed.health()['long_account']['protocol_failure'], 'FILL_FACT_CHANGED')
        self.assertFalse(self.feed.entry_allowed(A))

    def test_evicted_committed_fingerprint_can_replay_for_durable_deduplication(self):
        with patch.object(wake, 'MAX_SEEN', 1):
            self.send([economic_fill()])
            old = self.feed.pending_fills(A)
            self.assertTrue(self.feed.acknowledge_fills(old))
            self.send([fill(tid=2)])
            self.assertTrue(self.send([economic_fill()]))
            fresh = self.feed.pending_fills(A)
            self.assertEqual(fresh.fills, old.fills)
            self.assertGreater(fresh.items[0][0], old.items[0][0])
            self.assertTrue(self.feed.acknowledge_fills(old))
            self.assertEqual(self.feed.pending_fills(A), fresh)

    def test_identity_hint_can_later_upgrade_to_full_fill_and_wakes_again(self):
        self.assertTrue(self.send([fill()]))
        self.assertEqual(self.feed.pending_fills(), ())
        self.reconcile()
        self.feed.wake_event.clear()
        self.assertTrue(self.send([economic_fill()]))
        self.assertEqual(len(self.feed.pending_fills(A).fills), 1)
        self.assertTrue(self.feed.wake_event.is_set())
        self.assertFalse(self.feed.entry_allowed(A))

    def test_initial_snapshot_can_stage_fills_but_cannot_prove_history(self):
        generation = self.feed._opened(A)
        self.assertTrue(self.send([economic_fill()], snapshot=True))
        batch = self.feed.pending_fills(A)
        self.assertEqual(len(batch.fills), 1)
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertTrue(self.feed.acknowledge_fills(batch))
        self.assertFalse(self.feed.finish_reconciliation(
            self.feed.begin_reconciliation(A), complete=True))
        self.assertEqual(self.feed._states[A]['generation'], generation)

    def test_complete_batch_validates_before_accepting_first_good_row(self):
        self.send([economic_fill(9)])
        before = self.feed.pending_fills(A)
        invalid = economic_fill(2)
        invalid['sz'] = 'NaN'
        self.assertFalse(self.send([economic_fill(1), invalid]))
        self.assertEqual(self.feed.pending_fills(A), before)
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertEqual(self.feed.health()['long_account']['protocol_failure'], 'INVALID_FILL_FACT')

    def test_partial_economic_payload_and_invalid_exact_fields_fail_closed(self):
        for update in ({'sz': '1'}, {'px': 1}, {'fee': '0'}, {'hash': 'metadata'}):
            self.ready(A)
            self.assertFalse(self.send([{**fill(), **update}]))
            self.assertEqual(self.feed.pending_fills(A).fills, ())
            self.assertFalse(self.feed.entry_allowed(A))
        for key, value in (('sz', True), ('px', '0'), ('fee', 'NaN'),
                           ('side', 'BUY'), ('oid', 0), ('time', 0), ('feeToken', None)):
            self.ready(A)
            row = economic_fill()
            row[key] = value
            self.assertFalse(self.send([row]), (key, value))
            self.assertEqual(self.feed.pending_fills(A).fills, ())

    def test_same_fill_identifier_with_changed_facts_fails_closed(self):
        self.send([economic_fill()])
        before = self.feed.pending_fills(A)
        for changed in (economic_fill(quantity='11'), economic_fill(symbol='ETH'),
                        economic_fill(oid=8), economic_fill(at=1001)):
            self.ready(A)
            self.assertFalse(self.send([economic_fill(2), changed]))
            self.assertEqual(self.feed.pending_fills(A), before)
            self.assertEqual(self.feed.health()['long_account']['protocol_failure'], 'FILL_FACT_CHANGED')

    def test_conflicting_facts_within_same_frame_reject_entire_frame(self):
        self.assertFalse(self.send([economic_fill(), economic_fill(quantity='11')]))
        self.assertEqual(self.feed.pending_fills(), ())
        self.assertFalse(self.feed._states[A]['connected'])

    def test_uncommitted_sample_and_disconnect_reconnect_keep_pending_fills(self):
        old_generation = self.feed._states[A]['generation']
        self.send([economic_fill()])
        before = self.feed.pending_fills(A)
        self.feed._disconnected(A, old_generation)
        self.assertEqual(self.feed.pending_fills(A), before)
        self.ready(A)
        self.assertEqual(self.feed.pending_fills(A), before)
        self.assertTrue(self.send([economic_fill()], snapshot=True))
        self.assertEqual(self.feed.pending_fills(A), before)
        self.assertFalse(self.send([economic_fill(2)], generation=old_generation))
        self.assertEqual(self.feed.pending_fills(A), before)

    def test_ack_of_captured_batch_preserves_new_concurrent_arrivals(self):
        self.send([economic_fill()])
        batch = self.feed.pending_fills(A)
        thread = threading.Thread(target=lambda: self.send([economic_fill(2)]))
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.feed.acknowledge_fills(batch))
        remaining = self.feed.pending_fills(A)
        self.assertEqual([row['fill_id'] for row in remaining.fills], ['hl:2'])
        self.assertGreater(remaining.items[0][0], batch.items[0][0])

    def test_old_ack_does_not_erase_same_fill_redelivered_after_commit(self):
        self.send([economic_fill()])
        batch = self.feed.pending_fills(A)
        self.assertTrue(self.feed.acknowledge_fills(batch))
        self.ready(A)
        self.assertTrue(self.send([economic_fill()], snapshot=True))
        fresh = self.feed.pending_fills(A)
        self.assertGreater(fresh.items[0][0], batch.items[0][0])
        self.assertTrue(self.feed.acknowledge_fills(batch))
        self.assertEqual(self.feed.pending_fills(A), fresh)

    def test_buffer_overflow_is_atomic_bounded_gap_and_never_clears_prior_rows(self):
        with patch.object(wake, 'MAX_PENDING_FILLS', 2):
            self.assertTrue(self.send([economic_fill()]))
            before = self.feed.pending_fills(A)
            self.assertFalse(self.send([economic_fill(2), economic_fill(3)]))
            self.assertEqual(self.feed.pending_fills(A), before)
            health = self.feed.health()['long_account']
            self.assertEqual(health['protocol_failure'], 'PENDING_FILL_BUFFER_FULL')
            self.assertEqual(health['pending_fill_count'], 1)
            self.assertFalse(self.feed.entry_allowed(A))
            self.assertFalse(self.feed.finish_reconciliation(
                self.feed.begin_reconciliation(A), complete=True))
            self.assertTrue(self.feed.acknowledge_fills(before))
            self.ready(A)
            self.assertTrue(self.send([economic_fill(2), economic_fill(3)]))
            self.assertEqual(len(self.feed.pending_fills(A).fills), 2)

    def test_duplicate_fills_do_not_exhaust_capacity(self):
        with patch.object(wake, 'MAX_PENDING_FILLS', 1):
            self.assertTrue(self.send([economic_fill()] * 10))
            self.assertTrue(self.send([economic_fill()] * 10))
            self.assertEqual(len(self.feed.pending_fills(A).fills), 1)

    def test_stop_keeps_accepted_rows_available_for_commit_but_refuses_new_data(self):
        self.send([economic_fill()])
        batch = self.feed.pending_fills(A)
        self.feed.stop()
        self.assertEqual(self.feed.pending_fills(A), batch)
        self.assertFalse(self.send([economic_fill(2)]))
        self.assertTrue(self.feed.acknowledge_fills(batch))
        self.assertFalse(self.feed.entry_allowed(A))

    def test_ack_rejects_invalid_batch_without_mutating_pending(self):
        self.send([economic_fill()])
        before = self.feed.pending_fills(A)
        self.assertFalse(self.feed.acknowledge_fills(None))
        malformed = replace(before, items=before.items + (('bad-sequence', {}),))
        self.assertFalse(self.feed.acknowledge_fills(malformed))
        self.assertEqual(self.feed.pending_fills(A), before)

    def test_socket_explicitly_subscribes_to_individual_fills(self):
        socket = FakeSocket()
        feed = wake.FillWakeups({'long_account': A}, connector=lambda *a, **k: socket)
        original_send = socket.send
        def send_and_stop(raw):
            original_send(raw)
            message = json.loads(raw)
            if message.get('subscription', {}).get('type') == 'userFills':
                feed._stop.set()
        socket.send = send_and_stop
        feed._worker(A)
        subscriptions = [message['subscription'] for message in socket.sent]
        self.assertEqual(subscriptions, [dict(type='orderUpdates', user=A),
            dict(type='userFills', user=A, aggregateByTime=False)])


if __name__ == '__main__':
    unittest.main()
