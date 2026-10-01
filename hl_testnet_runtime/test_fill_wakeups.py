"""Offline transport/gap/race tests; no websocket value can prove a trade."""
from dataclasses import replace
import json
import queue
import threading
import time
import unittest

from . import fill_wakeups as wake


A = '0x' + '1' * 40
B = '0x' + '2' * 40
ROUTES = {'long_account': A, 'short_account': B}


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def fill(symbol='SOL', tid=1, oid=7, at=1000):
    # Notification fields intentionally omit prices/quantities. This API cannot
    # return those values or substitute them for authoritative saved REST fills.
    return dict(coin=symbol, tid=tid, oid=oid, time=at)


def fills(account=A, *, snapshot=False, rows=None):
    return dict(channel='userFills', data=dict(user=account, isSnapshot=snapshot,
                fills=[fill()] if rows is None else rows))


def order(symbol='SOL', oid=7, at=1000, status='filled'):
    return dict(channel='orderUpdates', data=[dict(order=dict(coin=symbol, oid=oid),
                                                statusTimestamp=at, status=status)])


class WakeupTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.event = threading.Event()
        self.feed = wake.FillWakeups(ROUTES, wake_event=self.event, clock=self.clock)

    def send(self, message, account=A, generation=None):
        if generation is None:
            generation = self.feed._states[account]['generation']
        return self.feed._receive(account, generation, json.dumps(message))

    def ready(self, account=A):
        generation = self.feed._opened(account)
        for channel in wake.CHANNELS:
            self.assertTrue(self.send(dict(channel='subscriptionResponse', data=dict(
                method='subscribe', subscription=dict(type=channel, user=account))), account))
        self.assertTrue(self.send(fills(account, snapshot=True, rows=[]), account))
        return generation

    def reconcile(self, account=A):
        token = self.feed.begin_reconciliation(account)
        self.assertTrue(self.feed.finish_reconciliation(token, complete=True))
        return token

    def test_account_configuration_exact_distinct_roles_without_url_override(self):
        for routes in ({}, {'real_account': A}, {'long_account': 'not-address'},
                       {'long_account': '0x' + '0' * 40},
                       {'long_account': A, 'short_account': A.upper().replace('0X', '0x')}):
            with self.assertRaises(ValueError):
                wake.FillWakeups(routes)
        with self.assertRaises(TypeError):
            wake.FillWakeups(ROUTES, endpoint='wss://api.hyperliquid.xyz/ws')
        with self.assertRaises(ValueError):
            self.feed.begin_reconciliation('0x' + '3' * 40)

    def test_startup_fails_closed_and_acknowledgements_snapshot_cannot_open_entries(self):
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertEqual(set(self.feed.pending_accounts()), {A, B})
        self.assertIsNone(self.feed.dirty_symbols(A))
        self.ready()
        self.assertFalse(self.feed.entry_allowed(A))
        token = self.feed.begin_reconciliation(A)
        self.assertFalse(self.feed.finish_reconciliation(token))
        self.assertFalse(self.feed.finish_reconciliation(token, complete=1))
        self.reconcile()
        self.assertTrue(self.feed.entry_allowed('long_account'))
        self.assertFalse(self.feed.entry_allowed(B))

    def test_both_subscriptions_and_first_snapshot_required_before_clearing_gap(self):
        self.feed._opened(A)
        self.send(dict(channel='subscriptionResponse', data=dict(method='subscribe',
                  subscription=dict(type='userFills', user=A))))
        token = self.feed.begin_reconciliation(A)
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        self.send(fills(snapshot=True, rows=[]))
        token = self.feed.begin_reconciliation(A)
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        self.send(dict(channel='subscriptionResponse', data=dict(method='subscribe',
                  subscription=dict(type='orderUpdates', user=A))))
        self.reconcile()

    def test_fresh_fill_only_wakes_rest_reconciliation_and_isolated_account(self):
        self.ready(A)
        self.ready(B)
        self.reconcile(A)
        self.reconcile(B)
        self.event.clear()
        self.assertTrue(self.send(fills(rows=[fill('SOL'), fill('ETH', 2)])))
        self.assertTrue(self.event.is_set())
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertTrue(self.feed.entry_allowed(B))
        self.assertEqual(self.feed.pending_accounts(), (A,))
        self.assertEqual(self.feed.dirty_symbols(A), ('ETH', 'SOL'))
        self.assertNotIn('fills', self.feed.health()['long_account'])

    def test_optional_snapshot_flag_stages_live_hint_but_never_replaces_initial_snapshot(self):
        self.ready()
        self.reconcile()
        message = fills(rows=[fill('ETH', 2)])
        message['data'].pop('isSnapshot')
        self.assertTrue(self.send(message))
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertEqual(self.feed.dirty_symbols(A), ('ETH',))
        self.feed._opened(A)
        self.send(dict(channel='subscriptionResponse', data=dict(method='subscribe',
                  subscription=dict(type='userFills', user=A))))
        self.assertTrue(self.send(message))
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertFalse(self.feed.finish_reconciliation(self.feed.begin_reconciliation(A),complete=True))

    def test_userless_order_updates_invalidate_only_the_bound_socket_account(self):
        self.ready(A)
        self.ready(B)
        self.reconcile(A)
        self.reconcile(B)
        self.assertTrue(self.send(order('DOGE'), B))
        self.assertTrue(self.feed.entry_allowed(A))
        self.assertFalse(self.feed.entry_allowed(B))
        self.assertEqual(self.feed.dirty_symbols(B), ('DOGE',))

    def test_cross_account_fill_or_ack_forces_socket_gap_without_notifying_other_account(self):
        for message in (fills(B), dict(channel='subscriptionResponse', data=dict(
                method='subscribe', subscription=dict(type='userFills', user=B)))):
            feed = wake.FillWakeups(ROUTES, clock=self.clock)
            self.feed = feed
            self.ready(A)
            self.ready(B)
            self.reconcile(A)
            self.reconcile(B)
            self.assertFalse(self.send(message, A))
            self.assertFalse(feed.entry_allowed(A))
            self.assertTrue(feed.entry_allowed(B))
            self.assertIsNone(feed.dirty_symbols(A))

    def test_new_event_during_rest_or_disconnect_cannot_clear_old_token(self):
        generation = self.ready()
        token = self.feed.begin_reconciliation(A)
        self.send(order())
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        token = self.feed.begin_reconciliation(A)
        self.feed._disconnected(A, generation)
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        self.ready()
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        self.reconcile()

    def test_old_session_message_disconnect_and_heartbeat_cannot_touch_new_session(self):
        old = self.ready()
        self.ready()
        self.reconcile()
        before = self.feed.health()
        self.assertFalse(self.send(fills(rows=[fill('ETH', 2)]), generation=old))
        self.feed._disconnected(A, old)
        self.assertEqual(self.feed._heartbeat(A, old), 'CLOSE')
        self.assertEqual(self.feed.health(), before)

    def test_only_current_complete_rest_token_clears_the_continuity_gap(self):
        self.ready()
        token = self.feed.begin_reconciliation(A)
        self.clock.now += wake.REST_CURRENT_SECONDS + 0.001
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        token = self.reconcile()
        self.assertFalse(self.feed.finish_reconciliation(replace(token, started=self.clock.now + 1), complete=True))
        self.clock.now += wake.REST_CURRENT_SECONDS + 0.001
        self.assertTrue(self.feed.entry_allowed(A))

    def test_continuous_pongs_preserve_quiet_account_gap_clear_without_rest_rescans(self):
        generation = self.ready()
        self.reconcile()
        for _ in range(4):
            self.clock.now += 20
            self.assertTrue(self.send(dict(channel='pong')))
            self.assertTrue(self.feed.entry_allowed(A))
            self.assertNotIn(A, self.feed.pending_accounts())
            self.assertEqual(self.feed.dirty_symbols(A), ())
        # Continuity never lets a new hint or a disconnected session pass.
        self.send(fills())
        self.assertFalse(self.feed.entry_allowed(A))
        self.reconcile()
        self.feed._disconnected(A, generation)
        self.assertFalse(self.feed.entry_allowed(A))

    def test_duplicate_messages_do_not_rewake_or_invalidate_a_current_observation(self):
        self.ready()
        message = fills()
        self.send(message)
        self.reconcile()
        self.event.clear()
        token = self.feed.begin_reconciliation(A)
        self.assertTrue(self.send(message))
        self.assertFalse(self.event.is_set())
        self.assertTrue(self.feed.finish_reconciliation(token, complete=True))
        self.assertTrue(self.feed.entry_allowed(A))

    def test_burst_overflow_becomes_bounded_full_account_hint_without_lost_wakeup(self):
        self.ready()
        self.reconcile()
        self.send(fills(rows=[fill('ASSET' + str(i), i) for i in range(600)]))
        state = self.feed._states[A]
        self.assertLessEqual(len(state['seen']), wake.MAX_SEEN)
        self.assertEqual(len(state['dirty_symbols']), 0)
        self.assertIsNone(self.feed.dirty_symbols(A))
        self.assertTrue(self.event.is_set())
        self.assertFalse(self.feed.entry_allowed(A))
        self.reconcile()
        self.assertTrue(self.feed.entry_allowed(A))

    def test_repeated_snapshot_forces_full_account_reconciliation_even_when_empty(self):
        self.ready()
        self.reconcile()
        self.assertTrue(self.send(fills(snapshot=True, rows=[])))
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertIsNone(self.feed.dirty_symbols(A))

    def test_malformed_batches_are_rejected_before_any_reconciliation_readiness(self):
        malformed = [dict(channel='unknown', data=[]), fills(rows=[{'coin': 'SOL'}]),
            fills(rows=[fill(), fill(at=True)]),
            dict(channel='userFills', data=dict(user=A, fills=[], isSnapshot=None)),
            order(status='filled<script>'),
            dict(channel='subscriptionResponse', data=dict(method='post', subscription={})),
            dict(channel='orderUpdates', data=[dict(order=dict(coin='SOL', oid=True),
                                                    statusTimestamp=1, status='open')])]
        for message in malformed:
            self.ready()
            self.reconcile()
            self.assertFalse(self.send(message))
            self.assertFalse(self.feed.entry_allowed(A))
            self.assertIsNone(self.feed.dirty_symbols(A))

    def test_raw_oversized_bad_json_and_unknown_frames_freeze_until_new_bootstrap(self):
        for raw in ('{', '[]', b'\xff', ' ' * (wake.MAX_FRAME_BYTES + 1)):
            generation = self.ready()
            self.reconcile()
            self.assertFalse(self.feed._receive(A, generation, raw))
            self.assertFalse(self.feed.entry_allowed(A))

    def test_data_before_ack_and_live_fills_before_snapshot_cannot_clear_startup_gap(self):
        self.feed._opened(A)
        self.assertTrue(self.send(order()))
        self.assertFalse(self.feed.entry_allowed(A))
        self.send(dict(channel='subscriptionResponse', data=dict(method='subscribe',
                  subscription=dict(type='userFills', user=A))))
        self.assertTrue(self.send(fills()))
        self.assertFalse(self.feed.finish_reconciliation(self.feed.begin_reconciliation(A),complete=True))

    def test_pre_ack_snapshot_and_normalized_optional_ack_still_require_both_acks_and_rest(self):
        self.feed._opened(A)
        self.assertTrue(self.send(fills(snapshot=True,rows=[])))
        self.assertFalse(self.feed.finish_reconciliation(self.feed.begin_reconciliation(A),complete=True))
        for channel in ('orderUpdates','userFills'):
            subscription=dict(type=channel,user=A)
            if channel=='userFills':
                subscription['aggregateByTime']=False
            self.assertTrue(self.send(dict(channel='subscriptionResponse',
                data=dict(method='subscribe',subscription=subscription))))
            self.assertFalse(self.feed.entry_allowed(A))
        self.reconcile()
        self.assertTrue(self.feed.entry_allowed(A))

    def test_exact_sdk_banner_is_transport_only_and_json_spoof_is_rejected(self):
        generation=self.feed._opened(A)
        self.assertTrue(self.feed._receive(A,generation,'Websocket connection established.'))
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertEqual(self.feed.health()['long_account']['subscriptions_acknowledged'],0)
        self.assertFalse(self.send(dict(channel='_transportBanner')))
        self.assertEqual(self.feed.health()['long_account']['protocol_failure'],'UNEXPECTED_CHANNEL')

    def test_changed_aggregation_or_extra_ack_fields_fail_closed_with_redacted_reason(self):
        for extra,reason in ((dict(aggregateByTime=True),'ACK_PARAMETERS_CHANGED'),
                             (dict(aggregateByTime='false'),'ACK_PARAMETERS_CHANGED'),
                             (dict(privateUnexpectedField='never expose this'),'INVALID_ACK_SUBSCRIPTION')):
            self.feed._opened(A)
            self.assertFalse(self.send(dict(channel='subscriptionResponse',data=dict(method='subscribe',
                subscription=dict(type='userFills',user=A,**extra)))))
            health=self.feed.health()['long_account']
            self.assertEqual(health['protocol_failure'],reason)
            self.assertNotIn('never expose this',json.dumps(health))
            self.assertFalse(self.feed.entry_allowed(A))

    def test_json_ping_pong_deadline_and_idle_gap_fail_closed(self):
        generation = self.ready()
        self.reconcile()
        self.clock.now += wake.PING_INTERVAL
        self.assertEqual(self.feed._heartbeat(A, generation), 'PING')
        self.assertTrue(self.send(dict(channel='pong')))
        self.assertEqual(self.feed._heartbeat(A, generation), 'WAIT')
        self.reconcile()
        self.clock.now += wake.PING_INTERVAL
        self.assertEqual(self.feed._heartbeat(A, generation), 'PING')
        self.clock.now += wake.PONG_TIMEOUT
        self.assertEqual(self.feed._heartbeat(A, generation), 'CLOSE')
        self.assertFalse(self.feed.entry_allowed(A))
        generation = self.ready()
        self.reconcile()
        self.clock.now += wake.IDLE_TIMEOUT
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertEqual(self.feed._heartbeat(A, generation), 'CLOSE')

    def test_expired_pong_blocks_gates_even_before_delayed_transport_thread_closes(self):
        generation = self.ready()
        self.reconcile()
        self.clock.now += wake.PING_INTERVAL
        self.assertEqual(self.feed._heartbeat(A, generation), 'PING')
        token = self.feed.begin_reconciliation(A)
        self.clock.now += wake.PONG_TIMEOUT
        self.assertFalse(self.feed.entry_allowed(A))
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))

    def test_bootstrap_timeout_stops_incomplete_subscription_without_waiting_for_idle(self):
        generation = self.feed._opened(A)
        self.clock.now += wake.BOOTSTRAP_TIMEOUT
        self.assertEqual(self.feed._heartbeat(A, generation), 'CLOSE')
        self.assertFalse(self.feed.finish_reconciliation(self.feed.begin_reconciliation(A), complete=True))

    def test_stop_invalidates_tokens_and_old_data_cannot_reopen_or_restart(self):
        generation = self.ready()
        token = self.reconcile()
        self.feed.stop()
        self.assertFalse(self.feed.finish_reconciliation(token, complete=True))
        self.assertFalse(self.send(fills(snapshot=True, rows=[]), generation=generation))
        self.assertFalse(self.feed.entry_allowed(A))
        with self.assertRaises(ValueError):
            self.feed.start()


class FakeSocket:
    def __init__(self, *, normalized_ack=False, snapshot_before_ack=False, banner=False):
        self.messages = queue.Queue()
        self.sent = []
        self.closed = False
        self.timeout = None
        self.normalized_ack=normalized_ack
        self.snapshot_before_ack=snapshot_before_ack
        if banner:
            self.messages.put('Websocket connection established.')

    def settimeout(self, value):
        self.timeout = value

    def send(self, raw):
        message = json.loads(raw)
        self.sent.append(message)
        if message['method'] == 'subscribe':
            subscription = message['subscription']
            acknowledgement=json.loads(raw)
            if subscription['type']=='userFills' and self.normalized_ack:
                acknowledgement['subscription']['aggregateByTime']=False
            if subscription['type']=='userFills' and self.snapshot_before_ack:
                self.messages.put(json.dumps(fills(subscription['user'],snapshot=True,rows=[])))
            self.messages.put(json.dumps(dict(channel='subscriptionResponse', data=acknowledgement)))
            if subscription['type'] == 'userFills':
                if not self.snapshot_before_ack:
                    self.messages.put(json.dumps(fills(subscription['user'], snapshot=True, rows=[])))
        elif message['method'] == 'ping':
            self.messages.put('{"channel":"pong"}')
        else:
            raise AssertionError('Only subscriptions and heartbeat may be sent')

    def recv(self):
        try:
            return self.messages.get(timeout=.01)
        except queue.Empty:
            raise TimeoutError() from None

    def close(self, **kwargs):
        self.closed = True
        self.messages.put('')


class TransportTests(unittest.TestCase):
    def test_protocol_bootstrap_handles_sdk_banner_normalized_ack_and_pre_ack_snapshot(self):
        socket=FakeSocket(normalized_ack=True,snapshot_before_ack=True,banner=True)
        feed=wake.FillWakeups({'long_account':A},connector=lambda *args,**kwargs:socket)
        try:
            feed.start()
            deadline=time.monotonic()+2
            while not (feed.health()['long_account']['snapshot_received']
                       and feed.health()['long_account']['subscriptions_acknowledged']==2):
                self.assertLess(time.monotonic(),deadline)
                feed.wake_event.wait(.02)
                feed.wake_event.clear()
            self.assertIsNone(feed.health()['long_account']['protocol_failure'])
            self.assertFalse(feed.entry_allowed(A))
            self.assertTrue(feed.finish_reconciliation(feed.begin_reconciliation(A),complete=True))
            self.assertTrue(feed.entry_allowed(A))
        finally:
            feed.stop()

    def test_failed_connections_use_interruptible_bounded_reconnect_budget(self):
        class Stop:
            def __init__(self):
                self.delays = []

            def is_set(self):
                return len(self.delays) >= 5

            def wait(self, delay):
                self.delays.append(delay)
                return self.is_set()

        calls = []
        def unavailable(url, **kwargs):
            calls.append(url)
            raise OSError('private upstream data must not be logged')
        feed = wake.FillWakeups({'long_account': A}, connector=unavailable)
        stop = Stop()
        feed._stop = stop
        feed._worker(A)
        self.assertEqual(stop.delays, [5, 10, 20, 30, 30])
        self.assertEqual(calls, [wake.TESTNET_WS] * 5)
        self.assertFalse(feed.entry_allowed(A))

    def test_fixed_endpoint_two_isolated_sockets_lifecycle_no_order_action_and_bounded_shutdown(self):
        calls = []
        sockets = []
        def connector(url, **kwargs):
            calls.append((url, kwargs))
            socket = FakeSocket()
            sockets.append(socket)
            return socket
        feed = wake.FillWakeups(ROUTES, connector=connector)
        try:
            feed.start()
            feed.start()
            deadline = time.monotonic() + 2
            while not all(row['subscriptions_acknowledged'] == 2 and row['snapshot_received']
                          for row in feed.health().values()):
                self.assertLess(time.monotonic(), deadline)
                feed.wake_event.wait(.02)
                feed.wake_event.clear()
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(url == wake.TESTNET_WS and kwargs['timeout'] == 3
                                for url, kwargs in calls))
            self.assertEqual({socket.sent[0]['subscription']['user'] for socket in sockets}, {A, B})
            self.assertTrue(all(len(socket.sent) == 2 and socket.timeout == 1 for socket in sockets))
            for account in (A, B):
                self.assertFalse(feed.entry_allowed(account))
                self.assertTrue(feed.finish_reconciliation(feed.begin_reconciliation(account), complete=True))
        finally:
            feed.stop()
        self.assertTrue(all(socket.closed for socket in sockets))
        self.assertTrue(all(not thread.is_alive() for thread in feed._threads))
        self.assertFalse(feed.entry_allowed(A))
        self.assertFalse(feed.entry_allowed(B))
