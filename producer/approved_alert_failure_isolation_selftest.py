"""Offline fault boundaries for approved v2 producer, sender, and source retry."""
from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
import io
import json
import socket
import unittest
from unittest.mock import AsyncMock, Mock, patch

import approved_alert_contract as contract
import approved_alert_outbox as outbox
import approved_alert_producer as producer
import experimental_execution_bridge as bridge
import experimental_execution_forwarder as forwarder
import experimental_execution_transport as transport
import experimental_hyperliquid_source as source
import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as worker
from approved_alert_producer_selftest import approved_fixture, OutboxDatabase, approved_env
from experimental_execution_forwarder_selftest import environment
from experimental_execution_source_audit_selftest import BASE, CONFIG, M, SCOPE
from experimental_execution_transport_selftest import ack
from maxpain_experimental_specs import SPECS


def cfg():
    return forwarder.configuration(approved_env()).transport


def values():
    return [producer.approved_messages(f['state'], now_ms=f['now_ms'], fence_ms=1)[0]
            for f in [approved_fixture(SPECS[c]) for c in ('SOL', 'HYPE', 'DOGE')]]


def cancel(value):
    result = deepcopy(value)
    asof = contract.moment_ms(value['approved_at']) + M
    result.update(kind='CANCEL', source_state='CANCELED', source_as_of=contract.iso_ms(asof),
        source_sequence=asof*10+contract.RANK['CANCEL'], cancel_reason='SOURCE_OBSERVATION_ENDED')
    return contract.validate(result)


class SourceIsolationTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.create_connection'):
            guard = patch(target, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def test_invalid_intent_does_not_poison_valid_sibling_or_rewrite_evidence(self):
        f = approved_fixture(); state = deepcopy(f['state'])
        bad = deepcopy(state['intents'][0]); bad['intent_id'] = 'e'*64
        state['intents'].insert(0, bad)
        before = deepcopy(state)
        with self.assertRaisesRegex(ValueError, 'APPROVED_SOURCE_IDENTITY'):
            producer.approved_messages(state, now_ms=f['now_ms'], fence_ms=1)
        records, ids, errors = producer.isolated_records(state, now_ms=f['now_ms'], fence_ms=1)
        self.assertEqual(len(records), 1); self.assertEqual(len(errors), 1)
        self.assertEqual(state, before)
        db = OutboxDatabase()
        with db.connect() as conn, redirect_stdout(io.StringIO()):
            self.assertTrue(outbox.synchronize(conn, f['key'], state, f['now_ms']))
            result, = outbox.read(conn, [f['key']])
        self.assertEqual(result, next(iter(records.values())))

    def test_invalid_sibling_does_not_drop_original_cancellation(self):
        f = approved_fixture(); state = deepcopy(f['state'])
        state['intents'][0]['expires_ms'] += 1
        state['active'] = []; state['history'] = []
        state['bar_cursor_ms'] = f['approved_ms']
        db = f['database']
        with db.connect() as conn, redirect_stdout(io.StringIO()):
            self.assertTrue(outbox.synchronize(conn, f['key'], state, f['approved_ms']+M))
            value, = outbox.read(conn, [f['key']])
        self.assertEqual(value['kind'], 'CANCEL')
        self.assertEqual(value['expires_at'], contract.iso_ms(f['approved_ms']+90_000))

    def test_unchanged_approval_avoids_global_capacity_lock_and_count(self):
        f = approved_fixture(); db = f['database']; db.calls.clear()
        with db.connect() as conn:
            self.assertFalse(outbox.synchronize(conn, f['key'], f['state'], f['now_ms']))
        self.assertEqual(len(db.calls), 1)
        self.assertNotIn('pg_advisory', db.calls[0][0])
        self.assertNotIn('count(*)', db.calls[0][0])

    def test_structural_source_failure_still_aborts_transaction(self):
        f = approved_fixture(); state = deepcopy(f['state']); state['intents'] = None
        before = deepcopy(f['database'].outbox)
        with self.assertRaisesRegex(ValueError, 'SOURCE_INTENTS_INVALID'):
            with f['database'].connect() as conn:
                outbox.synchronize(conn, f['key'], state, f['now_ms'])
        self.assertEqual(f['database'].outbox, before)

    def test_conflicting_same_occurrence_is_excluded_not_last_copy_wins(self):
        f = approved_fixture(); state = deepcopy(f['state'])
        bad = deepcopy(state['intents'][0]); bad['payload']['fill_price'] += .001
        state['intents'].append(bad)
        records, ids, errors = producer.isolated_records(state, now_ms=f['now_ms'], fence_ms=1)
        self.assertEqual(records, {}); self.assertEqual(ids, {}); self.assertEqual(len(errors), 1)

    def test_malformed_row_quarantines_only_its_sql_identity(self):
        fixtures = [approved_fixture(SPECS[c]) for c in ('SOL', 'HYPE')]
        db = OutboxDatabase()
        for f in fixtures: db.outbox.update(deepcopy(f['database'].outbox))
        bad = next(iter(fixtures[0]['database'].outbox))
        db.outbox[bad]['payload_json'] = 'malformed'
        before = deepcopy(db.outbox)
        with patch('alert_cards_forwarder._source_dsn', return_value='offline'):
            batch = bridge.read_approved({SCOPE}, BASE, connect=db.connect,
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE})
        self.assertEqual([v['symbol'] for v in batch], ['HYPE'])
        self.assertEqual(batch.blocked_occurrences, {bad[1]})
        self.assertEqual(batch.source_errors, 1); self.assertEqual(db.outbox, before)

    def test_bad_payload_cannot_impersonate_another_sql_occurrence(self):
        f = approved_fixture(); db = f['database']; key, identity = next(iter(db.outbox))
        db.outbox[(key, 'e'*64)] = db.outbox.pop((key, identity))
        db.outbox[(key, 'e'*64)]['occurrence_id'] = 'e'*64
        with db.connect() as conn:
            batch = outbox.read(conn, [key], isolate=True)
        self.assertEqual(batch, []); self.assertEqual(batch.blocked_occurrences, {'e'*64})

    def test_soft_deadline_rotates_and_returns_only_fully_read_sources(self):
        keys = bridge.approved_source_keys({SCOPE}); queried = []
        @contextmanager
        def connection(*a, **kw): yield object()
        def read(conn, source_keys, **kw):
            queried.extend(source_keys); return outbox.ReadBatch()
        times = iter((0., 7.))
        with patch.object(bridge, '_approved_connect', connection), patch.object(outbox, 'read', read):
            first = bridge.read_approved({SCOPE}, BASE,
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE},
                deadline_monotonic=6, clock=lambda: next(times))
            second = bridge.read_approved({SCOPE}, BASE,
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE},
                source_offset=first.next_source_offset)
        self.assertFalse(first.complete); self.assertEqual(first.source_errors, 1)
        self.assertEqual(queried[:2], keys[:2]); self.assertTrue(second.complete)
        self.assertEqual(queried[-1], keys[0])

    def test_shared_sql_error_does_not_return_earlier_partial_data(self):
        @contextmanager
        def connection(*a, **kw): yield object()
        first = outbox.ReadBatch(); first.extend(values()[:1])
        with patch.object(bridge, '_approved_connect', connection), \
                patch.object(outbox, 'read', side_effect=[first, RuntimeError('DB_FAILED')]), \
                self.assertRaisesRegex(RuntimeError, 'DB_FAILED'):
            bridge.read_approved({SCOPE}, BASE,
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE})


class SenderIsolationTests(unittest.TestCase):
    def tick(self, sender, records, **kw):
        now = contract.moment_ms(values()[0]['approved_at']) + 10_000
        return sender.tick({SCOPE}, cfg(), now_ms=now, reader=lambda *a, **k: records,
                           acknowledge=lambda *a, **k: True, **kw)

    def test_bad_occurrence_does_not_starve_unrelated_valid_alert(self):
        good, bad, _ = values(); bad['entry'] = 'invalid'
        seen = []
        report = self.tick(transport.Sender(), [bad, good], post=lambda v, *a, **k: (seen.append(v), ack(v))[1])
        self.assertEqual(seen, [good]); self.assertEqual(report['recorded'], 1)
        self.assertEqual(report['status'], 'SOURCE_PARTIAL')

    def test_conflicting_duplicate_never_sent_but_other_coin_sent(self):
        first, good, _ = values(); bad = deepcopy(first); bad['entry'] = contract.price(float(bad['entry'])+.001)
        seen = []
        report = self.tick(transport.Sender(), [first, good, bad], post=lambda v, *a, **k: (seen.append(v), ack(v))[1])
        self.assertEqual(seen, [good]); self.assertEqual(report['status'], 'SOURCE_PARTIAL')

    def test_retained_alert_not_sent_without_fresh_source_observation(self):
        stale, fresh, _ = values(); sender = transport.Sender(); sender._merge([stale])
        seen = []
        batch = outbox.ReadBatch(); batch.extend([fresh]); batch.complete = False; batch.source_errors = 1
        self.tick(sender, batch, post=lambda v, *a, **k: (seen.append(v), ack(v))[1])
        self.assertEqual(seen, [fresh]); self.assertIn(stale['occurrence_id'], sender.pending)

    def test_retained_cancel_still_sent_with_partial_read_and_has_priority(self):
        old, fresh, _ = values(); old = cancel(old); sender = transport.Sender(); sender._merge([old])
        seen = []
        self.tick(sender, [fresh], post=lambda v, *a, **k: (seen.append(v['kind']), ack(v))[1])
        self.assertEqual(seen, ['CANCEL', 'ALERT'])

    def test_shared_outage_blocks_all_alerts_but_keeps_cancellation(self):
        old, fresh, _ = values(); old = cancel(old); sender = transport.Sender(); sender._merge([old, fresh])
        seen = []
        def unavailable(*a, **kw): raise RuntimeError('DB_FAILED')
        report = sender.tick({SCOPE}, cfg(), now_ms=contract.moment_ms(old['source_as_of']),
            reader=unavailable, post=lambda v, *a, **k: (seen.append(v['kind']), ack(v))[1],
            acknowledge=lambda *a, **k: True)
        self.assertEqual(seen, ['CANCEL']); self.assertEqual(report['status'], 'SOURCE_UNAVAILABLE')
        self.assertIn(fresh['occurrence_id'], sender.pending)

    def test_cancel_dominates_duplicate_alert_and_ack_never_retimes(self):
        original = values()[0]; withdrawn = cancel(original); sender = transport.Sender(); seen = []
        self.tick(sender, [original, withdrawn, original], post=lambda v, *a, **k: (seen.append(v), ack(v))[1])
        self.assertEqual(seen, [withdrawn])
        self.assertEqual(seen[0]['expires_at'], original['expires_at'])
        self.tick(sender, [original], post=Mock(side_effect=AssertionError('NO_REPLAY')))

    def test_row_ack_failure_rotates_with_original_identity_and_deadline(self):
        good = values()[0]; sender = transport.Sender(); seen = []
        now = contract.moment_ms(good['approved_at'])+10_000
        def post(v, *a, **k): seen.append(deepcopy(v)); return ack(v)
        report = sender.tick({SCOPE}, cfg(), now_ms=now, reader=lambda *a, **k:[good], post=post,
            acknowledge=Mock(side_effect=TimeoutError('ACK_UNKNOWN')))
        self.assertEqual(report['deferred'], 1)
        self.tick(sender, [good], post=post)
        self.assertEqual(seen, [good, good]); self.assertEqual(sender.pending, {})

    def test_unexpected_forwarder_cycle_failure_recovers_at_existing_cadence(self):
        process = forwarder.Forwarder(forwarder.configuration(environment()))
        stop = Mock(); stop.is_set.side_effect = [False, False]; stop.wait.side_effect = [False, True]
        reports = []
        with patch.object(process, 'run_once', side_effect=[RuntimeError('SECRET'), {'status':'COMPLETED'}]):
            process.run(stop, emit=reports.append)
        self.assertEqual([r['status'] for r in reports], ['FORWARD_CYCLE_UNAVAILABLE', 'COMPLETED'])
        self.assertEqual([c.args for c in stop.wait.call_args_list], [(30,), (30,)])
        self.assertNotIn('SECRET', str(reports))
        self.assertIsNone(reports[0]['attempted'])


class WorkerRetryTests(unittest.IsolatedAsyncioTestCase):
    def test_transient_delays_are_bounded_without_changing_source_deadline(self):
        f = approved_fixture(); now = f['now_ms']; w = worker.SolProximityWorker(clock=lambda:now)
        for delay in (10_000, 20_000, 30_000, 30_000):
            with redirect_stdout(io.StringIO()): w.evidence_error(TimeoutError())
            self.assertEqual(w.retry_ms-now, delay); self.assertEqual(w.runtime['retry_class'], 'TRANSIENT')
        self.assertEqual(f['state']['intents'][0]['expires_ms'], f['approved_ms']+90_000)

    def test_integrity_failure_and_rate_limit_keep_conservative_delays(self):
        now = 1_790_000_000_000; w = worker.SolProximityWorker(clock=lambda:now)
        for error, delay in ((source.HyperliquidSourceError('HTTP_429'), M),
                             (source.HyperliquidSourceError('RATE_LIMIT_COOLDOWN'), M),
                             (source.HyperliquidSourceError('RETENTION_EXPIRED'), 5*M),
                             (ValueError('corrupt evidence'), 5*M)):
            with redirect_stdout(io.StringIO()): w.evidence_error(error)
            self.assertEqual(w.retry_ms-now, delay)

    async def test_transient_source_retries_and_resumes_on_existing_tick(self):
        f = approved_fixture(); current = [f['now_ms']]; w = worker.SolProximityWorker(spec=SPECS['HYPE'], clock=lambda:current[0])
        w.allowed = lambda _: True; w.reprice_bundle = AsyncMock(side_effect=[TimeoutError(), {}])
        w.initialize = AsyncMock(return_value={}); w.monitor = AsyncMock(return_value={})
        w.caught_up = lambda _: True; w.db = AsyncMock(return_value='OK')
        with patch.object(signal, 'decode_bundle', return_value={'rows':[]}), redirect_stdout(io.StringIO()):
            await w.observe({}, 1)
            await w.observe({}, 1)
            self.assertEqual(w.reprice_bundle.await_count, 1)
            current[0] += 10_000
            await w.observe({}, 1)
        self.assertEqual(w.reprice_bundle.await_count, 2)
        self.assertTrue(w.runtime['ready']); self.assertEqual(w.transient_failures, 0)
        self.assertEqual(w.retry_ms, 0)

    async def test_unknown_only_legacy_evidence_kept_without_unusable_history_fetch(self):
        f = approved_fixture(); now = f['now_ms']; state = deepcopy(f['state'])
        state['bar_cursor_ms'] = now//M*M-M
        old = signal.initial(now-10*M); p = deepcopy(state['active'][0]); p['status'] = 'UNKNOWN'
        old['active'] = [p]; old['source_route'] = 'legacy'; state['legacy_source_state'] = old
        before = deepcopy(state)
        legacy = Mock(); legacy.fill.side_effect = AssertionError('UNNECESSARY_LEGACY_HISTORY')
        w = worker.SolProximityWorker(clock=lambda:now, legacy_cache=legacy)
        self.assertTrue(w.caught_up(state))
        with patch.object(store, 'snapshot', return_value=state):
            result = await w.monitor('scope', state)
        legacy.fill.assert_not_called(); self.assertEqual(result, before)
        self.assertEqual(w.runtime['legacy_unknown'], 1)
        old['active'][0]['status'] = 'OPEN'
        self.assertFalse(w.caught_up(state))
        with self.assertRaisesRegex(AssertionError, 'UNNECESSARY_LEGACY_HISTORY'):
            await w.monitor('scope', state)


if __name__ == '__main__': unittest.main()
