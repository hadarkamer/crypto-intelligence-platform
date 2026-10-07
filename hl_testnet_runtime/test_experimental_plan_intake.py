"""Prospective endpoint authentication and default-off isolation regressions."""
from io import BytesIO
import json
import unittest
from unittest.mock import Mock, patch

import alert_cards_wire
from . import app
from . import experimental_plan_intake as intake
from .experimental_plan_store import PlanStoreError
from .test_experimental_plan_store import maxpain_plan, FakeJournal, NOW


class ExperimentalIntakeTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'http.client.HTTPSConnection', 'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def request(self, body=b'{}', *, auth=True, **changes):
        env = dict(REQUEST_METHOD='POST', PATH_INFO=intake.PATH, QUERY_STRING='',
            CONTENT_TYPE='application/json', CONTENT_LENGTH=str(len(body)),
            **{'wsgi.input': BytesIO(body)})
        if auth:
            env.update(HTTP_X_PLAN_TIMESTAMP='1791302400',
                       HTTP_X_PLAN_SIGNATURE=intake.signature('a'*64, '1791302400', body))
        env.update(changes)
        status = []
        result = app.application(env, lambda code, headers: status.append((code, headers)))
        return status[0][0], json.loads(b''.join(result))

    def test_disabled_path_does_not_initialize_db_metadata_or_dispatch(self):
        with patch.dict('os.environ', {}, clear=True), patch.object(intake, 'PlanStore') as store:
            self.assertEqual(self.request()[0], '404 Not Found')
            store.assert_not_called()

    def test_new_mode_requires_own_secret_activation_time_and_existing_testnet_gates(self):
        good = dict(HL_TESTNET_EXPERIMENTAL_PLAN_INTAKE=intake.MODE,
            HL_TESTNET_EXPERIMENTAL_PLAN_SECRET='a'*64,
            HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE='2026-10-06T00:00:00+00:00')
        with patch('hl_testnet_runtime.alert_cards_intake.enabled', return_value=True):
            self.assertTrue(intake.enabled(good))
            for key in good:
                missing = dict(good); missing.pop(key)
                self.assertFalse(intake.enabled(missing))
            self.assertFalse(intake.enabled({**good, 'HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE':'yesterday'}))
        with patch('hl_testnet_runtime.alert_cards_intake.enabled', return_value=False):
            self.assertFalse(intake.enabled(good))

    def test_signature_is_path_scoped_and_old_delivery_auth_cannot_replay(self):
        raw = b'{}'; stamp = '1791302400'; now = int(stamp)
        signature = intake.signature('a'*64, stamp, raw)
        self.assertTrue(intake.authenticate('a'*64, stamp, signature, raw, now))
        self.assertFalse(intake.authenticate('a'*64, stamp, alert_cards_wire.signature('a'*64, stamp, raw), raw, now))
        for key, time, digest, body, clock in (
            ('b'*64, stamp, signature, raw, now), ('a'*64, stamp, signature, b'[]', now),
            ('a'*64, stamp, signature, raw, now+61), ('a'*64, stamp, signature, raw, now-61),
            ('a'*64, None, signature, raw, now), ('a'*64, stamp, None, raw, now)):
            self.assertFalse(intake.authenticate(key, time, digest, body, clock))

    def test_json_rejects_duplicate_keys_nonfinite_and_unbounded_body(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'\xff',
                    b' ', b'[]'*65536, b'{', b''):
            with self.subTest(raw=raw[:25]):
                with self.assertRaises(PlanStoreError):
                    intake.decoded(raw)

    def _active(self):
        return patch.dict('os.environ', {'HL_TESTNET_EXPERIMENTAL_PLAN_SECRET':'a'*64,
             'HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE':'2026-10-06T00:00:00+00:00'}, clear=True)

    def test_bad_http_auth_or_schema_has_no_db_side_effect(self):
        with self._active(), patch.object(intake, 'enabled', return_value=True), \
             patch.object(intake.time, 'time', return_value=1791302400), \
             patch.object(intake.PostgresJournal, 'from_env') as journal:
            self.assertEqual(self.request(auth=False)[0], '403 Forbidden')
            self.assertEqual(self.request(REQUEST_METHOD='GET')[0], '405 Method Not Allowed')
            self.assertEqual(self.request(QUERY_STRING='key=a')[0], '405 Method Not Allowed')
            self.assertEqual(self.request(CONTENT_TYPE='text/plain')[0], '415 Unsupported Media Type')
            self.assertEqual(self.request(CONTENT_LENGTH='65537')[0], '413 Content Too Large')
            self.assertEqual(self.request()[0], '400 Bad Request')
            journal.assert_not_called()

    def test_commit_failure_has_no_success_acknowledgement_or_raw_error(self):
        with self._active(), patch.object(intake, 'enabled', return_value=True), \
             patch.object(intake.time, 'time', return_value=1791302400), \
             patch.object(intake.PostgresJournal, 'from_env'), \
             patch.object(intake, 'PlanStore'), \
             patch.object(intake, 'accept', side_effect=RuntimeError('private database endpoint')):
            status, body = self.request(json.dumps(maxpain_plan()).encode())
            self.assertEqual(status, '503 Service Unavailable')
            self.assertNotIn('private', str(body))

    def test_schema_failure_is_retryable_not_success_or_invalid_source(self):
        with self._active(), patch.object(intake, 'enabled', return_value=True), \
             patch.object(intake.time, 'time', return_value=1791302400), \
             patch.object(intake.PostgresJournal, 'from_env'), \
             patch.object(intake, 'PlanStore'), \
             patch.object(intake, 'accept', side_effect=PlanStoreError('EXPERIMENTAL_SCHEMA_OR_DOMAIN_MISMATCH')):
            self.assertEqual(self.request(json.dumps(maxpain_plan()).encode())[0], '503 Service Unavailable')

    def test_real_store_lost_commit_ack_returns_503_then_duplicate_receipt(self):
        from datetime import datetime
        from .experimental_plan_store import PlanStore
        journal = FakeJournal()
        journal.lose_commit_ack_once = True
        message = maxpain_plan()
        raw = json.dumps(message).encode()
        now = datetime.fromisoformat(NOW).timestamp()
        stamp = str(int(now))
        active = dict(HL_TESTNET_RUNTIME_MODE='read_only',
            RENDER_SERVICE_ID='srv-dakptbh594qs7395460g',
            HL_TESTNET_CARDS_INTAKE='record_only_v1',
            HL_TESTNET_CARDS_PHASE1='record_only_v1',
            HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',
            HL_TESTNET_CARDS_INTAKE_SECRET='b' * 64,
            HL_TESTNET_EXPERIMENTAL_PLAN_INTAKE=intake.MODE,
            HL_TESTNET_EXPERIMENTAL_PLAN_SECRET='a' * 64,
            HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE='2026-10-06T00:00:00+00:00')
        signed = dict(HTTP_X_PLAN_TIMESTAMP=stamp,
            HTTP_X_PLAN_SIGNATURE=intake.signature('a' * 64, stamp, raw))
        with patch.dict('os.environ', active, clear=True), \
             patch.object(intake.time, 'time', return_value=now), \
             patch.object(intake.PostgresJournal, 'from_env', return_value=journal), \
             patch.object(PlanStore, 'initialize', side_effect=AssertionError('NO_REQUEST_DDL')):
            self.assertTrue(intake.enabled(active))
            status, reply = self.request(raw, **signed)
            self.assertEqual((status, reply['status']),
                ('503 Service Unavailable', 'EXPERIMENTAL_RECORDING_UNAVAILABLE'))
            self.assertEqual(len(journal.events), 1)
            status, reply = self.request(raw, **signed)
            self.assertEqual((status, reply['status'], reply['revision'], reply['record_only']),
                ('200 OK', 'DUPLICATE', 1, True))
        self.assertEqual(len(journal.events), 1)
        state = PlanStore(journal).load(message['occurrence_id'])
        self.assertEqual(state['initial_source'], message)
        self.assertIsNone(state['strategy'])


if __name__ == '__main__':
    unittest.main()
