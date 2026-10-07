"""Authenticated approved alerts reach durable records, never venue calls."""
from io import BytesIO
import json
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import BASE, maxpain_alert
from . import experimental_plan_intake as intake
from .experimental_plan_store import PlanStore
from .test_experimental_plan_store import FakeJournal


class ApprovedAlertIntakeTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'http.client.HTTPSConnection', 'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def request(self, message, *, key='a' * 64, now_ms=BASE + 1000):
        raw = json.dumps(message).encode()
        stamp = str(now_ms // 1000)
        env = dict(REQUEST_METHOD='POST', PATH_INFO=intake.PATH, QUERY_STRING='',
            CONTENT_TYPE='application/json', CONTENT_LENGTH=str(len(raw)),
            HTTP_X_PLAN_TIMESTAMP=stamp,
            HTTP_X_PLAN_SIGNATURE=intake.signature(key, stamp, raw),
            **{'wsgi.input': BytesIO(raw)})
        status = []
        with patch.object(intake.time, 'time', return_value=now_ms / 1000):
            result = intake.application(env, lambda code, headers: status.append(code))
        return status[0], json.loads(b''.join(result))

    def active(self):
        return patch.dict('os.environ', {
            'HL_TESTNET_EXPERIMENTAL_PLAN_SECRET': 'a' * 64,
            'HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE': contract.iso_ms(BASE - 60_000),
        }, clear=True)

    def test_authenticated_retry_returns_one_committed_approval(self):
        journal = FakeJournal(); receiver = PlanStore(journal)
        with self.active(), patch.object(intake, 'enabled', return_value=True), \
                patch.object(intake.PostgresJournal, 'from_env', return_value=journal):
            first = self.request(maxpain_alert())
            second = self.request(maxpain_alert())
        self.assertEqual(first[0], '200 OK')
        self.assertEqual(first[1]['status'], 'RECORDED')
        self.assertEqual(second[1]['status'], 'DUPLICATE')
        self.assertTrue(first[1]['record_only'])
        self.assertEqual(len(journal.events), 1)
        self.assertEqual(receiver.load(maxpain_alert()['occurrence_id'])['source']['source_state'], 'APPROVED')

    def test_wrong_signature_and_malformed_prices_never_open_database(self):
        with self.active(), patch.object(intake, 'enabled', return_value=True), \
                patch.object(intake.PostgresJournal, 'from_env') as journal:
            self.assertEqual(self.request(maxpain_alert(), key='b' * 64)[0], '403 Forbidden')
            malformed = maxpain_alert(); malformed['entry'] = '90'
            self.assertEqual(self.request(malformed)[0], '400 Bad Request')
            journal.assert_not_called()

    def test_late_first_receipt_is_acknowledged_as_retired_not_retried(self):
        journal = FakeJournal()
        with self.active(), patch.object(intake, 'enabled', return_value=True), \
                patch.object(intake.PostgresJournal, 'from_env', return_value=journal):
            status, receipt = self.request(maxpain_alert(), now_ms=BASE + 90_000)
        self.assertEqual(status, '200 OK')
        self.assertEqual(receipt['entry_permission'], 'RETIRED')
        state = PlanStore(journal).load(maxpain_alert()['occurrence_id'])
        self.assertEqual(state['terminal_reason'], 'APPROVED_ALERT_EXPIRED_BEFORE_ACCEPTANCE')


if __name__ == '__main__':
    unittest.main()
