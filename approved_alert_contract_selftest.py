"""Approved signal invariants, independent of source formula computation."""
from copy import deepcopy
import unittest

import approved_alert_contract as contract
import experimental_execution_contract as legacy
from approved_alert_fixtures import BASE, maxpain_alert
from experimental_execution_fixtures import maxpain_message


class ApprovedAlertContractTests(unittest.TestCase):
    def test_exact_alert_prices_survive_detached_validation(self):
        source = maxpain_alert()
        result = contract.validate(source)
        self.assertEqual(tuple(result[k] for k in ('entry', 'stop', 'take_profit')),
                         ('93.544', '91.405', '95.3265'))
        source['entry'] = '1'
        self.assertEqual(result['entry'], '93.544')
        self.assertEqual(result['policy']['entry_kind'], 'LIMIT_AFTER_ALERT')

    def test_contract_does_not_reconstruct_or_recheck_formula_distance(self):
        source = maxpain_alert(entry='94', stop='90', take_profit='98')
        self.assertEqual(contract.validate(source)['entry'], '94')
        self.assertNotIn('source_price', source['proof'])

    def test_same_source_identity_as_prospective_protocol(self):
        source = maxpain_alert()
        self.assertEqual(contract.occurrence_id(source), legacy.occurrence_id(source))
        old = maxpain_message()
        self.assertEqual(contract.validate(old), legacy.validate(old))
        self.assertEqual(contract.plan_digest(old), legacy.plan_digest(old))
        self.assertFalse(contract.is_approved(old))

    def test_recipient_times_do_not_manufacture_occurrence_or_immutable_change(self):
        first = maxpain_alert()
        second = maxpain_alert(as_of_ms=BASE + 1000)
        second['created_at'] = contract.iso_ms(BASE - 165_000)
        contract.validate(second)
        self.assertEqual(first['occurrence_id'], second['occurrence_id'])
        self.assertEqual(contract.plan_digest(first), contract.plan_digest(second))

    def test_cancel_preserves_original_approval_terms_and_identity(self):
        approved = maxpain_alert()
        canceled = maxpain_alert(kind='CANCEL', as_of_ms=BASE + 300_000,
                                 cancel_reason='SOURCE_TAKE_TOUCHED')
        self.assertEqual(contract.plan_digest(approved), contract.plan_digest(canceled))
        self.assertEqual(approved['occurrence_id'], canceled['occurrence_id'])

    def test_incompatible_policy_prices_identity_and_lease_are_rejected(self):
        bad_cases = [
            ('version', 'unknown'), ('kind', 'HEARTBEAT'),
            ('source_state', 'PENDING'), ('execution_environment', 'mainnet'),
            ('symbol', 'ETH'), ('entry', '91'), ('entry', 93.544),
            ('take_profit', '90'), ('occurrence_id', 'b' * 64),
            ('valid_until', contract.iso_ms(BASE + 90_000)),
            ('expires_at', contract.iso_ms(BASE)),
            ('expires_at', contract.iso_ms(BASE + 90_001)),
            ('approved_at', contract.iso_ms(BASE + 1)),
            ('source_sequence', True),
        ]
        for key, value in bad_cases:
            with self.subTest(key=key):
                source = maxpain_alert(); source[key] = value
                with self.assertRaises(contract.ContractError):
                    contract.validate(source)
        source = maxpain_alert(); source['policy']['entry_kind'] = 'MARKET'
        with self.assertRaises(contract.ContractError):
            contract.validate(source)

    def test_changed_deadline_approval_or_prices_changes_immutable_digest(self):
        original = maxpain_alert()
        for key, value in (('entry', '93.5'),
                           ('expires_at', contract.iso_ms(BASE + 100_000))):
            changed = deepcopy(original); changed[key] = value
            if key == 'expires_at':
                with self.assertRaises(contract.ContractError):
                    contract.validate(changed)
            else:
                contract.validate(changed)
            self.assertEqual(original['occurrence_id'], changed['occurrence_id'])
            self.assertNotEqual(contract.plan_digest(original), contract.plan_digest(changed))


if __name__ == '__main__':
    unittest.main()
