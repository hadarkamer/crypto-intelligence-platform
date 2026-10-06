from __future__ import annotations

import unittest

import research_formula_ordered_v7 as evidence
import research_ordered_acceptance_policy as policy
import research_ordered_question_catalog as questions
import research_ordered_validation as validation
from research_ordered_validation_selftest import SCOPE


class PolicyTests(unittest.TestCase):
    def test_only_new_catalog_receives_exact_bound_policy(self):
        candidate=next(c for c in questions.candidates() if 'CORE_PRICE_OI_TOTAL_65' in c['formula_id'])
        scope={**SCOPE,'candidate_key':candidate['formula_id']}
        exact=validation.binding(scope,candidate)
        configured=policy.policy_for(exact,candidate)
        self.assertEqual(configured['binding_sha256'],validation.digest(exact))
        self.assertEqual(configured['combination'],'PROBABILITY_AND_ASYMMETRY')
        self.assertEqual(validation.validate_acceptance(configured,exact),configured)
        old=evidence.candidate_catalog()[0]
        old_scope={**SCOPE,'candidate_key':old['formula_id']}
        self.assertIsNone(policy.policy_for(validation.binding(old_scope,old),old))


if __name__ == '__main__':
    unittest.main()
