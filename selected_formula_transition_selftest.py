"""Focused no-network source retirement; old fixtures are historical state only."""
from copy import deepcopy
from unittest.mock import patch
import unittest

import alert_delivery_policy as policy
import approved_alert_contract as contract
import selected_six_formula_source as source
import selected_six_formula_store as store
import selected_six_formula_signal as rules
import selected_six_formula_signal_selftest as fixture
from selected_six_formula_source_fixtures import watch_capture

KEEP = frozenset((rules.SFF1361, rules.XRP_LIQUIDITY_INVERSE, 'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK'))


def legacy_state(formula):
    # Generate a previously valid frozen approval; new-admission policy is
    # overridden ONLY while constructing the pre-upgrade database fixture.
    current, history = fixture.make_ct009() if formula == rules.CT009 else fixture.make_oconf()
    now = fixture.AT + 70_000 if formula == rules.CT009 else fixture.AT + 10_000
    evaluate = rules.evaluate_ct009 if formula == rules.CT009 else rules.evaluate_oconf0034_13
    result = evaluate(current, history, now)
    assert result['signal'], result
    state = store._initial(fixture.AT - 1000)
    with patch.object(policy, 'selected_rule_enabled', return_value=True), patch.object(contract, 'new_entry_allowed', return_value=True):
        store.approve_state(state, result['decision'], dict(cycle_id='historical',episode_key='historical-wave',timeframe='3d'), now)
    return state, result['decision'], now


class TransitionTests(unittest.TestCase):
    def setUp(self):
        self.gate = patch.object(policy,'selected_rule_enabled',side_effect=lambda r:r in KEEP)
        self.gate.start()
        self.addCleanup(self.gate.stop)

    def test_watch_keeps_sol_xrp_and_ordinary_raw_intact(self):
        score, decisions, raw = watch_capture(fixture.AT)
        before = deepcopy((score,decisions,raw))
        mapped = source.map_watch_capture(score,decisions,raw,fixture.AT+10)
        self.assertEqual({x['formula_id'] for x in mapped['candidates']}, {rules.SFF1361,rules.XRP_LIQUIDITY_INVERSE})
        self.assertEqual(mapped['confirmations'], [])
        self.assertEqual((score,decisions,raw), before)

    def test_paused_ct_does_not_read_delivered_source(self):
        class NoQuery:
            def execute(self,*args): raise AssertionError('paused CT queried history')
        self.assertEqual(source.read_delivered(NoQuery(),store._initial(fixture.AT),fixture.AT),0)

    def test_saved_approvals_cancel_remainder_without_claiming_exchange_success(self):
        for formula in (rules.CT009,rules.OCONF0034_13):
            with self.subTest(formula=formula):
                state,decision,now = legacy_state(formula)
                identity=state['active'][0]['position_id']
                state['candidates']['queued'] = {'formula_id':formula,'snapshot':{'source_event_id':'late'}}
                store.apply_entry_policy(state,now+1)
                self.assertEqual(state['active'],[])
                self.assertEqual(state['candidates'],{})
                self.assertEqual(state['history'][0]['position_id'],identity)
                self.assertEqual(state['intents'][0]['status'],'CANCELED')
                payload=state['intents'][0]['payload']
                self.assertEqual(payload['kind'],'CANCEL')
                self.assertEqual(payload['cancel_reason'],'FORMULA_NEW_ENTRIES_DISABLED')
                self.assertEqual(contract.validate(payload)['rule_id'],formula)
                snapshot=deepcopy(state)
                store.apply_entry_policy(state,now+2)
                self.assertEqual(state,snapshot)
                duplicate=store.approve_state(state,decision,{},now+2)
                self.assertEqual(duplicate,dict(created=False,reason='FORMULA_DISABLED'))

    def test_prior_unknown_telegram_attempt_is_not_retried(self):
        state,_,now=legacy_state(rules.CT009)
        state['intents'][0]['status']='UNKNOWN'
        store.apply_entry_policy(state,now+1)
        self.assertEqual(state['intents'][0]['status'],'UNKNOWN')
        self.assertEqual(state['intents'][0]['payload']['kind'],'CANCEL')

    def test_keep_candidate_and_activation_clock_preserved(self):
        state=store._initial(fixture.AT)
        state['candidates']['keep']={'formula_id':rules.SFF1361,'snapshot':{}}
        before=deepcopy(state)
        store.apply_entry_policy(state,fixture.AT+1)
        self.assertEqual(state,before)

    def test_quiet_delivery_profile_preserves_retained_approval_and_candidate(self):
        state=store._initial(fixture.AT-1000)
        snapshot=fixture.make_sff()
        decision=rules.evaluate_sff1361(snapshot,fixture.AT+70_000)['decision']
        store.approve_state(state,decision,dict(cycle_id='kept',episode_key='kept',timeframe='30m'),fixture.AT+70_000)
        state['candidates']['kept']={'formula_id':rules.SFF1361,'snapshot':deepcopy(snapshot)}
        retained=deepcopy(state)
        with patch.object(policy,'selected_rule_enabled',return_value=False):
            store.apply_entry_policy(state,fixture.AT+80_000)
            self.assertEqual(state,retained)
            self.assertEqual(source.evaluate_ready(state,[],fixture.AT+80_000),0)
            self.assertEqual(state,retained)
            with patch.object(store,'load',return_value=state),patch.object(store,'save'):
                self.assertIsNone(store.claim(object(),'general-watch:test',fixture.AT+80_000))
        self.assertEqual(state['intents'][0]['status'],'PENDING')
        self.assertEqual(state['active'],retained['active'])
        self.assertEqual(state['candidates'],retained['candidates'])

    def test_quiet_profile_still_retires_only_explicit_paused_identities(self):
        state,_,now=legacy_state(rules.CT009)
        with patch.object(policy,'selected_rule_enabled',return_value=False):
            store.apply_entry_policy(state,now+1)
        self.assertEqual(state['history'][0]['rule_id'],rules.CT009)
        self.assertEqual(state['intents'][0]['payload']['kind'],'CANCEL')

if __name__=='__main__': unittest.main()
