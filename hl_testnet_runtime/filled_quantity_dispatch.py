"""Connected filled-quantity execution path, LOCKED by default.

Real PostgreSQL + existing card/lifecycle/reader + Testnet-only transport. The
same controller is tested by replacing ONLY the external venue boundary. No
HTTP control route, automatic startup, application callbacks or extra timer.
No existing normalTpsl order is converted. Release requires a separate approval.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import http.client
import json
import re
import time

from . import card_lifecycle as life, filled_quantity_exits as selected
from . import card_exit_recovery as recovery, card_sync_evidence as evidence
from . import filled_pending_cancel as half_cancel
from . import residual_exit_fence as residual
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .trade_card_store import CardStore
from . import checks, two_account_execution as roles

VERSION = 'connected-filled-quantity-dispatch-v1'
AFTER_EXIT = 'cancel_remainder_after_exit_v1'
HOST = 'api.hyperliquid-testnet.xyz'


def rejection_reason(raw):
    """Bound and redact a venue error before it reaches durable state or logs."""
    if not isinstance(raw, dict):
        return None
    message = raw.get('response') if raw.get('status') == 'err' else None
    if message is None and raw.get('status') == 'ok':
        response = raw.get('response')
        data = response.get('data') if isinstance(response, dict) else None
        statuses = data.get('statuses') if isinstance(data, dict) else None
        if (isinstance(statuses, list) and len(statuses) == 1
                and isinstance(statuses[0], dict) and set(statuses[0]) == {'error'}):
            message = statuses[0]['error']
    if not isinstance(message, str) or not 0 < len(message) <= 1024:
        return None
    message = re.sub(r'0x[0-9a-fA-F]{8,}', '[address]', message)
    message = re.sub(r'\b[0-9a-fA-F]{32,}\b', '[hex]', message)
    message = re.sub(r'\b[A-Za-z0-9_+/=-]{48,}\b', '[token]', message)
    message = re.sub(r'\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b', '[email]', message)
    if len(message) > 240 or re.search(r'[^\x20-\x7e]', message):
        return None
    return message


def rejection_subject(raw, account, agent):
    """Classify a recovered signer without persisting either wallet address."""
    if not isinstance(raw, dict):
        return None
    message = raw.get('response') if raw.get('status') == 'err' else None
    match = (re.search(r'User or API Wallet (0x[0-9a-fA-F]{40}) does not exist', message)
             if isinstance(message, str) and len(message) <= 1024 else None)
    if match is None:
        return None
    recovered = match.group(1).lower()
    if recovered == str(agent).lower():
        return 'AGENT'
    if recovered == str(account).lower():
        return 'ACCOUNT'
    return 'UNEXPECTED_SIGNER'


def canonical_wire_action(action):
    """Rebuild the exact SDK wire order after JSONB has discarded key order."""
    if not isinstance(action, dict):
        raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
    kind = action.get('type')
    if kind == 'batchModify':
        if (set(action) != {'type','modifies'} or not isinstance(action['modifies'],list)
                or len(action['modifies']) != 1):
            raise DispatchError('ACTION_WIRE_SHAPE_INVALID')
        item=action['modifies'][0]
        if (not isins…31542 tokens truncated…)
    def test_plan_digest_bound_to_actual_input(self):
        one=self.plan_case()['budget_diagnostics']['plan_sha256']
        two=self.plan_case(dict(reversed(list(PLAN.items()))))['budget_diagnostics']['plan_sha256']
        three=self.plan_case({**PLAN,'take_profit':'111'})['budget_diagnostics']['plan_sha256']
        self.assertEqual(one,two)
        self.assertNotEqual(one,three)
    def test_mark_value_lab_bound_not_raised(self):
        self.reader.capacity['markPx']='6000'
        self.reader.capacity['availableToTrade']=['100000','100000']
        self.reader.spot['balances'][0]['total']='100000'
        result=self.plan_case()
        self.assertFalse(result['test_plan_checked'])
        self.assertIn('mark_notional_within_lab_cap',result['budget_diagnostics']['failed_checks'])
    def test_ten_dollar_risk_does_not_shrink_to_make_budget_pass(self):
        self.reader.capacity['leverage']['value']=5
        # 10/(100-99) = 10 units, not 1; cap deliberately excludes it.
        result=self.plan_case({**PLAN,'stop':'99'})
        self.assertEqual(result['status'],'QUANTITY_EXCEEDS_EXCHANGE_CAP')
        self.assertFalse(result['budget_diagnostics']['risk_rule_changed'])
    def test_legacy_wrapper_remains_conservative_without_context(self):
        from decimal import Decimal as D
        with self.assertRaises(checks.Blocked):
            checks.plan_check(PLAN,'BTC',2,D('100'),D('100'),D('5'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
