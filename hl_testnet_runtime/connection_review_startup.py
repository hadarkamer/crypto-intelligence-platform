"""Opt-in one-shot READ-ONLY connection and past-trade inspection."""
import json
import os


def _doge_card_review(env):
    from .doge_lifecycle_review import PublicReader, run
    class DiagnosticReader(PublicReader):
        def __init__(self):
            super().__init__()
            self.terminal_fields = []

        def read(self, kind, account, **kwargs):
            value = super().read(kind, account, **kwargs)
            if kind == 'orderStatus' and isinstance(value, dict):
                order = (value.get('order') or {}).get('order') or {}
                fields = {}
                for key in ('origSz','sz','limitPx','triggerPx','orderType','isTrigger','reduceOnly','side'):
                    item = order.get(key)
                    if type(item) in (str,bool,int) and len(str(item)) <= 80:
                        fields[key] = item
                if len(self.terminal_fields) < 3:
                    self.terminal_fields.append(fields)
            return value
    reader = DiagnosticReader()
    report = run(env, reader=reader)
    if report.get('failed_stage') == 'COLLECT_PUBLIC_EVIDENCE':
        report['public_terminal_field_diagnostics'] = reader.terminal_fields
    return report


def startup():
    from .two_account_execution import inspect_second
    from .saved_trade_review import review_saved_trade
    reviews = [('testnet_second_connection', inspect_second),
               ('testnet_saved_trade_review', review_saved_trade)]
    if os.environ.get('HL_TESTNET_FILLED_PREPARATION') == 'prepare_second_account_no_orders_v1':
        from .filled_trial_runtime import inspect_preparation
        reviews.append(('testnet_filled_preparation', inspect_preparation))
    if os.environ.get('HL_TESTNET_CLOSED_CARD_REVIEW') == 'saved_doge_shadow_v1':
        reviews.append(('testnet_doge_lifecycle', _doge_card_review))
    for label, function in reviews:
        try:
            report = function(os.environ)
        except Exception:
            report = {'status':'READ_ONLY_REVIEW_UNAVAILABLE','order_requests_sent':0}
        print(json.dumps({label:report}, sort_keys=True), flush=True)
