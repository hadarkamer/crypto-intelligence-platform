"""Opt-in one-shot READ-ONLY connection and past-trade inspection."""
import json
import os


def startup():
    from .two_account_execution import inspect_second
    from .saved_trade_review import review_saved_trade
    for label, function in (('testnet_second_connection', inspect_second),
                            ('testnet_saved_trade_review', review_saved_trade)):
        try:
            report = function(os.environ)
        except Exception:
            report = {'status':'READ_ONLY_REVIEW_UNAVAILABLE','order_requests_sent':0}
        print(json.dumps({label:report}, sort_keys=True), flush=True)
