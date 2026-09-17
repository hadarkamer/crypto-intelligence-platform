"""Public read-only health, plus separate authenticated record-only intake.

No HTTP path can sign or submit exchange orders.
"""
import json
import os
import threading
from .checks import run_check

CONFIG_NAMES = ('HL_TESTNET_RUNTIME_MODE', 'HL_TESTNET_ACCOUNT_ADDRESS',
                'HL_TESTNET_AGENT_ADDRESS', 'HL_TESTNET_AGENT_KEY',
                'HL_TESTNET_CHECK_SYMBOL', 'HL_TESTNET_CHECK_PLAN')


def startup_check():
    config = {name: os.environ.get(name, '') for name in CONFIG_NAMES if name in os.environ}
    report = run_check(config)
    print(json.dumps({'testnet_runtime': report}, sort_keys=True), flush=True)


def review_configured_signal():
    from .checks import decode
    from .guarded_execution import review_only
    raw = os.environ.get('HL_TESTNET_REVIEW_SIGNAL', '')
    try:
        if not raw or len(raw) > 2048 or os.environ.get('HL_TESTNET_RUNTIME_MODE', 'read_only') != 'read_only':
            raise ValueError()
        policy = os.environ.get('HL_TESTNET_PRICE_ROUNDING', '')
        if policy:
            from .price_precision import POLICY, review_rounded_signal
            if policy != POLICY:
                raise ValueError()
            review_only = review_rounded_signal
        report = review_only(decode(raw),
            account=os.environ.get('HL_TESTNET_ACCOUNT_ADDRESS', ''),
            agent=os.environ.get('HL_TESTNET_AGENT_ADDRESS', ''),
            journal=os.environ.get('HL_TESTNET_JOURNAL_PATH') or None,
            exit_type=os.environ.get('HL_TESTNET_EXIT_TYPE') or None)
    except Exception:
        report = {'phase': 'review_only', 'status': 'REVIEW_CONFIGURATION_INVALID',
                  'order_requests_sent': 0, 'signing_tested': False}
    print(json.dumps({'testnet_submission_review': report}, sort_keys=True), flush=True)


def start_read_only_check():
    task = review_configured_signal if os.environ.get('HL_TESTNET_REVIEW_SIGNAL') else startup_check
    threading.Thread(target=task, daemon=True, name='testnet-preflight').start()


def application(environ, start_response):
    method = environ.get('REQUEST_METHOD', '')
    path = environ.get('PATH_INFO', '')
    if path == '/internal/testnet-cards/v1':
        from .alert_cards_intake import application as intake
        return intake(environ, start_response)
    if method not in ('GET', 'HEAD'):
        status, body = '405 Method Not Allowed', b'No public controls. No input accepted.\n'
    elif path not in ('/', '/healthz') or environ.get('QUERY_STRING'):
        status, body = '404 Not Found', b'Not found\n'
    else:
        status = '200 OK'
        mode = os.environ.get('HL_TESTNET_RUNTIME_MODE')
        controlled = mode == 'single_testnet_attempt_v1'
        monitoring = mode == 'cancel_monitor_testnet_v1'
        rehearsal = mode == 'cancel_rehearsal_testnet_v1'
        report = {'service':'hyperliquid-testnet-preflight','running':True,
            'read_only':not (controlled or monitoring or rehearsal),'single_attempt_configured':controlled,
            'public_order_controls':False,'continuous_trading':False,
            'cancellation_monitor_configured':monitoring,'technical_rehearsal_configured':rehearsal}
        if monitoring:
            from .pending_cancel_monitor import health
            report['monitor'] = health()
        if os.environ.get('HL_TESTNET_CARD_SYNC') == 'registered_readonly_v1':
            from .card_sync import health as card_health
            report['card_sync'] = card_health()
        body = json.dumps(report).encode()
    headers = [('Content-Type', 'application/json' if status == '200 OK' else 'text/plain'),
               ('Content-Length', str(len(body))), ('Cache-Control', 'no-store'),
               ('X-Content-Type-Options', 'nosniff'), ('Referrer-Policy', 'no-referrer'),
               ('Content-Security-Policy', "default-src 'none'; frame-ancestors 'none'")]
    start_response(status, headers)
    return [b'' if method == 'HEAD' else body]
