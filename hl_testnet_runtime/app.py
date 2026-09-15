"""Minimal static WSGI health page. Requests never trigger checks or orders."""
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
    # Separate read-only task; never replace or implicitly activate a sender.
    from .checks import decode
    from .guarded_execution import review_only
    raw = os.environ.get('HL_TESTNET_REVIEW_SIGNAL', '')
    try:
        if not raw or len(raw) > 2048 or os.environ.get('HL_TESTNET_RUNTIME_MODE', 'read_only') != 'read_only':
            raise ValueError()
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
    # One of two read-only tasks per boot; HTTP requests never call either task.
    task = review_configured_signal if os.environ.get('HL_TESTNET_REVIEW_SIGNAL') else startup_check
    threading.Thread(target=task, daemon=True, name='testnet-preflight').start()


def application(environ, start_response):
    method = environ.get('REQUEST_METHOD', '')
    path = environ.get('PATH_INFO', '')
    if method not in ('GET', 'HEAD'):
        status, body = '405 Method Not Allowed', b'Read-only service. No input accepted.\n'
    elif path not in ('/', '/healthz') or environ.get('QUERY_STRING'):
        status, body = '404 Not Found', b'Not found\n'
    else:
        status = '200 OK'
        body = b'{"service":"hyperliquid-testnet-preflight","running":true,"read_only":true,"order_sending_enabled":false}'
    headers = [('Content-Type', 'application/json' if status == '200 OK' else 'text/plain'),
               ('Content-Length', str(len(body))), ('Cache-Control', 'no-store'),
               ('X-Content-Type-Options', 'nosniff'), ('Referrer-Policy', 'no-referrer'),
               ('Content-Security-Policy', "default-src 'none'; frame-ancestors 'none'")]
    start_response(status, headers)
    return [b'' if method == 'HEAD' else body]
