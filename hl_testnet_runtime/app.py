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
    # Only fixed status codes/booleans/source label. Never config, amounts, keys.
    print(json.dumps({'testnet_runtime': report}, sort_keys=True), flush=True)


def start_read_only_check():
    # Exactly one read-only pass per worker boot. Restarts cannot submit trades.
    threading.Thread(target=startup_check, daemon=True, name='testnet-preflight').start()


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
