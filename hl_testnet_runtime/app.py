"""Public read-only health, plus separate authenticated record-only intake.

No HTTP path can sign or submit exchange orders.
"""
import json
import hmac
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
    if path == '/internal/testnet-diagnostics/v1':
        # Dedicated, authenticated journal reads only. This surface never
        # instantiates a venue, signer or execution controller.
        from .testnet_diagnostics import application as diagnostics
        return diagnostics(environ, start_response)
    if path == '/internal/testnet-trades/v1':
        # Private read-only projection from this service's durable journal.
        # The Telegram bot receives no database credential or trading key.
        token = os.environ.get('HL_TESTNET_REPORT_API_TOKEN', '')
        supplied = environ.get('HTTP_X_TRADE_REPORT_TOKEN', '')
        if (method != 'GET' or environ.get('QUERY_STRING') or len(token) < 32
                or len(token) > 256 or not isinstance(supplied, str)
                or not hmac.compare_digest(token, supplied)):
            status, body = '404 Not Found', b'Not found\n'
        else:
            try:
                from .trade_report_store import load_trades
                body = json.dumps(load_trades(), separators=(',', ':'), allow_nan=False).encode()
                if len(body) > 262144:
                    raise ValueError('REPORT_TOO_LARGE')
                status = '200 OK'
            except Exception:
                status, body = '503 Service Unavailable', b'Report unavailable\n'
        start_response(status,[('Content-Type','application/json' if status == '200 OK' else 'text/plain'),
            ('Content-Length',str(len(body))),('Cache-Control','no-store'),
            ('X-Content-Type-Options','nosniff')])
        return [body]
    if method not in ('GET', 'HEAD'):
        status, body = '405 Method Not Allowed', b'No public controls. No input accepted.\n'
    elif path not in ('/', '/healthz') or environ.get('QUERY_STRING'):
        status, body = '404 Not Found', b'Not found\n'
    else:
        status = '200 OK'
        mode = os.environ.get('HL_TESTNET_RUNTIME_MODE')
        controlled = mode == 'single_testnet_attempt_v1'
        filled = mode == 'filled_card_controlled_v1'
        long_stream = mode == 'long_stream_testnet_v1'
        monitoring = mode == 'cancel_monitor_testnet_v1'
        rehearsal = mode == 'cancel_rehearsal_testnet_v1'
        report = {'service':'hyperliquid-testnet-preflight','running':True,
            'read_only':not (controlled or filled or long_stream or monitoring or rehearsal),'single_attempt_configured':controlled,
            'public_order_controls':False,'continuous_trading':long_stream and (
                os.environ.get('HL_TESTNET_LONG_ENTRY_ENABLED')=='true' or
                (os.environ.get('HL_TESTNET_SHORT_STREAM')=='approved_alerts_v1' and
                 os.environ.get('HL_TESTNET_SHORT_ENTRY_ENABLED')=='true')),
            'cancellation_monitor_configured':monitoring,'technical_rehearsal_configured':rehearsal}
        if filled:
            from .filled_trial_runtime import health as filled_health
            report['controlled_card'] = filled_health()
        if long_stream:
            from .long_stream_runtime import health as long_health
            report['long_stream'] = long_health()
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
