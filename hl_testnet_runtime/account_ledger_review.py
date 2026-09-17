"""Bounded public Testnet history for explaining an observed balance difference.

No credentials, account changes or order calls. Report exact ledger fields rather
than assuming a one-dollar difference is a fee. Counterparty addresses are not
printed. This is an observation of a seven-day window, not lifetime accounting.
"""
from datetime import datetime, timezone
import hashlib
import http.client
import json
import re
import time

from . import checks

HOST = 'api.hyperliquid-testnet.xyz'
KINDS = ('userNonFundingLedgerUpdates', 'userFillsByTime', 'userFunding')
WEEK_MS = 7 * 24 * 60 * 60 * 1000
AMOUNTS = ('usdc', 'amount', 'fee', 'usdcValue', 'gas', 'gasFee',
           'activationFee', 'accountValue', 'netWithdrawnUsd')


class HistoryReader:
    def __init__(self):
        self.calls = 0

    def read(self, kind, account, start, end):
        if (HOST != 'api.hyperliquid-testnet.xyz' or kind not in KINDS
                or type(start) is not int or type(end) is not int
                or not 0 <= start < end or end-start > WEEK_MS):
            raise checks.Blocked('PUBLIC_HISTORY_QUERY_REQUIRED')
        account = checks.address(account)
        body = {'type':kind, 'user':account, 'startTime':start, 'endTime':end}
        connection = http.client.HTTPSConnection('api.hyperliquid-testnet.xyz', timeout=4)
        self.calls += 1
        started = time.monotonic()
        try:
            connection.request('POST', '/info', json.dumps(body).encode(),
                               {'Content-Type':'application/json'})
            response = connection.getresponse()
            if response.status != 200:
                raise checks.Blocked('PUBLIC_HISTORY_UNAVAILABLE')
            raw = response.read(checks.MAX_BYTES + 1)
            if len(raw) > checks.MAX_BYTES or time.monotonic()-started > 8:
                raise checks.Blocked('PUBLIC_HISTORY_BOUND_EXCEEDED')
            return checks.decode(raw)
        except (OSError, http.client.HTTPException):
            raise checks.Blocked('PUBLIC_HISTORY_UNAVAILABLE') from None
        finally:
            connection.close()


def _rows(value, start, end):
    if not isinstance(value, list) or len(value) > 2000:
        raise checks.Blocked('INVALID_PUBLIC_HISTORY')
    for row in value:
        if (not isinstance(row, dict) or type(row.get('time')) is not int
                or not start <= row['time'] <= end):
            raise checks.Blocked('INVALID_PUBLIC_HISTORY_TIME')
    return value


def _ledger_event(row, account):
    delta = row.get('delta')
    if (not isinstance(delta, dict) or not isinstance(delta.get('type'), str)
            or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', delta['type'])):
        raise checks.Blocked('INVALID_PUBLIC_LEDGER_EVENT')
    result = dict(type=delta['type'],
        at_utc=datetime.fromtimestamp(row['time']/1000,timezone.utc).isoformat(),
        delta_fields=sorted(k for k in delta if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', k)),
        amounts={})
    for key in AMOUNTS:
        if key in delta:
            value = delta[key]
            if type(value) not in (str, int, float):
                raise checks.Blocked('INVALID_PUBLIC_LEDGER_AMOUNT')
            result['amounts'][key] = format(checks.number(str(value),signed=True),'f')
    for key in ('user', 'destination', 'sender', 'recipient'):
        if key in delta:
            address = checks.address(delta[key])
            result[key+'_is_checked_account'] = address == account
    for key in ('coin', 'token'):
        if key in delta and isinstance(delta[key], str) and re.fullmatch(r'[A-Za-z0-9_:@.\-]{1,80}', delta[key]):
            result[key] = delta[key]
    if type(delta.get('toPerp')) is bool:
        result['to_perp'] = delta['toPerp']
    result['event_sha256'] = hashlib.sha256(json.dumps(row,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()
    return result


def review_history(account, *, client=None, end_ms=None):
    """Three reads maximum. No automated attribution of fees or net balance.

    Ledger excerpt is limited to 32 entries; truncation is explicit. Fill/funding
    counts only describe returned rows within the specified time window.
    """
    report = dict(environment='testnet', version='balance-ledger-observation-v1',
        status='PUBLIC_HISTORY_UNAVAILABLE', order_requests_sent=0, transfers_sent=0,
        account_settings_changes=0, signing_tested=False, fee_cause_inferred=False,
        public_reads=0, ledger_entries=[], reads={})
    baseline = getattr(client,'calls',0)
    try:
        account = checks.address(account)
        end = time.time_ns()//1_000_000 if end_ms is None else end_ms
        if type(end) is not int or end < WEEK_MS:
            raise checks.Blocked('INVALID_PUBLIC_HISTORY_TIME')
        start = end-WEEK_MS
        report.update(account_suffix=account[-4:], window_start_utc=datetime.fromtimestamp(
            start/1000,timezone.utc).isoformat(), window_end_utc=datetime.fromtimestamp(
            end/1000,timezone.utc).isoformat())
        client = HistoryReader() if client is None else client
        for kind in KINDS:
            try:
                rows = _rows(client.read(kind,account,start,end),start,end)
                if kind == 'userNonFundingLedgerUpdates':
                    report['ledger_entries'] = [_ledger_event(row,account)
                                               for row in sorted(rows,key=lambda r:r['time'])[:32]]
                    report['ledger_excerpt_truncated'] = len(rows)>32
                report['reads'][kind] = dict(status='OBSERVED', returned_count=len(rows),
                    may_be_truncated=len(rows)>=(2000 if kind=='userFillsByTime' else 500))
            except Exception:
                report['reads'][kind] = dict(status='PUBLIC_HISTORY_READ_UNAVAILABLE')
        report['status'] = ('PUBLIC_HISTORY_OBSERVED' if all(
            item['status']=='OBSERVED' for item in report['reads'].values()) else 'PUBLIC_HISTORY_PARTIAL')
    except Exception:
        report['status'] = 'PUBLIC_HISTORY_UNAVAILABLE'
    finally:
        report['public_reads'] = getattr(client,'calls',0)-baseline
        report['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    return report
