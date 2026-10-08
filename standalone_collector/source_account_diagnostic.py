"""Opt-in, read-only CoinGlass account response diagnostic.

One fixed-origin GET uses the existing session's obe value, as the public
frontend does. Only HTTP status and the outer success boolean are retained.
Encrypted data is left untouched; acceptance does not prove heatmap access.
No browser, model, database, credential files or raw response logging.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re
import time
from urllib.parse import unquote

USER_INFO_URL='https://capi.coinglass.com/coin-community/api/userapi/info'
USER_AGENT='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36'
MAX_HEADER_BYTES=64*1024
MAX_RESPONSE_BYTES=32*1024
MAX_STORAGE_STATE_BYTES=64*1024
COOKIE_DOMAINS=frozenset({'coinglass.com','.coinglass.com','www.coinglass.com'})
SOURCE_PAGE_PATH='/pro/futures/LiquidationHeatMap'


def _safe_session_value(value):
    if not isinstance(value,str):return None
    value=value.strip()
    if not value or len(value)>MAX_HEADER_BYTES:return None
    # Match cookie.parse(document.cookie)'s decodeURIComponent once. Malformed
    # escapes/UTF-8 preserve the raw value, and '+' is never changed to a space.
    if re.search(r'%(?![0-9a-fA-F]{2})',value) is None:
        try:value=unquote(value,encoding='utf-8',errors='strict')
        except (UnicodeError,ValueError):pass
    # Requests header values cannot contain decoded CR/LF or other controls.
    # Never decrypt, normalize or log the token or any account/profile data.
    if any(ord(char)<32 or ord(char)>126 for char in value):return None
    return value


def _session_value(environment):
    header=environment.get('COINGLASS_COOKIE_HEADER','')
    if not isinstance(header,str):return None
    if header.strip():
        if len(header)>MAX_HEADER_BYTES or any(ord(char)<32 or ord(char)>126 for char in header):return None
        if header.lstrip().lower().startswith('cookie:'):return None
        value=None
        for part in header.split(';'):
            name,separator,candidate=part.strip().partition('=')
            if separator and name.strip()=='obe':value=_safe_session_value(candidate)
        return value
    state=environment.get('COINGLASS_STORAGE_STATE_JSON','')
    if not isinstance(state,str) or not state.strip():return None
    try:
        if len(state.encode('utf-8'))>MAX_STORAGE_STATE_BYTES:return None
        payload=json.loads(state)
    except (ValueError,TypeError):return None
    if not isinstance(payload,dict) or not isinstance(payload.get('cookies'),list):return None
    value=None
    for cookie in payload['cookies']:
        if not isinstance(cookie,dict) or cookie.get('name')!='obe':continue
        domain=cookie.get('domain')
        if not isinstance(domain,str) or domain not in COOKIE_DOMAINS:continue
        path=cookie.get('path','/')
        if not isinstance(path,str) or not path.startswith('/'):continue
        # Match a cookie visible on the BTC heatmap page, rather than another
        # site's or unrelated path's credential. No localStorage is inspected.
        if path!='/' and not (SOURCE_PAGE_PATH==path or SOURCE_PAGE_PATH.startswith(path.rstrip('/')+'/')):continue
        value=_safe_session_value(cookie.get('value'))
    return value


def check_account_response(*,env=None,http_get=None):
    environment=os.environ if env is None else env
    report={'checked_at':datetime.now(timezone.utc).isoformat(),
        'source_account_requests':0,'source_attempts':0,'ai_calls':0,'sheet_writes':0,
        'http_status':None,'account_api_success':None,'code':'source_account_check_unavailable'}
    value=_session_value(environment)
    if value is None:return report
    headers={'Accept':'application/json','language':'en','encryption':'true',
        'cache-ts-v2':str(int(time.time()*1000)),'obe':value,
        'Origin':'https://www.coinglass.com','Referer':'https://www.coinglass.com/',
        'User-Agent':USER_AGENT}
    if sum(len(key)+len(val)+4 for key,val in headers.items())>MAX_HEADER_BYTES:return report
    try:
        if http_get is None:
            import requests
            http_get=requests.get
        report['source_account_requests']=1
        with http_get(USER_INFO_URL,headers=headers,allow_redirects=False,
                      stream=True,timeout=(4,8)) as response:
            status=response.status_code
            if type(status) is not int or not 100<=status<=599:return report
            report['http_status']=status
            if status!=200:
                report['code']=('source_account_request_denied' if status in (401,403)
                    else 'source_account_rate_limited' if status==429
                    else 'source_account_redirect_rejected' if 300<=status<=399
                    else 'source_account_check_unavailable')
                return report
            body=bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                if not isinstance(chunk,bytes) or len(body)+len(chunk)>MAX_RESPONSE_BYTES:return report
                body.extend(chunk)
            payload=json.loads(body)
            if not isinstance(payload,dict) or type(payload.get('success')) is not bool:return report
            success=payload['success']
            report['account_api_success']=success
            report['code']='source_account_api_accepted' if success else 'source_account_response_unsuccessful'
    except Exception:
        # No exception message, provider error, response header or profile field
        # can reach the sanitized report, even for transport or parsing failures.
        report['code']='source_account_check_unavailable'
        report['account_api_success']=None
    return report


def main():
    print('COINGLASS_ACCOUNT_DIAGNOSTIC '+json.dumps(check_account_response()),flush=True)


if __name__=='__main__':main()
