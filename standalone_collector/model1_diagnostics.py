"""Passive source diagnostics: host/status counts only, no response bodies or credentials."""
from collections import Counter
import atexit
import json
import os
from urllib.parse import urlsplit


def attach(page):
    if os.getenv('MODEL1_NETWORK_DIAGNOSTICS','').lower() != 'true':
        return
    statuses,failures,js_errors=Counter(),Counter(),Counter()
    reported=False
    def response_seen(response):
        if response.status >= 400:
            statuses[f'{urlsplit(response.url).hostname or "unknown"}:{response.status}']+=1
    def request_failed(request):
        host=urlsplit(request.url).hostname or 'unknown'
        failure=str(request.failure or 'network-failure')
        category=failure if failure.startswith('net::ERR_') and len(failure)<80 else 'network-failure'
        failures[f'{host}:{category}']+=1
    def page_error(error):
        js_errors[type(error).__name__]+=1
    def report(*args):
        nonlocal reported
        if reported: return
        reported=True
        print('MODEL1_NETWORK_DIAGNOSTIC '+json.dumps({
            'http_errors':dict(statuses.most_common(12)),
            'network_errors':dict(failures.most_common(12)),
            'page_error_types':dict(js_errors.most_common(4)),
        }),flush=True)
    page.on('response',response_seen)
    page.on('requestfailed',request_failed)
    page.on('pageerror',page_error)
    page.on('close',report)
    atexit.register(report)
