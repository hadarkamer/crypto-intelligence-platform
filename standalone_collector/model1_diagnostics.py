"""Passive public-source diagnostics: status counts only, no API bodies or credentials."""
from collections import Counter
import json
import os
from urllib.parse import urlsplit


def attach(page):
    if os.getenv('MODEL1_NETWORK_DIAGNOSTICS','').lower() != 'true':
        return
    statuses=Counter()
    failures=Counter()
    js_errors=Counter()
    def response_seen(response):
        if response.status >= 400:
            host=urlsplit(response.url).hostname or 'unknown'
            statuses[f'{host}:{response.status}']+=1
    def request_failed(request):
        host=urlsplit(request.url).hostname or 'unknown'
        error=str(request.failure or 'network-failure').split('\n')[0][:100]
        # Record host and error category only, never URL paths/queries/headers.
        failures[f'{host}:{error}']+=1
    def page_error(error):
        js_errors[type(error).__name__]+=1
    def closed(*args):
        print('MODEL1_NETWORK_DIAGNOSTIC '+json.dumps({
            'http_errors':dict(statuses.most_common(12)),
            'network_errors':dict(failures.most_common(12)),
            'page_error_types':dict(js_errors.most_common(4)),
        }),flush=True)
    page.on('response',response_seen)
    page.on('requestfailed',request_failed)
    page.on('pageerror',page_error)
    page.on('close',closed)
