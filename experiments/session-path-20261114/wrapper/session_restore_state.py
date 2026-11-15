"""Cookie-only validation reused from the existing recovery package. No transfer API."""

import json

import time

STATE_LIMIT = 65536

COOKIE_FIELDS = frozenset({'name', 'value', 'domain', 'path', 'expires',
                          'httpOnly', 'secure', 'sameSite', 'partitionKey'})

class Rejected(BaseException):
    def __init__(self, code): self.code = code

def cookie_only_state(cookies):
    """Keep original supported attributes; no document.cookie or storage export."""
    if not isinstance(cookies, list) or len(cookies) > 256:
        raise Rejected('state_invalid')
    selected, identities = [], set()
    for item in cookies:
        if not isinstance(item, dict): raise Rejected('state_invalid')
        domain = item.get('domain')
        if not isinstance(domain, str): raise Rejected('state_invalid')
        host = domain.lstrip('.').lower()
        import re
        if (domain.startswith('..') or len(host) > 253
                or re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', host) is None
                or any(not label or len(label) > 63 or label.startswith('-')
                       or label.endswith('-') for label in host.split('.'))):
            raise Rejected('state_invalid')
        if host != 'coinglass.com' and not host.endswith('.coinglass.com'):
            continue
        if set(item) - COOKIE_FIELDS: raise Rejected('unsupported_cookie_attribute')
        if not all(isinstance(item.get(k), str) for k in ('name', 'value', 'path')):
            raise Rejected('state_invalid')
        if not item['name'] or not item['path'].startswith('/'):
            raise Rejected('state_invalid')
        if any(type(item.get(k)) is not bool for k in ('httpOnly', 'secure')):
            raise Rejected('state_invalid')
        expiry = item.get('expires')
        import math
        if (type(expiry) not in (int, float) or not math.isfinite(expiry)
                or (expiry != -1 and expiry <= time.time())):
            raise Rejected('state_expired_or_invalid')
        if item.get('sameSite') not in ('Strict', 'Lax', 'None'):
            raise Rejected('state_invalid')
        if 'partitionKey' in item and not isinstance(item['partitionKey'], str):
            raise Rejected('state_invalid')
        identity = (item['name'], domain, item['path'], item.get('partitionKey'))
        if identity in identities: raise Rejected('duplicate_cookie_identity')
        identities.add(identity)
        selected.append(dict(item))
    if not selected: raise Rejected('coinglass_cookies_missing')
    value = json.dumps({'cookies': selected, 'origins': []}, ensure_ascii=True,
                       allow_nan=False, separators=(',', ':'))
    if len(value.encode('utf-8')) > STATE_LIMIT: raise Rejected('state_too_large')
    return value

def validate_storage_state(raw):
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > STATE_LIMIT:
        raise Rejected('state_too_large')
    try: state = json.loads(raw)
    except (ValueError, TypeError): raise Rejected('state_invalid') from None
    if (not isinstance(state, dict) or set(state) != {'cookies', 'origins'}
            or state['origins'] != []):
        raise Rejected('cookies_only_state_required')
    cleaned = cookie_only_state(state['cookies'])
    if json.loads(cleaned) != state: raise Rejected('unexpected_cookie_scope')
    return cleaned
