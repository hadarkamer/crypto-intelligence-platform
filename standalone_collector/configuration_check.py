"""Configuration-only deployment check. No source visit, inference, or DB writes.

The saved OpenAI key stays on the server. At most one fixed-origin GET retrieves
model metadata. A successful metadata request does not prove billing balance or
that a Responses API inference will succeed. No response/error body is logged.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Mapping
from urllib.parse import quote

MAX_METADATA_BYTES = 32 * 1024


def configuration_status(env: Mapping[str, str]) -> dict:
    present = lambda name: bool(env.get(name, '').strip())
    return {
        'openai_key_present': present('OPENAI_API_KEY'),
        'source_session_present': (present('COINGLASS_COOKIE_HEADER') or
                                   present('COINGLASS_STORAGE_STATE_JSON')),
        'bridge_token_configured': len(env.get('COINGLASS_COLLECTOR_TOKEN', '').strip()) >= 32,
        'storage_configured': present('DATABASE_URL'),
        'collection_enabled': env.get('COLLECTION_BRIDGE_ENABLED', '').lower() == 'true',
    }


def check_configuration(*, env=None, http_get=None) -> dict:
    environment = os.environ if env is None else env
    report = {
        'checked_at': datetime.now(timezone.utc).isoformat(),
        **configuration_status(environment),
        'openai_check': 'not_configured',
        'model_metadata_requests': 0,
        'inference_requests': 0,
        'source_attempts': 0,
        'sheet_writes': 0,
    }
    if not report['openai_key_present']:
        return report
    model = environment.get('OPENAI_MARKET_SCANNER_MODEL', 'gpt-5.6').strip()
    if re.fullmatch(r'(?:gpt-|o[1-9])[A-Za-z0-9._:-]{1,90}', model) is None:
        report['openai_check'] = 'invalid_model_setting'
        return report
    if http_get is None:
        import requests
        http_get = requests.get
    report['model_metadata_requests'] = 1
    try:
        with http_get(
            'https://api.openai.com/v1/models/' + quote(model, safe=''),
            headers={'Authorization': 'Bearer ' + environment['OPENAI_API_KEY'].strip()},
            allow_redirects=False, stream=True, timeout=(4, 8),
        ) as response:
            status = response.status_code
            report['http_status'] = status
            if status != 200:
                report['openai_check'] = {
                    401: 'authentication_rejected',
                    403: 'metadata_permission_denied',
                    404: 'model_not_accessible',
                    429: 'rate_limited',
                }.get(status, 'metadata_request_failed')
                return report
            body = bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                body.extend(chunk)
                if len(body) > MAX_METADATA_BYTES:
                    report['openai_check'] = 'invalid_metadata_response'
                    return report
            payload = json.loads(body)
            if not (isinstance(payload, dict) and payload.get('object') == 'model'
                    and isinstance(payload.get('id'), str) and payload.get('id')):
                report['openai_check'] = 'invalid_metadata_response'
                return report
            report['openai_check'] = 'model_metadata_access_confirmed'
            report['model_id_matches_requested'] = payload['id'] == model
    except Exception:
        # Never stringify errors, headers, secrets, or provider response bodies.
        report['openai_check'] = 'metadata_request_failed'
    return report


def main() -> None:
    print('MODEL1_CONFIG_CHECK ' + json.dumps(check_configuration()), flush=True)


if __name__ == '__main__':
    main()
