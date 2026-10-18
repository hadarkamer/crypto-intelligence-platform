"""Build-only, isolated Render check. Never starts the collector service.

Publish ONLY standalone_collector/hover-check-public. Source reports stay outside
that directory. Auto deploy must be off. Initial mode prepares without scanning;
an operator may explicitly select source mode after configuring the source session.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
PUBLIC = HERE / 'hover-check-public'
PRIVATE = HERE / 'hover-check-private'
OS_KEYS = ('PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL', 'TZ',
           'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE',
           'PLAYWRIGHT_BROWSERS_PATH')
AUTH_KEYS = ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON')


def child_environment(source=False, ai=False):
    # An allowlist prevents accidentally inheriting production or AI credentials.
    env = {key: os.environ[key] for key in OS_KEYS if key in os.environ}
    env.update(PYTHONUNBUFFERED='1', COLLECTION_BRIDGE_ENABLED='0',
               DATABASE_URL='', OPENAI_API_KEY='', COINGLASS_COLLECTOR_TOKEN='',
               MODEL1_NETWORK_DIAGNOSTICS='0')
    if source:
        env.update({key: os.environ[key] for key in AUTH_KEYS if key in os.environ})
    if ai:
        if not source or not os.environ.get('OPENAI_API_KEY', '').strip():
            raise RuntimeError('Authorized AI key missing; no request was started')
        env['OPENAI_API_KEY'] = os.environ['OPENAI_API_KEY']
        if os.environ.get('OPENAI_MARKET_SCANNER_MODEL'):
            env['OPENAI_MARKET_SCANNER_MODEL'] = os.environ['OPENAI_MARKET_SCANNER_MODEL']
    return env


def has_session(env):
    cookie = env.get('COINGLASS_COOKIE_HEADER', '').strip()
    if cookie and '=' in cookie:
        return True
    try:
        state = json.loads(env.get('COINGLASS_STORAGE_STATE_JSON', ''))
        return isinstance(state, dict) and any(
            isinstance(c, dict) and c.get('value') and
            c.get('domain', '').lstrip('.') in ('coinglass.com', 'www.coinglass.com')
            for c in state.get('cookies', []))
    except (ValueError, TypeError, AttributeError):
        return False


def publish_status(source_passed=False, diagnostic_only=False, models12=False, ai_check=False):
    if PUBLIC.is_symlink():
        raise RuntimeError('Public output must not be a symlink')
    if PUBLIC.exists():
        shutil.rmtree(PUBLIC)
    PUBLIC.mkdir()
    message = ('Source checks passed for models 1, 2 and 3 at 12H, 24H and 48H.'
               if source_passed else
               'Preparation passed. No source scans have been run by this build.')
    if diagnostic_only:
        message = ('Models 1/2 source check completed.' if models12 else 'Model 3 diagnostic completed.') + ' See private build logs; full acceptance remains pending.'
    if ai_check:
        message = 'Three bounded GPT checks completed. See private build logs; full acceptance remains pending.'
    isolation = ('No application database or background worker. At most three AI requests.'
                 if ai_check else 'No application database, AI calls or background worker.')
    (PUBLIC / 'index.html').write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="robots" content="noindex,nofollow">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Isolated hover check</title><main><h1>Isolated hover check</h1>'
        f'<p>{message}</p><p>{isolation}</p>'
        '<p>Build resources differ from the production collector. '
        'This does not establish production memory limits or full application readiness.</p>'
        '</main></html>', encoding='utf-8')


def main():
    mode = os.environ.get('HOVER_CHECK_MODE', 'prepare')
    if mode not in ('prepare', 'source', 'model3-diagnostic', 'models12', 'gpt-check'):
        raise RuntimeError('Invalid HOVER_CHECK_MODE')
    source_env = child_environment(source=True, ai=(mode == 'gpt-check'))
    if mode != 'prepare' and not has_session(source_env):
        raise RuntimeError('Source session missing; no source scan was started')
    if mode == 'source' and (PRIVATE.exists() or PRIVATE.is_symlink()):
        raise RuntimeError('Source output already exists; refusing a duplicate run')
    build_env = child_environment()
    subprocess.run([sys.executable, str(HERE / 'prepare.py')], cwd=HERE.parent,
                   env=build_env, check=True, timeout=900)
    subprocess.run([sys.executable, '-m', 'unittest', 'test_render_hover_check', '-q'],
                   cwd=HERE, env=build_env, check=True, timeout=60)
    if mode == 'gpt-check':
        subprocess.run([sys.executable, str(HERE / 'hover_ai_check.py'),
                        '--output', str(HERE / 'gpt-check-private')],
                       cwd=HERE.parent, env=source_env, check=True, timeout=960)
        # Even when successful, GPT artifacts stay private. Do not reuse the
        # source-only public success message, which asserts zero AI calls.
        publish_status(ai_check=True)
        print('ISOLATED_GPT_CHECK passed=true max_ai_calls=3 database_writes=0 worker_started=false', flush=True)
        return
    if mode in ('model3-diagnostic','models12'):
        command=[sys.executable, str(HERE / 'model3_hover_diagnostic.py'),
                        '--output', str(HERE / (mode+'-private'))]
        if mode=='models12':command.append('--models12')
        subprocess.run(command,cwd=HERE.parent,env=source_env,check=True,timeout=1200 if mode=='models12' else 600)
        if mode=='models12':publish_status(diagnostic_only=True,models12=True)
        else:publish_status(diagnostic_only=True)
        return
    if mode == 'source':
        # Existing runner is sequential, bounded per map and closes each browser.
        subprocess.run([sys.executable, str(HERE / 'hover_source_check.py'),
                        '--output', str(PRIVATE)], cwd=HERE.parent,
                       env=source_env, check=True, timeout=1800)
    publish_status(source_passed=(mode == 'source'))
    print(f'ISOLATED_HOVER_CHECK mode={mode} passed=true worker_started=false '
          'database_writes=0 ai_calls=0', flush=True)


if __name__ == '__main__':
    main()
