"""Build a fixed, offline visual fixture from verified product sources into a new directory."""
import argparse
import hashlib
import json
from pathlib import Path

PINS = {
    'operator_desktop.py': '1bb1306e9f4eeca3d1aa47816837537dd120700f',
    'operator_desktop_child.py': '3fa2a1c730280f3ca02722ad56bfee52c3459d87',
    'operator_chromium_policy.json': '02a1e2e408206985987b012a23a22f3730de18a6',
}
PAGE = '/tmp/decision-hub-visual-fixture.html'
DOCUMENT = """<!doctype html><meta charset="utf-8"><title>Decision Hub QA</title>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'none'; form-action 'none'; base-uri 'none'">
<style>html,body,button{margin:0;width:100%;height:100%;border:0}button{background:#134d91;color:white;font:24px sans-serif}span{position:absolute;left:30px;top:30px}button.on{background:#197553}</style>
<button id="sample"><span>Decision Hub QA 1</span></button>
<script>const b=document.getElementById('sample');b.onclick=e=>{if(e.isTrusted){b.classList.add('on');b.firstElementChild.textContent='Decision Hub QA 2'}}</script>"""


def replace_once(source, before, after):
    if source.count(before) != 1:
        raise RuntimeError('unexpected_product_source')
    return source.replace(before, after)


def build(base, output):
    sources = {}
    for name, expected in PINS.items():
        raw = (base / name).read_bytes()
        identity = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
        if identity != expected:
            raise RuntimeError('unexpected_product_version: ' + name)
        sources[name] = raw.decode()
    # The only network interface in the private browser namespace remains loopback.
    # Its only outbound relay now refuses every destination before any DNS lookup.
    parent = sources['operator_desktop.py']
    start = parent.index('def permitted_host(host):')
    end = parent.index('\n\nasync def _copy(', start)
    parent = parent[:start] + 'def permitted_host(host):\n    return False\n' + parent[end:]
    # The native host uses a root-owned venv under the existing read-only /usr mount.
    # Only the fixed interpreter path changes. The namespace and sandbox flags stay intact.
    if parent.count("'/usr/local/bin/python'") != 3:
        raise RuntimeError('unknown_native_launch_paths')
    parent = parent.replace("'/usr/local/bin/python'", "'/usr/local/lib/coinglass-window-fixture/bin/python'")
    child = replace_once(sources['operator_desktop_child.py'], "    mark_probe('native_browser')",
        f"    Path({PAGE!r}).write_text({DOCUMENT!r}, encoding='utf-8')\n    mark_probe('native_browser')")
    child = replace_once(child, "'about:blank' if probe else 'https://www.coinglass.com/login'",
                         f"'about:blank' if probe else {'file://' + PAGE!r}")
    # A click on "verify" must never attempt to read a real source or produce a session.
    child = replace_once(child, "    mark_probe('reader_import')",
        "    if not probe:\n        return 1\n    mark_probe('reader_import')")
    policy = json.loads(sources['operator_chromium_policy.json'])
    if policy.get('URLBlocklist') != ['*']:
        raise RuntimeError('missing_deny_default_policy')
    policy['URLAllowlist'] = ['about:blank', 'file://' + PAGE]
    sources.update({'operator_desktop.py': parent, 'operator_desktop_child.py': child,
                    'operator_chromium_policy.json': json.dumps(policy, indent=2) + '\n'})
    for name, value in sources.items():
        if name.endswith('.py'):
            compile(value, name, 'exec')
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    for name, value in sources.items():
        (output / name).write_text(value, encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reviewed_collector_directory', type=Path)
    parser.add_argument('new_fixture_overlay', type=Path)
    args = parser.parse_args()
    build(args.reviewed_collector_directory, args.new_fixture_overlay)
