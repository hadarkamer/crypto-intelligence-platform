"""Offline official-form contract in sandboxed Chromium. All page traffic is fulfilled locally."""
import os
from pathlib import Path
import secrets
import unittest
from urllib.parse import parse_qs
from unittest.mock import Mock

from playwright.sync_api import sync_playwright

from dedicated_login_form import LOGIN_URL, LoginFormResult, submit_official_login


HOME_URL = 'https://www.coinglass.com/'
SUBMIT_URL = HOME_URL + '__offline_fixture__/login'
SYSTEM_CHROME = Path('/opt/google/chrome/chrome')
FORM = '''<!doctype html><meta charset="utf-8"><link rel="icon" href="data:,"><title>Offline fixture</title>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'">
<form method="post" action="/__offline_fixture__/login">
  <input type="email" name="email" placeholder="Email" required>
  <input type="password" name="password" placeholder="Password" required>
  <button type="submit" disabled>Login</button>
</form>
<script>
const form = document.querySelector('form');
const button = form.querySelector('button');
const inputs = Array.from(form.querySelectorAll('input'));
const stats = window.fixtureEvents = { input: 0, change: 0, submit: 0 };
// Controlled-input value tracking mirrors the part of React's form contract
// relevant here: an instance setter updates the tracker, a native edit does not.
const native = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
const tracked = new WeakMap();
for (const field of inputs) {
  tracked.set(field, field.value);
  Object.defineProperty(field, 'value', {
    get() { return native.get.call(this); },
    set(value) { native.set.call(this, value); tracked.set(this, this.value); }
  });
}
form.addEventListener('input', event => {
  if (tracked.has(event.target) && tracked.get(event.target) !== event.target.value) {
    tracked.set(event.target, event.target.value);
    stats.input++;
  }
});
form.addEventListener('change', event => {
  if (tracked.has(event.target)) stats.change++;
  button.disabled = stats.input !== 2 || stats.change !== 2 || !form.checkValidity();
});
form.addEventListener('submit', async event => {
  event.preventDefault();
  if (++stats.submit !== 1) return;
  const result = await fetch(form.action, { method: form.method, body: new URLSearchParams(new FormData(form)) });
  if (result.ok) {
    form.remove();
    document.title = 'Offline fixture home';
    history.replaceState(null, '', '/');
  }
});
</script>'''


def browser_path():
    if os.environ.get('COINGLASS_QA_USE_SYSTEM_CHROME') == '1':
        candidates = [SYSTEM_CHROME]
    else:
        import playwright
        configured = os.environ.get('PLAYWRIGHT_BROWSERS_PATH')
        if configured == '0':
            cache = Path(playwright.__file__).resolve().parent / 'driver/package/.local-browsers'
        else:
            cache = Path(configured or Path(os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache')) / 'ms-playwright')
        candidates = [*sorted(cache.glob('chromium-*/chrome-linux*/chrome'), reverse=True),
                      *sorted(cache.glob('chromium_headless_shell-*/chrome-linux*/headless_shell'), reverse=True),
                      Path('/usr/lib/chromium/chromium'), SYSTEM_CHROME]
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)
    raise unittest.SkipTest('No installed Chromium binary for the sandboxed offline form fixture')


class DedicatedFormBrowser(unittest.TestCase):
    def test_official_form_in_sandboxed_chromium(self):
        if os.environ.get('PWDEBUG') or os.environ.get('DEBUG'):
            self.skipTest('Offline credential fixture requires diagnostic tracing to be disabled')
        executable = browser_path()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(executable_path=executable, headless=True,
                                                 chromium_sandbox=True)
            try:
                context = browser.new_context(offline=True, service_workers='block', locale='en-US')
                try:
                    self.exercise(context)
                finally:
                    context.close()
            finally:
                browser.close()

    def exercise(self, context):
        email = secrets.token_hex(12) + '@example.invalid'
        password = secrets.token_urlsafe(25)
        credentials = Mock(return_value=(email, password))
        stats = {'loads': 0, 'post': 0, 'valid': 0, 'other': 0}

        def local_only(route):
            request = route.request
            if request.url == LOGIN_URL and request.method == 'GET':
                stats['loads'] += 1
                # The first page has a foreign submit override. It must never get credentials.
                html = FORM
                if stats['loads'] == 1:
                    html = html.replace('type="submit" disabled',
                                        'type="submit" formaction="https://unused.invalid/login" disabled')
                route.fulfill(status=200, content_type='text/html; charset=utf-8', body=html)
                return
            if request.url == SUBMIT_URL and request.method == 'POST' and stats['post'] == 0:
                stats['post'] += 1
                matches = parse_qs(request.post_data or '') == {'email': [email], 'password': [password]}
                stats['valid'] += int(matches)
                route.fulfill(status=204 if matches else 400, body='')
                return
            # Never continue or forward an unexpected URL (including the foreign fixture).
            stats['other'] += 1
            route.abort('blockedbyclient')

        context.route('**/*', local_only)
        page = context.new_page()
        def reserve():
            return page.evaluate('window.fixtureEvents.input === 2 && window.fixtureEvents.change === 2 && window.fixtureEvents.submit === 0')
        fence = Mock(side_effect=reserve)
        refused = submit_official_login(page, credentials=credentials, before_submit=fence)
        self.assertEqual(refused, LoginFormResult('login_form_untrusted'))
        credentials.assert_not_called()
        fence.assert_not_called()
        accepted = submit_official_login(page, credentials=credentials, before_submit=fence)
        self.assertEqual(accepted, LoginFormResult('login_submitted', True))
        self.assertEqual(page.url, HOME_URL)
        self.assertEqual(page.title(), 'Offline fixture home')
        self.assertEqual(page.evaluate('window.fixtureEvents'), {'input': 2, 'change': 2, 'submit': 1})
        credentials.assert_called_once_with()
        fence.assert_called_once_with()
        self.assertEqual(stats, {'loads': 2, 'post': 1, 'valid': 1, 'other': 0})


if __name__ == '__main__':
    unittest.main()
