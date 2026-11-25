"""Offline official-form contract in sandboxed Chromium. All page traffic is fulfilled locally."""
import os
from pathlib import Path
import secrets
import unittest
from urllib.parse import parse_qs
from unittest.mock import Mock

from playwright.sync_api import sync_playwright

from dedicated_login_form import inspect_official_login, submit_official_login


HOME_URL = 'https://www.coinglass.com/'
ENTRY_URL = HOME_URL + 'en/account/access?source=header'
FINAL_URL = HOME_URL + 'account/sign-in?locale=en'
SUBMIT_URL = HOME_URL + '__offline_fixture__/login'
FOREIGN_URL = 'https://unused.invalid/login'
HEADER = '<!doctype html><meta charset="utf-8"><link rel="icon" href="data:,">'
NAV = f'<header><nav><a href="{ENTRY_URL}">Login</a></nav></header>'

SYSTEM_CHROME = Path('/opt/google/chrome/chrome')
FORM = '''<!doctype html><meta charset="utf-8"><link rel="icon" href="data:,"><title>Offline fixture</title>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'">
<nav><button type="button">Login</button></nav>
<aside><form><input type="email" placeholder="Newsletter"><button>Subscribe</button></form></aside>
<form id="account" method="post" action="/__offline_fixture__/login">
  <input type="email" name="email" placeholder="Email" required>
  <input type="password" name="password" placeholder="Password" required>
  <button type="submit" disabled>Login</button>
</form>
<script>
const form = document.getElementById('account');
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
    @classmethod
    def setUpClass(cls):
        if os.environ.get('PWDEBUG') or os.environ.get('DEBUG'):
            raise unittest.SkipTest('Offline credential fixture requires diagnostic tracing to be disabled')
        executable = browser_path()
        playwright = sync_playwright().start()
        cls.addClassCleanup(playwright.stop)
        cls.browser = playwright.chromium.launch(executable_path=executable, headless=True, chromium_sandbox=True)
        cls.addClassCleanup(cls.browser.close)

    def setUp(self):
        self.context = self.browser.new_context(offline=True, service_workers='block', locale='en-US')
        self.addCleanup(self.context.close)
        self.page = self.context.new_page()
        self.email = secrets.token_hex(12) + '@example.invalid'
        self.password = secrets.token_urlsafe(25)
        self.credentials = Mock(return_value=(self.email, self.password))
        self.fence = Mock(return_value=True)
        self.requests = []

    def serve(self, home=HEADER + NAV, form=FORM, *, entry_status=200, redirect=False):
        def local_only(route):
            request = route.request
            self.requests.append((request.method, request.url))
            if request.method == 'GET':
                if request.url == HOME_URL:
                    route.fulfill(status=200, content_type='text/html', body=home)
                    return
                if request.url == ENTRY_URL and redirect:
                    # A fresh official document sends the browser to its localized form.
                    route.fulfill(status=200, content_type='text/html',
                                  body=HEADER + f'<script>location.replace("{FINAL_URL}")</script>')
                    return
                if request.url in (ENTRY_URL, FINAL_URL, FOREIGN_URL):
                    route.fulfill(status=entry_status, content_type='text/html', body=form)
                    return
            if request.method == 'POST' and request.url == SUBMIT_URL:
                valid = parse_qs(request.post_data or '') == {'email': [self.email], 'password': [self.password]}
                route.fulfill(status=204 if valid else 400, body='')
                return
            route.abort('blockedbyclient')
        self.context.route('**/*', local_only)

    def submit(self, timeout=3500):
        return submit_official_login(self.page, timeout_ms=timeout, credentials=self.credentials, before_submit=self.fence)

    def test_passive_home_navigation_and_official_document(self):
        self.serve(home=HEADER + NAV + f'<footer><a href="{ENTRY_URL}">Login</a></footer>')
        result = inspect_official_login(self.page, timeout_ms=3500)
        self.assertEqual(result.code, 'login_form_ready')
        self.assertEqual(self.requests, [('GET', HOME_URL), ('GET', ENTRY_URL)])
        self.assertEqual(self.page.evaluate('window.fixtureEvents'), {'input': 0, 'change': 0, 'submit': 0})
        self.assertTrue(self.page.locator('input').evaluate_all("fields => fields.every(f => f.value === '')"))

    def test_official_form_in_sandboxed_chromium(self):
        self.serve(redirect=True)
        result = self.submit()
        self.assertEqual(result.code, 'login_submitted')
        self.assertTrue(result.submitted)
        self.assertEqual(self.page.url, HOME_URL)
        self.assertEqual(self.page.title(), 'Offline fixture home')
        self.assertEqual(self.page.evaluate('window.fixtureEvents'), {'input': 2, 'change': 2, 'submit': 1})
        self.assertEqual(self.page.locator('aside input').input_value(), '')
        self.credentials.assert_called_once_with()
        self.fence.assert_called_once_with()
        self.assertEqual(self.requests, [('GET', HOME_URL), ('GET', ENTRY_URL), ('GET', FINAL_URL), ('POST', SUBMIT_URL)])

    def test_same_document_login_control_opens_the_form(self):
        # The application renders the form in place. The HTML URL never changes.
        home = FORM.replace('<nav><button type="button">Login</button></nav>',
                            '<header><nav><button type="button" onclick="document.getElementById(\'account\').hidden=false">'
                            'Login</button></nav></header>').replace('<form id="account"', '<form hidden id="account"')
        self.serve(home)
        result = inspect_official_login(self.page, timeout_ms=3500)
        self.assertEqual(result.code, 'login_form_ready')
        self.assertEqual(self.page.url, HOME_URL)
        self.assertEqual(self.requests, [('GET', HOME_URL)])
        self.assertEqual(self.page.evaluate('window.fixtureEvents'), {'input': 0, 'change': 0, 'submit': 0})

    def test_official_popup_form_is_inspected_without_credentials(self):
        self.serve(home=HEADER + NAV.replace('<a ', '<a target="_blank" '))
        result = inspect_official_login(self.page, timeout_ms=3500)
        self.assertEqual(result.code, 'login_form_ready')
        self.assertEqual(self.requests, [('GET', HOME_URL), ('GET', ENTRY_URL)])

    def test_ambiguous_controls_are_refused_before_loading_credentials(self):
        form = FORM.replace('<button type="submit" disabled>', '<button type="submit">Login</button><button type="submit" disabled>')
        self.serve(form=form)
        self.assert_refused()

    def test_foreign_form_action_is_refused_before_loading_credentials(self):
        form = FORM.replace('type="submit" disabled', f'type="submit" formaction="{FOREIGN_URL}" disabled')
        self.serve(form=form)
        self.assert_refused()

    def test_javascript_only_button_in_form_is_refused(self):
        self.serve(form=FORM.replace('<button type="submit" disabled>', '<button type="button">'))
        self.assert_refused()

    def test_submit_without_form_owner_is_refused(self):
        form = FORM.replace('<form id="account" method="post"', '<section id="account" method="post"')
        self.serve(form=form.replace('</form>\n<script>', '</section>\n<script>'))
        self.assert_refused()

    def test_get_submit_override_is_refused(self):
        self.serve(form=FORM.replace('type="submit" disabled', 'type="submit" formmethod="get" disabled'))
        self.assert_refused()

    def test_empty_method_override_on_post_form_uses_get_and_is_refused(self):
        self.serve(form=FORM.replace('type="submit" disabled', 'type="submit" formmethod="" disabled'))
        self.assert_refused()

    def test_native_post_override_on_get_form_is_accepted_for_inspection(self):
        form = FORM.replace('method="post"', 'method="get"')
        self.serve(form=form.replace('type="submit" disabled', 'type="submit" formmethod="post" disabled'))
        result = inspect_official_login(self.page, timeout_ms=3500)
        self.assertEqual(result.code, 'login_form_ready')
        self.assertEqual(self.page.evaluate('window.fixtureEvents'), {'input': 0, 'change': 0, 'submit': 0})

    def test_foreign_browsing_context_on_form_is_refused(self):
        self.serve(form=FORM.replace('id="account" method="post"', 'id="account" target="_blank" method="post"'))
        self.assert_refused()

    def test_external_destination_is_refused_before_loading_credentials(self):
        self.serve(home=HEADER + NAV.replace(ENTRY_URL, FOREIGN_URL))
        self.assert_refused()

    def test_error_document_with_visible_fields_is_refused(self):
        self.serve(entry_status=404)
        result = self.assert_refused()
        self.assertEqual((result.phase, result.http_status), ('login_document', 404))
        self.assertIn(('GET', ENTRY_URL), self.requests)

    def test_empty_popup_error_document_reports_its_http_status(self):
        self.serve(home=HEADER + NAV.replace('<a ', '<a target="_blank" '), form=HEADER, entry_status=404)
        result = self.assert_refused()
        self.assertEqual((result.phase, result.http_status), ('login_document', 404))

    def assert_refused(self):
        result = self.submit(1600)
        self.assertFalse(result.submitted)
        self.assertNotIn(result.code, ('login_submitted', 'login_form_ready'))
        self.credentials.assert_not_called()
        self.fence.assert_not_called()
        self.assertTrue(all(method == 'GET' for method, _ in self.requests))
        return result


if __name__ == '__main__':
    unittest.main()
