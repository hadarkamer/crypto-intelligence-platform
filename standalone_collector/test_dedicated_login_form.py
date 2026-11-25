import os
import re
import unittest
from unittest.mock import Mock, patch

import dedicated_login_form as form


class Clock:
    def __init__(self):
        self.now = 1.0

    def __call__(self):
        return self.now


class Locator:
    def __init__(self, page, count=0, field=None):
        self.page, self.matches, self.field = page, count, field

    def count(self):
        return self.matches

    def nth(self, index):
        assert 0 <= index < self.matches
        return self

    def is_visible(self):
        return bool(self.matches)

    def element_handle(self, *, timeout):
        assert timeout > 0
        return self

    def evaluate(self, operation, args):
        assert self.field == 'button' and operation is form._FORM_OPERATION
        op = args['op']
        self.page.ops.append(op)
        if self.page.fail_op == op:
            return 'untrusted'
        if op == 'submit':
            self.page.submissions += 1
            self.page.after_submit(self.page)
        return 'ok'


class Page:
    def __init__(self, clock):
        self.clock = clock
        self.url = form.LOGIN_URL
        self.status = 200
        self.email_count = 1
        self.challenge = ''
        self.error_text = ''
        self.ops = []
        self.fail_op = None
        self.submissions = 0
        self.listener = None
        self.after_submit = lambda page: setattr(page, 'url', 'https://www.coinglass.com/')

    def goto(self, url, **kwargs):
        assert url == form.LOGIN_URL
        return Mock(status=self.status, header_value=lambda name: '65')

    def locator(self, selector):
        if selector == form._EMAIL:
            return Locator(self, self.email_count, 'email')
        if selector == form._PASSWORD:
            return Locator(self, 1, 'password')
        return Locator(self, int(selector == self.challenge))

    def get_by_text(self, pattern):
        return Locator(self, int(bool(pattern.search(self.error_text))))

    def get_by_role(self, role, *, name):
        assert role == 'button' and name.fullmatch('Login') and not name.search('Continue with Google')
        return Locator(self, 1, 'button')

    def wait_for_timeout(self, ms):
        self.clock.now += ms / 1000

    def on(self, event, listener):
        assert event == 'response'
        self.listener = listener

    def remove_listener(self, event, listener):
        assert event == 'response' and listener == self.listener
        self.listener = None


class TestOfficialForm(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch.object(form, 'monotonic', self.clock))
        self.enterContext(patch.dict(os.environ, {'DEBUG': '', 'PWDEBUG': ''}))
        self.page = Page(self.clock)
        self.loader = Mock(return_value=('test@example.invalid', 'fixture-only'))
        self.reserve = Mock(return_value=True)

    def call(self, **kwargs):
        return form.submit_official_login(self.page, timeout_ms=350, credentials=self.loader,
                                          before_submit=self.reserve, **kwargs)

    def test_redirect_is_submission_only_one_durable_reservation(self):
        self.reserve.side_effect = lambda: self.assertEqual(self.page.ops, ['check', 'email', 'password', 'check'])
        self.assertEqual(self.call(), form.LoginFormResult('login_submitted', True))
        self.reserve.assert_called_once_with()
        self.loader.assert_called_once_with()
        self.assertEqual(self.page.submissions, 1)
        self.assertIsNone(self.page.listener)

    def test_refuses_other_origins_and_login_lookalikes_before_loader(self):
        for url in ('https://accounts.google.com/login', 'http://www.coinglass.com/login',
                    'https://www.coinglass.com@untrusted.invalid/login', 'https://www.coinglass.com/reset',
                    'https://extra.coinglass.com/login', 'https://www.coinglass.com/login?next=unsafe'):
            with self.subTest(url=url):
                self.page.url = url
                self.assertEqual(self.call().code, 'login_form_untrusted')
                self.loader.assert_not_called()
                self.reserve.assert_not_called()

    def test_diagnostics_fail_before_navigation(self):
        self.page.goto = Mock(side_effect=AssertionError)
        with patch.dict(os.environ, {'DEBUG': 'pw:protocol'}):
            self.assertEqual(self.call().code, 'login_diagnostics_enabled')
        self.page.goto.assert_not_called()
        self.loader.assert_not_called()

    def test_duplicate_fields_and_missing_form_are_not_network_failures(self):
        for count in (0, 2):
            with self.subTest(count=count):
                self.page.email_count = count
                self.assertEqual(self.call().code, 'login_form_unavailable')
        self.loader.assert_not_called()

    def test_multiple_visible_login_buttons_require_one_shared_form_owner(self):
        for ownership, expected in (
            ([False, True], 'login_submitted'),
            ([True, True], 'login_form_unavailable'),
            ([False, False], 'login_form_unavailable'),
        ):
            with self.subTest(ownership=ownership):
                self.page = Page(self.clock)
                self.loader.reset_mock()
                self.reserve.reset_mock()
                nodes = []
                for owns_fields in ownership:
                    button = Locator(self.page, 1, 'button')
                    evaluate = button.evaluate
                    def operation(script, args, owned=owns_fields, guarded=evaluate):
                        if 'op' in args:
                            return guarded(script, args)
                        self.assertEqual((args['email'].field, args['password'].field), ('email', 'password'))
                        return owned
                    button.evaluate = operation
                    nodes.append(button)
                self.page.get_by_role = Mock(return_value=Mock(count=lambda: len(nodes), nth=nodes.__getitem__))
                self.assertEqual(self.call().code, expected)
                self.assertEqual(self.page.submissions, int(expected == 'login_submitted'))
                self.assertEqual(self.loader.call_count, self.page.submissions)
                self.assertEqual(self.reserve.call_count, self.page.submissions)

    def test_guarded_inputs_and_submit(self):
        for op in ('check', 'email', 'password', 'submit'):
            with self.subTest(op=op):
                self.page.fail_op = op
                self.assertEqual(self.call().code, 'login_form_untrusted')
        self.assertEqual(self.page.submissions, 0)

    def test_missing_or_unreadable_credentials_never_leak(self):
        self.loader.side_effect = RuntimeError('sensitive-fixture-do-not-emit')
        self.assertEqual(self.call(), form.LoginFormResult('login_credentials_missing'))
        self.loader.side_effect = None
        self.loader.return_value = ('', '')
        self.assertEqual(self.call(), form.LoginFormResult('login_credentials_missing'))
        self.reserve.assert_not_called()

    def test_cannot_submit_without_durable_fence(self):
        for outcome in (False, RuntimeError('private-fence-info')):
            with self.subTest(outcome=type(outcome).__name__):
                self.reserve.side_effect = [outcome]
                self.assertEqual(self.call(), form.LoginFormResult('login_attempt_not_reserved'))
                self.assertEqual(self.page.submissions, 0)
                self.assertIsNone(self.page.listener)

    def test_preexisting_challenge_never_requests_credentials(self):
        for selector, code in ((form._VERIFY, 'login_verification_required'), (form._CAPTCHA, 'login_captcha_required')):
            with self.subTest(code=code):
                self.page.challenge = selector
                self.assertEqual(self.call(), form.LoginFormResult(code))
                self.assertEqual(form.visible_login_blocker(self.page), code)
                self.loader.assert_not_called()

    def test_explicit_signals_after_submit(self):
        for message, code in (('Invalid email or password', 'login_credentials_rejected'),
                              ('Enter the verification code', 'login_verification_required'),
                              ('Too many requests', 'login_rate_limited')):
            with self.subTest(code=code):
                self.page.error_text = ''
                self.page.after_submit = lambda page: setattr(page, 'error_text', message)
                self.assertEqual(self.call(), form.LoginFormResult(code, True))

    def test_honors_retry_after_on_login_page_or_credential_post(self):
        self.page.status = 429
        self.assertEqual(self.call(), form.LoginFormResult('login_rate_limited', False, 65))
        self.loader.assert_not_called()
        self.page.status = 200
        def emit_429(page):
            page.listener(Mock(status=429, request=Mock(method='POST'), url='https://capi.coinglass.com/api/user/login',
                               header_value=lambda name: '179'))
        self.page.after_submit = emit_429
        self.assertEqual(self.call(), form.LoginFormResult('login_rate_limited', True, 179))

    def test_pre_submission_transport_error_only(self):
        self.page.goto = Mock(side_effect=RuntimeError('net::ERR_CONNECTION_RESET'))
        self.assertEqual(self.call(), form.LoginFormResult('login_network_transient'))
        self.loader.assert_not_called()
        self.page.goto.side_effect = TimeoutError('waiting for an unknown page')
        self.assertEqual(self.call(), form.LoginFormResult('login_form_unavailable'))

    def test_ambiguous_response_after_submission_is_never_retried_here(self):
        self.page.after_submit = lambda page: None
        self.assertEqual(self.call(), form.LoginFormResult('login_outcome_unconfirmed', True))
        self.assertEqual(self.page.submissions, 1)
        self.page.after_submit = Mock(side_effect=RuntimeError('net::ERR_CONNECTION_RESET'))
        self.assertEqual(self.call(), form.LoginFormResult('login_outcome_unconfirmed', True))
        self.assertEqual(self.page.submissions, 2)

    def test_chart_capture_guard(self):
        self.assertEqual(form.visible_login_blocker(self.page), 'login_form_visible')
        self.page.url = 'https://www.coinglass.com/LiquidationHeatMap/BTC'
        self.assertIsNone(form.visible_login_blocker(self.page))
        self.page.challenge = form._CAPTCHA
        self.assertEqual(form.visible_login_blocker(self.page), 'login_captcha_required')
        self.page.locator = Mock(side_effect=RuntimeError('private DOM'))
        self.assertEqual(form.visible_login_blocker(self.page), 'login_form_unavailable')


if __name__ == '__main__':
    unittest.main()
