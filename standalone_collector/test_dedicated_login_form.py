import os
import unittest
from unittest.mock import Mock, PropertyMock, patch

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

    def as_element(self):
        return self

    def element_handle(self, *, timeout):
        assert timeout > 0
        return self

    def evaluate_handle(self, script):
        assert self.field == 'button' and 'button.form' in script
        return self

    def click(self, *, timeout, no_wait_after):
        assert self.field == 'entry' and timeout > 0 and no_wait_after
        self.page.entries_clicked += 1
        self.page.url = self.page.form_destination
        self.page.emit_document(self.page.login_status)

    def evaluate(self, operation, args=None):
        if self.field == 'entry':
            return self.page.entry_destination
        assert self.field == 'button'
        if args is None:
            assert 'isConnected' in operation
            return True
        assert operation is form._FORM_OPERATION and args['binding'] is self
        op = args['op']
        self.page.ops.append(op)
        if self.page.fail_op == op:
            return 'untrusted'
        if op == 'submit':
            self.page.submissions += 1
            self.page.after_submit(self.page)
        return 'ok'


class Events:
    def __init__(self):
        self.handlers = {}

    def on(self, event, listener):
        self.handlers[event] = listener

    def remove_listener(self, event, listener):
        assert self.handlers.pop(event) == listener


class Page(Events):
    def __init__(self, clock):
        super().__init__()
        self.context = Events()
        self.main_frame = Mock(page=self)
        self.clock = clock
        self.url = 'about:blank'
        self.home_destination = form.HOME_URL
        self.form_destination = self.entry_destination = 'https://www.coinglass.com/en/account/access?from=home'
        self.status = self.login_status = 200
        self.entries_clicked = 0
        self.entry_count = self.email_count = 1
        self.challenge = ''
        self.error_text = ''
        self.ops = []
        self.fail_op = None
        self.submissions = 0
        self.after_submit = lambda page: setattr(page, 'url', form.HOME_URL)

    @property
    def listener(self):
        return self.handlers.get('response')

    def emit_document(self, status):
        request = Mock(url=self.url, redirected_from=None, is_navigation_request=lambda: True, frame=self.main_frame)
        response = Mock(url=self.url, status=status, request=request,
                        header_value=lambda name: '65')
        listener = self.context.handlers.get('response')
        if listener:
            listener(response)
        return response

    def goto(self, url, **kwargs):
        assert url == form.HOME_URL
        self.url = self.home_destination
        return self.emit_document(self.status)

    def locator(self, selector):
        if selector == 'input[type=password], [role=dialog] input[type=email]':
            return Locator(self, int(self.url == self.form_destination))
        return Locator(self, int(selector == self.challenge))

    def get_by_text(self, pattern):
        return Locator(self, int(bool(pattern.search(self.error_text))))

    def get_by_role(self, role, *, name):
        assert role in ('link', 'button') and name.fullmatch('Login') and not name.search('Continue with Google')
        return Locator(self, self.entry_count if role == 'link' else 0, 'entry')

    def evaluate_handle(self, script, selectors):
        assert script == form._SCOPED_FIELDS
        assert selectors == {'emailSelector': form._EMAIL, 'passwordSelector': form._PASSWORD}
        fields = {}
        if self.url == self.form_destination and self.email_count == 1:
            fields = {name: Locator(self, 1, name) for name in ('email', 'password', 'button')}
        return Mock(get_properties=lambda: fields)

    def evaluate(self, script):
        assert 'responseStatus' in script
        return [self.url, self.login_status]

    def wait_for_timeout(self, ms):
        self.clock.now += ms / 1000


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

    def test_foreign_and_non_https_entry_redirects_stop_before_loader(self):
        for url in ('https://accounts.google.com/login', 'http://www.coinglass.com/login',
                    'https://www.coinglass.com@untrusted.invalid/login', 'https://www.coinglass.com.invalid/account',
                    'https://other.coinglass.com/account'):
            with self.subTest(url=url):
                self.page.form_destination = url
                self.assertEqual(self.call().code, 'login_form_untrusted')
                self.loader.assert_not_called()
                self.reserve.assert_not_called()

    def test_diagnostics_fail_before_navigation(self):
        self.page.goto = Mock(side_effect=AssertionError)
        with patch.dict(os.environ, {'DEBUG': 'pw:protocol'}):
            self.assertEqual(self.call().code, 'login_diagnostics_enabled')
        self.page.goto.assert_not_called()
        self.loader.assert_not_called()

    def test_home_and_login_document_failures_identify_actual_status_and_phase(self):
        for attribute, phase in [('status', 'home_document'), ('login_status', 'login_document')]:
            for status, expected in ((401, 'login_form_unavailable'), (403, 'login_form_unavailable'),
                                     (404, 'login_form_unavailable'), (429, 'login_rate_limited'),
                                     (503, 'login_network_transient')):
                with self.subTest(status=status, phase=phase):
                    self.page = Page(self.clock)
                    setattr(self.page, attribute, status)
                    result = self.call()
                    self.assertEqual((result.code, result.phase, result.http_status), (expected, phase, status))
                    self.assertEqual(self.page.entries_clicked, int(phase == 'login_document'))
                    self.loader.assert_not_called()
                    self.reserve.assert_not_called()

    def test_missing_entry_or_scoped_form_reports_ui_timeout_without_http_failure(self):
        for field, count, expected_phase in (('entry_count', 0, 'entry_control'), ('email_count', 0, 'form'),
                                               ('email_count', 2, 'form')):
            with self.subTest(field=field, count=count):
                self.page = Page(self.clock)
                setattr(self.page, field, count)
                result = self.call()
                self.assertEqual((result.code, result.phase, result.http_status), ('login_form_unavailable', expected_phase, None))
                self.loader.assert_not_called()
                self.reserve.assert_not_called()

    def test_passive_entry_uses_one_official_control_and_guard_without_mutation(self):
        self.page.entry_count = 2
        result = form.inspect_official_login(self.page, timeout_ms=350)
        self.assertEqual(result.code, 'login_form_ready')
        self.assertEqual(self.page.entries_clicked, 1)
        self.assertEqual(self.page.ops, ['check'])
        self.assertEqual(self.page.submissions, 0)
        self.assertEqual(self.page.context.handlers, {})
        self.assertEqual(self.page.handlers, {})

    def test_popup_form_requires_one_matching_document_and_an_official_redirect_chain(self):
        url = self.page.form_destination
        for documents, previous, expected in (
            ([], None, 'login_form_unavailable'),
            ([(url + '&other=1', 200)], None, 'login_form_unavailable'),
            ([(url, 201)], None, 'login_form_unavailable'),
            ([(url, 200), (url, 200)], None, 'login_form_unavailable'),
            ([(url, 200)], 'https://other.invalid/sign-in', 'login_form_untrusted'),
            ([(url, 200)], form.HOME_URL, 'login_form_ready'),
        ):
            with self.subTest(documents=documents, previous=previous):
                popup = Page(self.clock)
                popup.url = url
                popup.close = Mock()
                def open_popup(control, *, timeout, no_wait_after):
                    for request_url, status in documents:
                        redirect = Mock(url=previous, redirected_from=None) if previous else None
                        request = Mock(url=request_url, redirected_from=redirect, is_navigation_request=lambda: True)
                        type(request).frame = PropertyMock(side_effect=RuntimeError('frame not attached yet'))
                        self.page.context.handlers['response'](Mock(url=request_url, status=status, request=request))
                    self.page.handlers['popup'](popup)
                with patch.object(Locator, 'click', open_popup):
                    if expected == 'login_form_ready':
                        result = form.inspect_official_login(self.page, timeout_ms=350)
                    else:
                        result = self.call()
                self.assertEqual(result.code, expected)
                popup.close.assert_called_once_with()
                self.loader.assert_not_called()
                self.reserve.assert_not_called()

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
        self.page.url = self.page.form_destination
        self.assertEqual(form.visible_login_blocker(self.page), 'login_form_visible')
        self.page.url = 'https://www.coinglass.com/LiquidationHeatMap/BTC'
        self.assertIsNone(form.visible_login_blocker(self.page))
        self.page.challenge = form._CAPTCHA
        self.assertEqual(form.visible_login_blocker(self.page), 'login_captcha_required')
        self.page.locator = Mock(side_effect=RuntimeError('private DOM'))
        self.assertEqual(form.visible_login_blocker(self.page), 'login_form_unavailable')


if __name__ == '__main__':
    unittest.main()
