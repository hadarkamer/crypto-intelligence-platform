"""Use the official email form once. Only the protected map can establish success."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
import os
import re
from time import monotonic
from urllib.parse import urlsplit


HOME_URL = 'https://www.coinglass.com/'
PHASE_SECONDS = 15
_EMAIL = 'input[type=email], input[autocomplete=username], input[placeholder="Email" i]'
_PASSWORD = 'input[type=password]:not([autocomplete=new-password])'
_BUTTON = re.compile(r'^(?:login|log in|sign in)$', re.I)
_CAPTCHA = 'iframe[src*="recaptcha" i], iframe[src*="hcaptcha" i], iframe[src*="challenges.cloudflare.com" i], [data-sitekey]'
_VERIFY = 'input[autocomplete=one-time-code], input[name=otp i], input[placeholder*="verification code" i]'
_MESSAGES = (
    ('login_captcha_required', r'(?:complete (?:the|this) captcha|verify (?:that )?you are human|security verification required)'),
    ('login_verification_required', r'(?:enter (?:the |your )?(?:one.time|verification|authentication) code|two.factor authentication required|enter (?:the )?code (?:we |sent ))'),
    ('login_credentials_rejected', r'(?:incorrect (?:email|password|credentials)|invalid (?:email or password|username or password|credentials)|wrong (?:email|password)|password is incorrect|email or password is incorrect)'),
    ('login_rate_limited', r'(?:too many (?:requests|attempts|login attempts)|rate limit exceeded|try again in \d+ (?:seconds|minutes))'),
)

# A handle pins the operation to the original document. Recheck ownership and
# the effective form action inside the same JS turn as each write and click.
_FORM_OPERATION = r'''(button, {email, password, op, value, expectedUrl, binding}) => {
  const doc = button.ownerDocument;
  const loc = new URL(doc.URL);
  if (doc !== document || window !== window.top || loc.protocol !== 'https:' ||
      !['www.coinglass.com', 'coinglass.com'].includes(loc.hostname) || loc.href !== expectedUrl ||
      loc.port || loc.username || loc.password) return 'untrusted';
  const inputs = [email, password, button];
  if (!inputs.every(el => el.ownerDocument === doc && el.isConnected && el.getClientRects().length &&
       getComputedStyle(el).visibility === 'visible')) return 'unavailable';
  if (!(email instanceof HTMLInputElement) || !['text','email'].includes(email.type) || email.readOnly || email.disabled ||
      !(password instanceof HTMLInputElement) || password.type !== 'password' || password.readOnly || password.disabled ||
      !(button instanceof HTMLButtonElement || (button instanceof HTMLInputElement && ['submit','button'].includes(button.type)))) return 'unavailable';
  const labelId = button.getAttribute('aria-labelledby');
  const labelled = labelId && labelId.split(/\s+/).map(id => doc.getElementById(id)?.textContent || '').join(' ');
  const name = labelled || button.getAttribute('aria-label') || button.textContent || button.value || '';
  if (!/^(?:login|log in|sign in)$/i.test(name.trim()) ||
      (op === 'submit' && (button.disabled || button.getAttribute('aria-disabled') === 'true'))) return 'unavailable';
  const form = email.form;
  if (!form || form !== binding.form || form !== password.form || form !== button.form || button.type !== 'submit') return 'untrusted';
  const safeAction = raw => {
    try {
      const url = new URL(raw, doc.baseURI);
      return url.protocol === 'https:' && url.origin === loc.origin && !url.username && !url.password;
    } catch { return false; }
  };
  const action = button.hasAttribute('formaction') ? button.formAction : form.action;
  if (action !== binding.action || !safeAction(form.action) || !safeAction(action)) return 'untrusted';
  const target = button.getAttribute('formtarget') ?? form.getAttribute('target') ?? doc.querySelector('base[target]')?.target ?? '';
  if (target && target.toLowerCase() !== '_self') return 'untrusted';
  const method = button.hasAttribute('formmethod') ? button.formMethod : form.method;
  if (method.toLowerCase() !== 'post') return 'untrusted';
  if (op === 'check') return 'ok';
  if (op === 'submit') { button.click(); return 'ok'; }
  const input = op === 'email' ? email : password;
  Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, value);
  input.dispatchEvent(new Event('input', {bubbles:true}));
  input.dispatchEvent(new Event('change', {bubbles:true}));
  return 'ok';
}'''


@dataclass(frozen=True)
class LoginFormResult:
    code: str
    submitted: bool = False
    retry_after_seconds: int | None = None
    phase: str | None = field(default=None, compare=False)
    http_status: int | None = field(default=None, compare=False)


def official_site_url(url):
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ''
        return (parsed.scheme == 'https' and host in ('www.coinglass.com', 'coinglass.com')
                and parsed.username is None and parsed.password is None and parsed.port in (None, 443))
    except (ValueError, TypeError):
        return False


def official_login_url(url):
    return official_site_url(url) and urlsplit(url).path.rstrip('/').split('/')[-1].lower() in ('login', 'signin', 'sign-in')


def _remaining(end):
    return max(1, ceil((end - monotonic()) * 1000))


_SCOPED_FIELDS = r'''({emailSelector, passwordSelector}) => {
  const visible = el => el.isConnected && el.getClientRects().length && getComputedStyle(el).visibility === 'visible';
  const buttons = 'button, input[type=submit], input[type=button]';
  const namedSubmit = el => {
    const ids = (el.getAttribute('aria-labelledby') || '').trim().split(/\s+/);
    const label = ids.map(id => document.getElementById(id)?.textContent || '').join(' ').trim() ||
                  el.getAttribute('aria-label') || el.textContent || el.value || '';
    return /^(login|log in|sign in)$/i.test(label.trim());
  };
  const matches = [];
  for (const password of document.querySelectorAll(passwordSelector)) {
    if (!visible(password) || password.disabled || password.readOnly) continue;
    const form = password.form;
    if (!form) continue;
    const owned = Array.from(form.elements).filter(el => el.form === form && visible(el));
    const emails = owned.filter(el => el.matches(emailSelector) && !el.disabled && !el.readOnly);
    const passwords = owned.filter(el => el.matches(passwordSelector));
    const submits = owned.filter(el => el.matches(buttons) && el.type === 'submit' && namedSubmit(el));
    if (emails.length === 1 && passwords.length === 1 && submits.length === 1)
      matches.push({email: emails[0], password, button: submits[0]});
  }
  return matches.length === 1 ? matches[0] : {};
}'''


def _fields(page):
    selected = page.evaluate_handle(_SCOPED_FIELDS, {'emailSelector': _EMAIL, 'passwordSelector': _PASSWORD})
    try:
        fields = selected.get_properties()
        if fields:
            return [fields[key].as_element() for key in ('email', 'password', 'button')]
    finally:
        selected.dispose()


def _visible(locator):
    return any(locator.nth(i).is_visible() for i in range(min(locator.count(), 12)))


def _challenge(page):
    if _visible(page.locator(_CAPTCHA)):
        return 'login_captcha_required'
    if _visible(page.locator(_VERIFY)):
        return 'login_verification_required'
    for code, message in _MESSAGES:
        if _visible(page.get_by_text(re.compile(message, re.I))):
            return code
    return None


def visible_login_blocker(page):
    """Fail closed before allowing a chart capture. Never return page content."""
    try:
        signal = _challenge(page)
        if signal:
            return signal
        if _visible(page.locator('input[type=password], [role=dialog] input[type=email]')):
            return 'login_form_visible'
    except Exception:
        return 'login_form_unavailable'
    return None


def retry_after_seconds(value):
    if not value:
        return None
    try:
        if re.fullmatch(r'[0-9]{1,12}', value.strip()):
            return int(value.strip())
        date = parsedate_to_datetime(value)
        if date.tzinfo is not None:
            return max(0, ceil((date - datetime.now(timezone.utc)).total_seconds()))
    except (TypeError, OverflowError, ValueError):
        pass
    return None


def _transient(exc):
    # Never emit exception text. In particular a generic navigation timeout
    # does not distinguish an incomplete form from a transport failure.
    return any(tag in str(exc) for tag in ('net::ERR_CONNECTION_RESET', 'net::ERR_NETWORK_CHANGED', 'net::ERR_TIMED_OUT'))


def _debug_enabled():
    return bool(os.environ.get('PWDEBUG', '').strip(' 0')) or any(
        x in os.environ.get('DEBUG', '') for x in ('*', 'pw:', 'playwright:'))


def _before_submit_blocker(page):
    signal = _challenge(page)
    # An old error label on a public page is not this account's credential response.
    return None if signal == 'login_credentials_rejected' else signal


def _document_failure(response, phase, status=None):
    if response is not None:
        status = response.status
        previous = response.request.redirected_from
        while previous is not None:
            if not official_site_url(previous.url):
                return LoginFormResult('login_form_untrusted', phase=phase, http_status=status)
            previous = previous.redirected_from
    if status == 429:
        return LoginFormResult('login_rate_limited', retry_after_seconds=retry_after_seconds(
            response.header_value('retry-after') if response is not None else None), phase=phase, http_status=status)
    if status in (502, 503, 504):
        return LoginFormResult('login_network_transient', phase=phase, http_status=status)
    if status is not None and status >= 400:
        return LoginFormResult('login_form_unavailable', phase=phase, http_status=status)
    return None


def _entry(page, end):
    # Multiple navigation controls can legitimately lead to the same account UI.
    for role in ('link', 'button'):
        controls = page.get_by_role(role, name=_BUTTON)
        for index in range(min(controls.count(), 32)):
            node = controls.nth(index).element_handle(timeout=_remaining(end))
            if not node or not node.is_visible():
                continue
            destination = node.evaluate("""el => {
              const a = el.closest('a');
              if (!a) return el.form ? null : document.URL;
              const target = a.getAttribute('target') || document.querySelector('base[target]')?.target || '';
              return a.hasAttribute('download') || !['', '_self', '_blank'].includes(target.toLowerCase()) ? null : a.href;
            }""")
            if destination and official_site_url(destination):
                return node
    return None


def _pause(page, end):
    page.wait_for_timeout(min(125, _remaining(end)))


def _prepare(page, end, popups):
    """Follow one visible official entry and verify its rendered form without accessing credentials."""
    phase = 'home_document'
    documents = {}
    unbound = {}

    def record(response):
        request = response.request
        if not request.is_navigation_request():
            return
        try:
            frame = request.frame
        except Exception:
            # A popup's first main-frame response may precede attachment to a Page.
            unbound.setdefault(response.url, []).append(response)
            return
        if frame == frame.page.main_frame:
            documents[frame.page] = response

    def opened(popup):
        popups.append(popup)

    context = page.context
    page.on('popup', opened)
    context.on('response', record)
    try:
        home = page.goto(HOME_URL, wait_until='domcontentloaded', timeout=min(PHASE_SECONDS * 1000, _remaining(end)))
        if not official_site_url(page.url):
            return LoginFormResult('login_form_untrusted', phase=phase), None, None
        failure = _document_failure(home, phase)
        if failure:
            return failure, None, None
        if home is None:
            return LoginFormResult('login_form_unavailable', phase=phase), None, None
        documents.setdefault(page, home)

        phase = 'entry_control'
        phase_end = min(end, monotonic() + PHASE_SECONDS)
        while monotonic() < phase_end:
            if not official_site_url(page.url):
                return LoginFormResult('login_form_untrusted', phase=phase), None, None
            failure = _document_failure(documents.get(page), 'home_document')
            if failure:
                return failure, None, None
            signal = _before_submit_blocker(page)
            if signal:
                return LoginFormResult(signal, phase=phase), None, None
            control = _entry(page, phase_end)
            if control:
                before_click = len(popups)
                unbound.clear()
                control.click(timeout=_remaining(phase_end), no_wait_after=True)
                break
            _pause(page, phase_end)
        else:
            return LoginFormResult('login_form_unavailable', phase=phase), None, None

        phase = 'form'
        phase_end = min(end, monotonic() + PHASE_SECONDS)
        while monotonic() < phase_end:
            opened_by_click = popups[before_click:]
            if len(opened_by_click) > 1:
                return LoginFormResult('login_form_untrusted', phase=phase), None, None
            active = opened_by_click[0] if opened_by_click else page
            if active.url == 'about:blank':
                _pause(page, phase_end)
                continue
            if not official_site_url(active.url):
                return LoginFormResult('login_form_untrusted', phase=phase), None, None
            try:
                # Navigation Timing belongs to the currently rendered document, including in a
                # fresh popup whose first Playwright request may precede main-frame attachment.
                document_url, status = active.evaluate("""() => {
                  const nav = performance.getEntriesByType('navigation')[0];
                  return [nav?.name || location.href, nav?.responseStatus || null];
                }""")
                response = documents.get(active)
                if response is not None and response.url != document_url:
                    response = None
                if response is None and opened_by_click:
                    candidates = unbound.get(document_url, ())
                    if len(candidates) == 1 and status == candidates[0].status:
                        response = candidates[0]
                failure = _document_failure(response, 'login_document', status)
                if failure:
                    return failure, None, None
                signal = _before_submit_blocker(active)
                if signal:
                    return LoginFormResult(signal, phase=phase), None, None
                if response is not None and 200 <= response.status < 300:
                    found = _fields(active)
                    if found:
                        if opened_by_click and official_site_url(page.url) and _fields(page):
                            return LoginFormResult('login_form_untrusted', phase=phase), None, None
                        return None, active, found
            except Exception as exc:
                if 'Execution context was destroyed' not in str(exc):
                    raise
            _pause(page, phase_end)
        return LoginFormResult('login_form_unavailable', phase=phase), None, None
    except Exception as exc:
        return LoginFormResult('login_network_transient' if _transient(exc) else 'login_form_unavailable', phase=phase), None, None
    finally:
        page.remove_listener('popup', opened)
        context.remove_listener('response', record)


def inspect_official_login(page, *, timeout_ms=30000):
    """Use the collector's navigation and trust checks, stopping before the credential loader."""
    return _official_login(page, timeout_ms, None, None, inspect_only=True)


def submit_official_login(page, *, timeout_ms=30000, credentials=None, before_submit=None):
    """Caller owns account locking, the durable attempt fence and protected-map proof."""
    return _official_login(page, timeout_ms, credentials, before_submit, inspect_only=False)


def _official_login(page, timeout_ms, credentials, before_submit, *, inspect_only):
    end = monotonic() + max(0, timeout_ms) / 1000
    submitted = False
    rate = []
    watching = False
    popups = []

    def observe(response):
        # Ignore third-party resources, telemetry and unrelated HTTP failures.
        if not submitted:
            return
        try:
            url = urlsplit(response.url)
            auth_path = re.search(r'(?:^|/)(?:login|signin|sign-in)(?:/|$)', url.path, re.I)
            if (response.status == 429 and response.request.method == 'POST' and auth_path
                    and url.scheme == 'https' and url.hostname and not url.username and not url.password
                    and (url.hostname == 'coinglass.com' or url.hostname.endswith('.coinglass.com'))):
                rate.append(retry_after_seconds(response.header_value('retry-after')))
        except Exception:
            pass

    try:
        if _debug_enabled():
            return LoginFormResult('login_diagnostics_enabled')
        if monotonic() >= end:
            return LoginFormResult('login_form_unavailable')
        failure, active, fields = _prepare(page, end, popups)
        if failure:
            return failure
        page = active
        form_url = page.url
        email_field, password_field, button = fields
        binding = button.evaluate_handle('''button => ({form: button.form, action:
            button.hasAttribute('formaction') ? button.formAction : button.form?.action})''')

        def perform(op, value=None):
            return button.evaluate(_FORM_OPERATION, {'email': email_field, 'password': password_field,
                                                       'op': op, 'value': value, 'expectedUrl': form_url, 'binding': binding})

        def failure(status):
            return LoginFormResult('login_form_untrusted' if status == 'untrusted' else 'login_form_unavailable',
                                   submitted, phase='form')

        ready = perform('check')
        if ready != 'ok':
            return failure(ready)
        if inspect_only:
            return LoginFormResult('login_form_ready', phase='form')
        if monotonic() >= end:
            return LoginFormResult('login_form_unavailable', phase='form')
        if not callable(before_submit):
            return LoginFormResult('login_attempt_not_reserved')
        try:
            email, password = credentials()
        except Exception:
            return LoginFormResult('login_credentials_missing')
        if not all(isinstance(value, str) and value.strip() for value in (email, password)):
            return LoginFormResult('login_credentials_missing')
        for op, value in (('email', email), ('password', password)):
            if monotonic() >= end:
                return LoginFormResult('login_form_unavailable')
            status = perform(op, value)
            if status != 'ok':
                return failure(status)
        signal = _before_submit_blocker(page)
        if signal:
            return LoginFormResult(signal)
        ready = perform('check')
        if ready != 'ok' or monotonic() >= end:
            return failure(ready)
        page.on('response', observe)
        watching = True
        try:
            if before_submit() is False:
                return LoginFormResult('login_attempt_not_reserved')
        except Exception:
            return LoginFormResult('login_attempt_not_reserved')
        if monotonic() >= end:
            return LoginFormResult('login_form_unavailable')
        submitted = True  # The click can reach the server even if the page disappears mid-call.
        status = perform('submit')
        if status != 'ok':
            return failure(status)
        while monotonic() < end:
            if rate:
                return LoginFormResult('login_rate_limited', True, rate[-1])
            signal = _challenge(page)
            if signal:
                return LoginFormResult(signal, True)
            if page.url != form_url or not button.evaluate('button => button.isConnected'):
                return LoginFormResult('login_submitted', True)
            page.wait_for_timeout(min(100, _remaining(end)))
        return LoginFormResult('login_outcome_unconfirmed', True)
    except Exception as exc:
        if submitted:
            if rate:
                return LoginFormResult('login_rate_limited', True, rate[-1])
            return LoginFormResult('login_outcome_unconfirmed', True)
        return LoginFormResult('login_network_transient' if _transient(exc) else 'login_form_unavailable')
    finally:
        if watching:
            try:
                page.remove_listener('response', observe)
            except Exception:
                pass
        for popup in popups:
            try:
                popup.close()
            except Exception:
                pass
