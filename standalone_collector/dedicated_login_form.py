"""Use the official email form once. Only the protected map can establish success."""

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
import os
import re
from time import monotonic
from urllib.parse import urlsplit


LOGIN_URL = 'https://www.coinglass.com/login'
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
_FORM_OPERATION = r'''(button, {email, password, op, value}) => {
  const doc = button.ownerDocument;
  const loc = new URL(doc.URL);
  if (doc !== document || window !== window.top || loc.origin !== 'https://www.coinglass.com' ||
      !/^\/login\/?$/.test(loc.pathname) || loc.username || loc.password || loc.search || loc.hash) return 'untrusted';
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
  if (form !== password.form || form !== button.form) return 'untrusted';
  if (!form) {
    let section = button.parentElement;
    while (section && !(section.contains(email) && section.contains(password))) section = section.parentElement;
    if (!section || section === doc.body || section === doc.documentElement) return 'untrusted';
  }
  const safeAction = raw => {
    if (!raw) return true;
    try {
      const url = new URL(raw, doc.baseURI);
      return url.protocol === 'https:' && url.origin === loc.origin && !url.username && !url.password;
    } catch { return false; }
  };
  if (!safeAction(button.getAttribute('formaction')) || (form && !safeAction(form.action))) return 'untrusted';
  const target = button.getAttribute('formtarget') ?? form?.getAttribute('target') ?? doc.querySelector('base[target]')?.target ?? '';
  if (target && target.toLowerCase() !== '_self') return 'untrusted';
  if (form && button.type === 'submit' && (button.getAttribute('formmethod') || form.method).toLowerCase() !== 'post') return 'untrusted';
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


def official_login_url(url):
    try:
        parsed = urlsplit(url)
        return (parsed.scheme == 'https' and parsed.netloc.lower() == 'www.coinglass.com'
                and parsed.path in ('/login', '/login/') and not parsed.query and not parsed.fragment)
    except (ValueError, TypeError):
        return False


def _remaining(end):
    return max(1, ceil((end - monotonic()) * 1000))


def _one_visible(locator):
    count = locator.count()
    if count > 12:
        return None
    visible = [locator.nth(i) for i in range(count) if locator.nth(i).is_visible()]
    return visible[0] if len(visible) == 1 else None


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
        if official_login_url(page.url) or _visible(page.locator('input[type=password], [role=dialog] input[type=email]')):
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


def _find_fields(page, end):
    while monotonic() < end:
        if not official_login_url(page.url):
            return 'login_form_untrusted', None
        signal = _challenge(page)
        if signal:
            return signal, None
        nodes = [_one_visible(locator) for locator in (page.locator(_EMAIL), page.locator(_PASSWORD),
                                                        page.get_by_role('button', name=_BUTTON))]
        if all(nodes):
            handles = [node.element_handle(timeout=_remaining(end)) for node in nodes]
            if all(handles):
                return None, handles
        page.wait_for_timeout(min(100, _remaining(end)))
    return 'login_form_unavailable', None


def submit_official_login(page, *, timeout_ms=12000, credentials=None, before_submit=None):
    """One credential-bearing action. Caller owns locking, retry and auth proof.

    Use a fresh, private, non-recording page/context without debug listeners.
    ``credentials`` is a private server-side loader returning (email, password).
    ``before_submit`` must reserve the attempt or raise/return False.
    """
    end = monotonic() + max(0, timeout_ms) / 1000
    submitted = False
    rate = []
    watching = False

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
        response = page.goto(LOGIN_URL, wait_until='domcontentloaded', timeout=_remaining(end))
        if not official_login_url(page.url):
            return LoginFormResult('login_form_untrusted')
        if response is not None:
            if response.status == 429:
                return LoginFormResult('login_rate_limited', False, retry_after_seconds(response.header_value('retry-after')))
            if response.status in (502, 503, 504):
                return LoginFormResult('login_network_transient')
            if response.status >= 400:
                return LoginFormResult('login_form_unavailable')
        signal, fields = _find_fields(page, end)
        if signal:
            return LoginFormResult(signal)
        email_field, password_field, button = fields

        def perform(op, value=None):
            return button.evaluate(_FORM_OPERATION, {'email': email_field, 'password': password_field,
                                                       'op': op, 'value': value})

        def failure(status):
            return LoginFormResult('login_form_untrusted' if status == 'untrusted' else 'login_form_unavailable', submitted)

        ready = perform('check')
        if ready != 'ok':
            return failure(ready)
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
        signal = _challenge(page)
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
            if not official_login_url(page.url):
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
