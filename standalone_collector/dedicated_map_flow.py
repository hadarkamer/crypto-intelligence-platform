"""Coordinate official-form access separately from map measurement and analysis."""
from dataclasses import dataclass
from time import monotonic

from dedicated_login_form import official_login_url, submit_official_login, visible_login_blocker
from model1_execution import capture_operation, StageFailure
from modern_map_reader import RANGES, choice, gate, map_access_budget, map_url, open_verified_map, option_control, read_selected_map, symbol_control

FORM_REASONS = {
    'login_network_transient': 'auth_network_transient',
    'login_credentials_missing': 'auth_configuration_invalid',
    'login_credentials_rejected': 'auth_invalid_credentials',
    'login_captcha_required': 'auth_challenge',
    'login_verification_required': 'auth_code_required',
    'login_rate_limited': 'auth_rate_limited',
    'login_form_unavailable': 'auth_form_unavailable',
    'login_form_untrusted': 'auth_form_unavailable',
    'login_diagnostics_enabled': 'auth_configuration_invalid',
}
HARD_BLOCKERS = frozenset({'auth_challenge', 'auth_code_required', 'auth_invalid_credentials', 'auth_rate_limited'})
LOGIN_BUDGET_SECONDS = 30


@dataclass(frozen=True)
class MapTarget:
    symbol: str
    model: int
    timeframe: str


PROTECTED_PROBE = MapTarget('BTC', 1, '12H')


def guard_surface(page, target):
    """Never treat a redirect, missing control, or generic HTTP failure as a sign-in request."""
    login_page = official_login_url(page.url)
    if not login_page and page.url != map_url(target.symbol):
        raise StageFailure('source_navigation_changed')
    blocker = visible_login_blocker(page)
    if login_page and blocker == 'login_form_visible':
        raise StageFailure('source_login_required')
    if not login_page and blocker == 'login_rate_limited':
        raise StageFailure('source_rate_limited')
    if blocker and blocker != 'login_form_visible':
        # An inability to inspect the document does not justify invoking a form.
        raise StageFailure(FORM_REASONS.get(blocker, 'source_not_readable')
                           if blocker != 'login_form_unavailable' else 'source_not_readable')
    if login_page:
        raise StageFailure('source_not_readable')
    if not blocker:
        try:
            gate(page)
            return
        except StageFailure as error:
            if error.code != 'source_login_required':
                raise
    # A retained gate on a different default model or range is not evidence
    # that the requested map needs the account. Confirm the live controls.
    try:
        matches = (choice(page,f'Model {target.model}') and choice(page,'Coin')
                   and symbol_control(page,target.symbol)
                   and option_control(page,'Range').inner_text().strip() == RANGES[target.timeframe])
    except Exception:
        matches = False
    raise StageFailure('source_login_required' if matches else 'source_not_readable')


def _checked_action(action, page, target, *args):
    def guard():
        guard_surface(page, target)

    try:
        return action(page, target.symbol, target.model, target.timeframe, *args, guard=guard)
    except StageFailure as error:
        if error.code in ('source_login_required', 'source_not_readable'):
            guard()
            if error.code == 'source_login_required':
                raise StageFailure('source_not_readable') from None
        raise
    except Exception as error:
        # Only an explicit gate may trigger the form. An unreachable site can
        # leave the page at about:blank even when the requested URL is right.
        if page.url not in ('about:blank', 'chrome-error://chromewebdata/'):
            guard()
        from playwright.sync_api import Error, TimeoutError
        if isinstance(error, TimeoutError):
            raise StageFailure('source_timeout') from None
        if isinstance(error, Error):
            raise StageFailure('source_capture_failed') from None
        raise


def _map_action(session, action, page, target, *args):
    try:
        return _checked_action(action, page, target, *args)
    except StageFailure as error:
        if error.code == 'source_rate_limited':
            session.source_cooldown(limited=True, retry_after_seconds=getattr(error, 'retry_after_seconds', None))
        raise


def _start(session):
    ticket = session.auth_start()
    if ticket['code'] != 'allowed':
        raise StageFailure(ticket['code'])
    return ticket['incident_id']


def _settle(session, incident, code, retry_after=None):
    decision = session.auth_result(incident, code, retry_after)
    return decision['code']


def _record_block(session, code, error=None):
    if code in HARD_BLOCKERS:
        decision = session.auth_start(observed_code=code,
                                      retry_after_seconds=getattr(error, 'retry_after_seconds', None))
        raise StageFailure(decision['code'])
    incident = _start(session)
    raise StageFailure(_settle(session, incident, code))


def _authenticate(session, context, page):
    deadline = monotonic() + LOGIN_BUDGET_SECONDS
    for attempt in range(2):
        incident = _start(session)
        remaining = int((deadline - monotonic()) * 1000)
        if remaining < 1000:
            raise StageFailure(_settle(session, incident, 'auth_network_transient'))
        outcome = submit_official_login(
            page, timeout_ms=min(12000, remaining),
            credentials=lambda: session.credentials(incident),
            before_submit=lambda: session.auth_submit(incident),
        )
        if outcome.code in ('login_submitted', 'login_outcome_unconfirmed') or (
                outcome.submitted and outcome.code == 'login_network_transient'):
            # Even if the submit response was lost, observe the map before
            # deciding anything. A public 24H chart cannot verify an account.
            try:
                _map_action(session, open_verified_map, page, PROTECTED_PROBE)
            except Exception as error:
                code = getattr(error, 'code', None)
                reason = code if code in HARD_BLOCKERS else 'auth_unconfirmed'
                denied = _settle(session, incident, reason, getattr(error, 'retry_after_seconds', None))
                if code in ('source_timeout', 'source_capture_failed', 'source_map_unavailable',
                            'source_navigation_changed', 'source_not_readable', 'source_rate_limited'):
                    raise
                raise StageFailure(denied) from None
            # Save a proven account while the protected probe is still the
            # current page. A missing requested combination cannot poison it.
            session.checkpoint(context, incident_id=incident)
            return
        reason = FORM_REASONS.get(outcome.code, 'auth_unconfirmed')
        decision = _settle(session, incident, reason, outcome.retry_after_seconds)
        if decision != 'auth_retry_pending':
            raise StageFailure(decision)
        if attempt or monotonic() >= deadline:
            raise StageFailure('auth_retry_pending')


def _refresh_rotated_state(session, context):
    # A public or previously unverified combination can publish on its own.
    # Only a real change to the scoped state warrants a second page. Observe
    # the canonical protected target there without disturbing the selected map.
    from contextlib import ExitStack
    try:
        with capture_operation('source_session_setup'):
            if not session.state_changed(context):
                return
            with map_access_budget(12), ExitStack() as resources:
                proof_page = context.new_page()
                resources.callback(proof_page.close)
                resources.enter_context(session.observe_proof(proof_page))
                _map_action(session, open_verified_map, proof_page, PROTECTED_PROBE)
                session.checkpoint(context, proof_page=proof_page)
    except Exception:
        # The captured operation records failure; the existing encrypted state
        # stays intact. An auxiliary gate never requests a password. A 429
        # persists its source cooldown through the ordinary map observer.
        return


def collect_with_dedicated_session(session, context, page, target, output, *, allow_login=True):
    """At most one renewal per work item. The parent durably fences all jobs."""
    cooldown = session.source_cooldown()
    if cooldown['code'] != 'allowed':
        raise StageFailure(cooldown['code'])
    restored = bool(session.state.get('cookies') or session.state.get('origins'))
    if restored:
        try:
            _map_action(session, open_verified_map, page, target)
            if target == PROTECTED_PROBE:
                session.checkpoint(context)
            else:
                _refresh_rotated_state(session, context)
            return _map_action(session, read_selected_map, page, target, output)
        except StageFailure as error:
            if error.code in HARD_BLOCKERS and allow_login:
                _record_block(session, error.code, error)
            if error.code != 'source_login_required' or not allow_login:
                raise
    elif not allow_login:
        raise StageFailure('source_login_required')
    _authenticate(session, context, page)
    try:
        if target != PROTECTED_PROBE:
            _map_action(session, open_verified_map, page, target)
        return _map_action(session, read_selected_map, page, target, output)
    except StageFailure as error:
        if error.code == 'source_login_required':
            _record_block(session, 'auth_gate_after_verification')
        if error.code in HARD_BLOCKERS:
            _record_block(session, error.code, error)
        raise
