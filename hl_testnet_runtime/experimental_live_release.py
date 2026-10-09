"""Pure, default-off release configuration for the experimental Testnet worker.

Reading these settings never reads a key, constructs a client, or starts work.
No code in this module grants approval or modifies an environment variable.
Bounded or continuous policy controls NEW entries only. Entry halt or bounded
expiry must not disable protection, and source deadlines remain independent.
"""
from copy import deepcopy
import re

import experimental_execution_contract as contract
from . import two_account_execution as roles
from .experimental_execution_dispatch import BoundaryError

MODE = 'experimental_testnet_v1'
DISPATCH = 'approved_testnet_v1'
HANDOVER = 'retain_legacy_until_flat_v1'
BOUNDED_ENTRY = 'bounded_v1'
CONTINUOUS_ENTRY = 'continuous_v1'
_HEX = re.compile(r'[0-9a-f]{64}\Z')


def configured(env):
    return env.get('HL_TESTNET_RUNTIME_MODE') == MODE


def configuration(env):
    """Validate explicit local settings; None means no new worker is selected.

    Legacy entries cannot be started in this mode. This is a local startup
    invariant, not evidence that a separately deployed old worker has stopped.
    Deployment handover still needs independent validation before activation.
    """
    if not configured(env):
        if env.get('HL_TESTNET_EXPERIMENTAL_DISPATCH'):
            raise BoundaryError('EXPERIMENTAL_EXCLUSIVE_RUNTIME_MODE_REQUIRED')
        return None
    try:
        if (env.get('HL_TESTNET_EXPERIMENTAL_DISPATCH') != DISPATCH
                or not _HEX.fullmatch(env.get('HL_TESTNET_EXPERIMENTAL_RELEASE_ID', ''))
                or env.get('HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED') not in ('true', 'false')
                or env.get('HL_TESTNET_EXPERIMENTAL_PROTECTION_ENABLED') != 'true'
                or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
                or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') != 'false'
                or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') != 'false'
                or env.get('HL_TESTNET_FILLED_AUTOWAIT')
                or env.get('HL_TESTNET_SAFETY_PIPELINE')
                or env.get('HL_TESTNET_CARD_SYNC')
                or env.get('HL_TESTNET_FILLED_CARD_ID')
                or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION', 'disabled') != 'disabled'):
            raise ValueError()
        routes = {role: roles.route_for(env, role)['account']
                  for role in ('long_account', 'short_account')}
        if len(set(routes.values())) != 2:
            raise ValueError()
        start = contract.moment_ms(env['HL_TESTNET_EXPERIMENTAL_PLAN_NOT_BEFORE'])
        policy = env.get('HL_TESTNET_EXPERIMENTAL_ENTRY_POLICY', BOUNDED_ENTRY)
        if policy == CONTINUOUS_ENTRY:
            if env.get('HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL') not in (None, ''):
                raise ValueError()
            end = None
        elif policy == BOUNDED_ENTRY:
            end = contract.moment_ms(env['HL_TESTNET_EXPERIMENTAL_ENTRY_UNTIL'])
            if end <= start:
                raise ValueError()
        else:
            raise ValueError()
        if env.get('HL_TESTNET_EXPERIMENTAL_HANDOVER') not in (None, '', HANDOVER):
            raise ValueError()
        attested = (env.get('HL_TESTNET_EXPERIMENTAL_HANDOVER') != HANDOVER or
            env.get('HL_TESTNET_EXPERIMENTAL_PREDECESSOR_RETIRED_RELEASE') == env['HL_TESTNET_EXPERIMENTAL_RELEASE_ID'])
        return dict(domain='testnet', release_id=env['HL_TESTNET_EXPERIMENTAL_RELEASE_ID'],
                    dispatch_enabled=True, protection_enabled=True,
                    entries_enabled=env['HL_TESTNET_EXPERIMENTAL_ENTRY_ENABLED'] == 'true' and attested,
                    not_before_ms=start, entry_policy=policy,
                    entry_expires_at_ms=end, routes=routes)
    except (ValueError, TypeError, KeyError):
        raise BoundaryError('EXPERIMENTAL_TESTNET_RELEASE_CONFIGURATION_REQUIRED') from None


def entry_window_valid(release):
    """An absent expiry is valid only with an explicit continuous policy.

    Previously persisted/tested bounded release values may omit entry_policy.
    That compatibility never interprets a missing deadline as continuous.
    """
    if not isinstance(release, dict) or type(release.get('not_before_ms')) is not int:
        return False
    policy = release.get('entry_policy', BOUNDED_ENTRY)
    if 'entry_expires_at_ms' not in release:
        return False
    end = release['entry_expires_at_ms']
    if policy == CONTINUOUS_ENTRY:
        return end is None
    return (policy == BOUNDED_ENTRY and type(end) is int
            and end > release['not_before_ms'])


def entry_enabled(release, now_ms):
    return bool(entry_window_valid(release) and type(now_ms) is int
                and release.get('entries_enabled') is True
                and release['not_before_ms'] <= now_ms
                and (release['entry_expires_at_ms'] is None
                     or now_ms < release['entry_expires_at_ms']))


class ReleaseLoader:
    """Re-read mutable entry controls while pinning identity and account routes."""
    def __init__(self, env):
        self.env = env
        self.initial = configuration(env)
        if self.initial is None:
            raise BoundaryError('EXPLICIT_EXPERIMENTAL_RELEASE_REQUIRED')

    def __call__(self):
        value = configuration(self.env)
        pinned = ('domain', 'release_id', 'not_before_ms', 'routes', 'entry_policy')
        if value is None or any(value[k] != self.initial[k] for k in pinned):
            raise BoundaryError('EXPERIMENTAL_RELEASE_IDENTITY_CHANGED')
        return deepcopy(value)
