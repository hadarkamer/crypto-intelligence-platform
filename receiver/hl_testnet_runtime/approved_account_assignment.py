"""Explicit public-address assignment. No key access, signing or exchange writes.

The opt-in alias lets the owner reuse the existing account's PUBLIC settings
without exporting its environment (which also contains secrets) to a client.
Suffix checking is a human configuration sanity check, NOT proof of ownership.
This record-only assignment must never be used as trade authorization.
"""
from datetime import datetime, timezone
import re

ADDRESS = re.compile(r'0x[0-9a-fA-F]{40}\Z')
MODE = 'existing_single_account_v1'
PUBLIC_FIELDS = tuple(f'HL_TESTNET_{side}_{field}_ADDRESS'
                      for side in ('LONG', 'SHORT') for field in ('ACCOUNT', 'AGENT'))


class AssignmentError(ValueError):
    """Fixed local codes only."""


def public_route_env(env):
    """Return only four public fields; never enumerate or mutate the environment."""
    result = {name: env.get(name, '') for name in PUBLIC_FIELDS}
    mode = env.get('HL_TESTNET_LONG_ACCOUNT_SOURCE', '')
    if not mode:
        return result  # Legacy settings are never used without explicit approval.
    if mode != MODE:
        raise AssignmentError('UNRECOGNIZED_LONG_ACCOUNT_SOURCE')
    suffix = env.get('HL_TESTNET_LONG_EXPECTED_SUFFIX', '')
    if not isinstance(suffix, str) or not re.fullmatch(r'[0-9a-f]{4}', suffix):
        raise AssignmentError('EXPECTED_FIRST_ACCOUNT_SUFFIX_REQUIRED')
    old = [env.get('HL_TESTNET_ACCOUNT_ADDRESS', ''), env.get('HL_TESTNET_AGENT_ADDRESS', '')]
    if not all(isinstance(v, str) and ADDRESS.fullmatch(v) and int(v[2:], 16) for v in old):
        raise AssignmentError('EXISTING_PUBLIC_ACCOUNT_PAIR_REQUIRED')
    account, agent = [v.lower() for v in old]
    if not account.endswith(suffix) or account == agent:
        raise AssignmentError('EXISTING_ACCOUNT_DOES_NOT_MATCH_APPROVAL')
    names = PUBLIC_FIELDS[:2]
    direct = [result[name] for name in names]
    if any(direct) and direct != old and [str(v).lower() for v in direct] != [account, agent]:
        raise AssignmentError('CONFLICTING_LONG_ACCOUNT_ASSIGNMENT')
    result.update(zip(names, (account, agent)))
    return result


def review_routes(routes, *, client=None):
    """One bounded public /info observation; never confirms key or trade readiness.

    A default abstraction mode is preserved as unresolved, not mapped to a
    different balance system merely because an account address was configured.
    Failure of this observation must not disable the existing order monitor.
    """
    report = dict(version='public-account-assignment-v1', environment='testnet',
        checked_at_utc=datetime.now(timezone.utc).isoformat(),
        order_requests_sent=0, signing_tested=False, ownership_verified=False,
        entry_sending_enabled=False, balance_checked=False, accounts={})
    used = set()
    for role in ('long_account', 'short_account'):
        route = routes.get(role, {})
        account, agent = route.get('account'), route.get('agent')
        if route.get('status') == 'WAITING_FOR_ACCOUNT' and account is None and agent is None:
            report['accounts'][role] = {'status': 'WAITING_FOR_ACCOUNT'}
            continue
        if (not all(isinstance(v, str) and ADDRESS.fullmatch(v) and int(v[2:], 16)
                    for v in (account, agent)) or account.lower() == agent.lower()
                or used.intersection((account.lower(), agent.lower()))):
            raise AssignmentError('INVALID_OR_REUSED_PUBLIC_ROUTES')
        used.update((account.lower(), agent.lower()))
        report['accounts'][role] = dict(account_suffix=account[-4:].lower(),
            agent_suffix=agent[-4:].lower(), status='CONFIGURED_NOT_VERIFIED',
            public_agent_link_verified=False, account_mode='NOT_READ',
            account_mode_requires_review=True)
    # Validate every route before making even the first network call.
    for role, result in report['accounts'].items():
        if result['status'] == 'WAITING_FOR_ACCOUNT':
            continue
        route = routes[role]
        account, agent = route['account'].lower(), route['agent'].lower()
        try:
            if client is None:
                from .checks import InfoReader
                client = InfoReader()  # Fixed Testnet host, /info only.
            user = client.read('userRole', user=account)
            link = client.read('userRole', user=agent)
            target = (link.get('data') or {}).get('user') if isinstance(link, dict) else None
            if (user != {'role': 'user'} or not isinstance(link, dict)
                    or link.get('role') != 'agent' or not isinstance(target, str)
                    or target.lower() != account):
                result['status'] = 'PUBLIC_AGENT_LINK_MISMATCH'
                continue
            result.update(status='PUBLIC_AGENT_LINK_VERIFIED', public_agent_link_verified=True)
            try:
                mode = client.read('userAbstraction', user=account)
                known = ('default', 'disabled', 'unifiedAccount', 'portfolioMargin', 'dexAbstraction')
                result['account_mode'] = mode if isinstance(mode, str) and mode in known else 'UNKNOWN'
                result['account_mode_requires_review'] = mode not in ('disabled', 'unifiedAccount')
            except Exception:
                result['account_mode'] = 'READ_UNAVAILABLE'
        except Exception:
            result['status'] = 'PUBLIC_AGENT_LINK_CHECK_UNAVAILABLE'
    report['both_public_links_verified'] = all(
        item.get('public_agent_link_verified') is True for item in report['accounts'].values())
    report['public_reads'] = getattr(client, 'calls', 0)
    return report
