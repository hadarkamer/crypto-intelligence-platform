"""Opt-in public Testnet balance observation, not a trading readiness gate.

Reads the actual account, never its agent key. Compare reported sources without
adding them, moving funds, or converting default into another account mode.
Only the requested balance fields appear in the private runtime report.
"""
from datetime import datetime, timezone
from decimal import Decimal
import time

from . import checks

KNOWN_MODES = ('default', 'disabled', 'unifiedAccount', 'portfolioMargin', 'dexAbstraction')


def _text(value):
    return format(value, 'f')


def _perp(state):
    if not isinstance(state, dict) or not isinstance(state.get('marginSummary'), dict):
        raise checks.Blocked('INVALID_PERP_BALANCE_RESPONSE')
    summary = state['marginSummary']
    values = {key: checks.number(summary.get(key), signed=key in ('accountValue', 'totalRawUsd'))
              for key in ('accountValue', 'totalRawUsd', 'totalMarginUsed', 'totalNtlPos')}
    withdrawable = checks.number(state.get('withdrawable'))
    positions = state.get('assetPositions')
    if not isinstance(positions, list) or len(positions) > 10000:
        raise checks.Blocked('INVALID_POSITION_RESPONSE')
    count, seen = 0, set()
    for row in positions:
        position = row.get('position') if isinstance(row, dict) else None
        if not isinstance(position, dict) or not isinstance(position.get('coin'), str):
            raise checks.Blocked('INVALID_POSITION_RESPONSE')
        if position['coin'] in seen:
            raise checks.Blocked('DUPLICATE_POSITION_RESPONSE')
        seen.add(position['coin'])
        count += checks.number(position.get('szi'), signed=True) != 0
    return dict(account_value_usd=_text(values['accountValue']),
                raw_usd=_text(values['totalRawUsd']),
                margin_used_usd=_text(values['totalMarginUsed']),
                position_notional_usd=_text(values['totalNtlPos']),
                withdrawable_usd=_text(withdrawable), nonzero_positions=count)


def review_balance(account, *, expected_amount, client=None):
    """At most six public calls. A match compares observations, not a deposit receipt.

    activeAssetData is corroboration for BTC only, NOT a specific trade check or
    a statement about every asset. Failure of that extra read does not erase a
    successful balance read. A mode change or an overlong sample invalidates the
    combined comparison. This function never changes existing execution guards.
    """
    report = dict(version='testnet-balance-observation-v1', environment='testnet',
        status='BALANCE_READ_UNAVAILABLE', account_mode='NOT_READ',
        account_mode_resolved=False, account_mode_changed_during_read=False,
        balance_observed=False, expected_amount_observed=False,
        order_requests_sent=0, account_settings_changes=0, transfers_sent=0,
        signing_tested=False, specific_trade_capacity_checked=False,
        entry_sending_enabled=False, balances_added_together=False,
        started_at_utc=datetime.now(timezone.utc).isoformat(), public_reads=0)
    started = time.monotonic()
    initial_calls = getattr(client, 'calls', 0)
    try:
        account = checks.address(account)
        expected = checks.number(expected_amount)
        if expected <= 0:
            raise checks.Blocked('POSITIVE_EXPECTED_AMOUNT_REQUIRED')
        report.update(account_suffix=account[-4:], expected_amount_usd=_text(expected))
        client = checks.InfoReader() if client is None else client
        if client.read('userRole', user=account) != {'role': 'user'}:
            raise checks.Blocked('USER_ACCOUNT_REQUIRED_FOR_BALANCE_READ')
        mode = client.read('userAbstraction', user=account)
        if not isinstance(mode, str) or mode not in KNOWN_MODES:
            raise checks.Blocked('UNRECOGNIZED_ACCOUNT_MODE')
        report['account_mode'] = mode
        perp = _perp(client.read('clearinghouseState', user=account))
        spot = client.read('spotClearinghouseState', user=account)
        total, unheld = checks.unified_usdc(spot)
        spot_summary = dict(usdc_total=_text(total), usdc_unheld=_text(unheld),
            positive_other_asset_count=sum(
                checks.number(row.get('total')) > 0 for row in spot['balances'] if row.get('coin') != 'USDC'))
        report.update(perp=perp, spot=spot_summary)
        try:
            active = client.read('activeAssetData', user=account, coin='BTC')
            available, max_size = checks.capacity(active, account, 'BTC')
            report['btc_capacity_observation'] = dict(status='OBSERVED',
                minimum_reported_available_usd=_text(available),
                both_size_caps_positive=max_size > 0, specific_order_checked=False)
        except Exception:
            report['btc_capacity_observation'] = dict(status='UNAVAILABLE', specific_order_checked=False)
        after = client.read('userAbstraction', user=account)
        if after != mode:
            report['account_mode_changed_during_read'] = True
            raise checks.Blocked('ACCOUNT_MODE_CHANGED_DURING_READ')
        if time.monotonic() - started > 20:
            raise checks.Blocked('BALANCE_SAMPLE_EXPIRED')
        perp_match = Decimal(perp['account_value_usd']) == expected
        spot_match = total == expected
        report.update(balance_observed=True,
            expected_matches_perp_account_value=perp_match,
            expected_matches_spot_usdc=spot_match,
            account_mode_resolved=mode in ('disabled', 'unifiedAccount'))
        if mode in ('unifiedAccount', 'portfolioMargin'):
            # Perp dex values are not the unified cash balance.
            report.update(expected_amount_observed=spot_match,
                          observed_balance_source='spotClearinghouseState')
        elif mode == 'disabled':
            report.update(expected_amount_observed=perp_match,
                          observed_balance_source='clearinghouseState')
        else:
            # Report independent raw observations, not an undocumented mode mapping.
            matches = []
            if perp_match:
                matches.append('clearinghouseState')
            if spot_match:
                matches.append('spotClearinghouseState')
            report.update(expected_amount_observed=bool(matches),
                          observed_balance_source=','.join(matches) or 'NO_EXACT_MATCH',
                          default_mode_not_coerced=True)
        report['status'] = ('EXPECTED_AMOUNT_OBSERVED_READ_ONLY'
                            if report['expected_amount_observed'] else 'BALANCE_READ_AMOUNT_DIFFERS')
    except checks.Blocked as exc:
        report['status'] = str(exc)
    except Exception:
        report['status'] = 'BALANCE_READ_UNAVAILABLE'
    finally:
        report['public_reads'] = getattr(client, 'calls', 0) - initial_calls
        report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
    return report
