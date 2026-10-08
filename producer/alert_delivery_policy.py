"""Owner-selected notification roster; collection and ordinary Watch stay independent.

The 2026-10-05 roster permanently retires experimental formulas preceding XRP
R2732. A legacy ALL deployment profile must not re-enable those old sources.
"""
from __future__ import annotations

import os

ROSTER_VERSION = "owner-r2732-and-later-20261008-v3-doge-partial"
SELECTED_MANUAL_RULES = frozenset()
MAXPAIN_COMPONENT_RULES = frozenset((
    'HYPE_MAXPAIN_DIST05_15_LONG_TF',
    'DOGE_MAXPAIN_DIST15_25_LONG_TF',
    'DOGE_MAXPAIN_ADVERSE_HALF_PART75_H24',
    'XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF',
    'ETH_MAXPAIN_LONG_DIST1_3',
))
SELECTED_EXPERIMENTAL_RULES = frozenset((
    'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK',
    'HYPE_ROW71205_SHORT',
    'SOL_MAXPAIN_DIST1_3_RANGE24',
    'SOL_G65_K49_PROFIT_LOCK',
)) | MAXPAIN_COMPONENT_RULES
RETIRED_EXPERIMENTAL_RULES = frozenset((
    'U21_XRP_SHORT', 'SOL_MAXPAIN_PROXIMITY_GT15',
    'C1274', 'PRICE_OI_ENTRY2', 'PRICE_OI_SPOT65', 'CONSENSUS_FULL',
    'C0964', 'MAGNET_OBSERVATION_DOGE_SHORT',
    'CORE_FUTURES_CVD_SPOT_CVD_TOTAL_65', 'FORMULA_MP65_CVD_SHORT',
    'ORDERED_V7_EXPERIMENTAL',
))
_SELECTED_PROFILES = frozenset(('SELECTED_EXPERIMENTAL_ONLY',
                               'ORDINARY_AND_SELECTED_EXPERIMENTAL'))
_PROFILES = frozenset(('ALL',)) | _SELECTED_PROFILES


def profile():
    return os.getenv('ALERT_DELIVERY_PROFILE', 'ALL').strip().upper()


def ordinary_alerts_enabled():
    return profile() in ('ALL', 'ORDINARY_AND_SELECTED_EXPERIMENTAL')


def other_experimental_alerts_enabled():
    # Ordered-v7, dual CVD65 and the dedicated MP65/CVD alert are retired.
    return False


def u21_experimental_enabled():
    # Its worker only drains an already open observation after retirement.
    return False


def selected_rule_enabled(rule_id):
    return profile() in _PROFILES and rule_id in SELECTED_EXPERIMENTAL_RULES


def xrp_r2732_experimental_enabled():
    return selected_rule_enabled('R2732_XRP_SHORT_NY_WEEKDAYS_LOCK')


def hype_row71205_experimental_enabled():
    return selected_rule_enabled('HYPE_ROW71205_SHORT')


def sol_proximity_experimental_enabled():
    return selected_rule_enabled('SOL_MAXPAIN_DIST1_3_RANGE24')


def sol_g65_experimental_enabled():
    return selected_rule_enabled('SOL_G65_K49_PROFIT_LOCK')


def maxpain_component_experimental_enabled(rule_id):
    return rule_id in MAXPAIN_COMPONENT_RULES and selected_rule_enabled(rule_id)


def manual_rule_enabled(rule_id):
    # No legacy manual formula belongs to the current owner-selected roster.
    return False


def status():
    current = profile()
    return {'profile': current, 'configuration_valid': current in _PROFILES,
            'roster_version': ROSTER_VERSION,
            'ordinary_alerts_enabled': ordinary_alerts_enabled(),
            'other_experimental_alerts_enabled': other_experimental_alerts_enabled(),
            'u21_experimental_enabled': u21_experimental_enabled(),
            'xrp_r2732_experimental_enabled': xrp_r2732_experimental_enabled(),
            'hype_row71205_experimental_enabled': hype_row71205_experimental_enabled(),
            'sol_proximity_experimental_enabled': sol_proximity_experimental_enabled(),
            'sol_g65_experimental_enabled': sol_g65_experimental_enabled(),
            'selected_experimental_rule_ids': (sorted(SELECTED_EXPERIMENTAL_RULES)
                                               if current in _PROFILES else []),
            'retired_experimental_rule_ids': sorted(RETIRED_EXPERIMENTAL_RULES),
            'manual_rule_allowlist': []}

