"""Deployment-controlled notification selection; collection stays independent."""
from __future__ import annotations

import os

SELECTED_MANUAL_RULES = frozenset()
SELECTED_EXPERIMENTAL_RULES = frozenset(('U21_XRP_SHORT',))
_SELECTED_PROFILES = frozenset(('SELECTED_EXPERIMENTAL_ONLY',
                               'ORDINARY_AND_SELECTED_EXPERIMENTAL'))
_PROFILES = frozenset(('ALL',)) | _SELECTED_PROFILES


def profile():
    return os.getenv('ALERT_DELIVERY_PROFILE', 'ALL').strip().upper()


def ordinary_alerts_enabled():
    return profile() in ('ALL', 'ORDINARY_AND_SELECTED_EXPERIMENTAL')


def other_experimental_alerts_enabled():
    return profile() == 'ALL'


def u21_experimental_enabled():
    return profile() in _PROFILES


def manual_rule_enabled(rule_id):
    current = profile()
    return current == 'ALL' or (current in _SELECTED_PROFILES
                                and rule_id in SELECTED_MANUAL_RULES)


def status():
    current = profile()
    return {'profile': current, 'configuration_valid': current in _PROFILES,
            'ordinary_alerts_enabled': current in ('ALL', 'ORDINARY_AND_SELECTED_EXPERIMENTAL'),
            'other_experimental_alerts_enabled': current == 'ALL',
            'u21_experimental_enabled': current in _PROFILES,
            'selected_experimental_rule_ids': (None if current == 'ALL' else
                                               sorted(SELECTED_EXPERIMENTAL_RULES)
                                               if current in _SELECTED_PROFILES else []),
            'manual_rule_allowlist': (None if current == 'ALL' else
                                      sorted(SELECTED_MANUAL_RULES)
                                      if current in _SELECTED_PROFILES else [])}
