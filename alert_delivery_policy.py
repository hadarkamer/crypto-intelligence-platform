"""Deployment-controlled notification selection; collection stays independent."""
from __future__ import annotations

import os

SELECTED_MANUAL_RULES = frozenset(('C1274', 'MAGNET_OBSERVATION_DOGE_SHORT'))
_PROFILES = frozenset(('ALL', 'SELECTED_EXPERIMENTAL_ONLY'))


def profile():
    return os.getenv('ALERT_DELIVERY_PROFILE', 'ALL').strip().upper()


def ordinary_alerts_enabled():
    return profile() == 'ALL'


def manual_rule_enabled(rule_id):
    current = profile()
    return current == 'ALL' or (current == 'SELECTED_EXPERIMENTAL_ONLY'
                                and rule_id in SELECTED_MANUAL_RULES)


def status():
    current = profile()
    return {'profile': current, 'configuration_valid': current in _PROFILES,
            'ordinary_alerts_enabled': current == 'ALL',
            'manual_rule_allowlist': (None if current == 'ALL' else
                                      sorted(SELECTED_MANUAL_RULES)
                                      if current == 'SELECTED_EXPERIMENTAL_ONLY' else [])}
