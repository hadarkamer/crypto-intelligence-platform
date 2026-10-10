"""Generate an initially disabled, separate worker after confirming its region.

JSON output is a valid YAML document accepted by the Render Blueprint parser.
This tool does not call Render or provision resources. Region and plan have no
defaults: confirm them from the intended workspace before generating a deploy file.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


REGIONS = ('oregon', 'ohio', 'frankfurt', 'singapore', 'virginia')
PLANS = ('1c-2g', '2c-4g')


def blueprint(region, plan):
    if region not in REGIONS or plan not in PLANS:
        raise ValueError('CONFIRMED_REGION_AND_PLAN_REQUIRED')
    return {
        'previews': {'generation': 'off'},
        'services': [{
            'type': 'worker', 'name': 'crypto-registered-research-oct2026',
            'runtime': 'docker', 'region': region, 'plan': plan,
            'repo': 'https://github.com/hadarkamer/crypto-intelligence-platform',
            'branch': 'research/registered-worker-host-20261008',
            'dockerfilePath': './deploy/research-worker/Dockerfile',
            'dockerContext': '.', 'autoDeployTrigger': 'off',
            'numInstances': 1, 'maxShutdownDelaySeconds': 300,
            'disk': {'name': 'original-research-state',
                     'mountPath': '/var/data/no-horizon', 'sizeGB': 5},
            'envVars': [
                {'key': 'NO_HORIZON_SUPERVISOR_ENABLED', 'value': '0'},
                {'key': 'RESEARCH_NO_HORIZON_READ_DATABASE_URL', 'sync': False},
            ],
        }],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', choices=REGIONS, required=True)
    parser.add_argument('--plan', choices=PLANS, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(blueprint(args.region, args.plan), output, indent=2)
        output.write('\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
