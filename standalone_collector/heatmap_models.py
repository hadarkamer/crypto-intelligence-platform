"""App-owned heatmap identity. Never contains credentials or arbitrary URLs."""
from __future__ import annotations
import os
import re

MODELS=(1,2,3)
TIMEFRAMES=('12H','24H','48H')
PATHS={1:'LiquidationHeatMap',2:'LiquidationHeatMapNew',3:'LiquidationHeatMapModel3'}


def select_visible_control(page,label):
    """Use the source's exact visible button, or its current ARIA tab."""
    wanted=re.compile(r'^'+re.escape(label)+r'$',re.I)
    for role in ('button','tab'):
        matches=page.get_by_role(role,name=wanted)
        for index in range(min(matches.count(),8)):
            control=matches.first if index==0 else matches.nth(index)
            if control.is_visible():
                control.click(timeout=2000)
                page.wait_for_timeout(500)
                return True
    return False


def model_number(value):
    if type(value) is not int or value not in MODELS:
        raise ValueError('Unsupported heatmap model')
    return value


def source_url(model):
    return 'https://www.coinglass.com/pro/futures/'+PATHS[model_number(model)]+'?coin=BTC&type=symbol'


def schema_version(model):
    return f'coinglass-model{model_number(model)}.v1'


def child_environment(model):
    # Model is supplied by the trusted queue row, not page text. Each subprocess
    # receives its own mapping; no concurrent job changes the parent environment.
    return {**os.environ,'COLLECTION_HEATMAP_MODEL':str(model_number(model))}


raw_model=os.getenv('COLLECTION_HEATMAP_MODEL','1')
if raw_model not in ('1','2','3'):
    raise ValueError('Invalid child model setting')
HEATMAP_MODEL=int(raw_model)
MODEL_LABEL=f'Model {HEATMAP_MODEL}'
SOURCE_URL=source_url(HEATMAP_MODEL)
SCHEMA_VERSION=schema_version(HEATMAP_MODEL)
