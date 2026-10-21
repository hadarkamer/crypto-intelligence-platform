"""DOGE observations with separate approved-alert forwarding; no order sender."""
from copy import deepcopy
from pathlib import Path
import hashlib
import os
from html import escape
from zoneinfo import ZoneInfo
import sol_proximity_experimental_worker as parent
import sol_proximity_experimental_store as storage
import doge_partial_experimental_signal as reducer
from maxpain_experimental_specs import FormulaSpec

RULE_ID = 'DOGE_MAXPAIN_ADVERSE_HALF_PART75_H24'
SPEC = FormulaSpec(RULE_ID,'ADVERSE0.5_TP80_STOP_SOURCE1D_H24__DIST_1_LT3__DOGE__BOTH__CAP1__PART75_AT_TP75_KEEP_STOP',
                   'DOGE',1,3,entry_adverse=.5,take_fraction=.8)
# Preserve the deployed source fingerprint: only execution wiring and wording
# change here. The frozen reducer/spec inputs are checked explicitly so a
# strategy change still requires a deliberate source-state migration.
SOURCE_CONFIG_SHA256 = 'd14e1ef6d8936143ce74e31b0d6015eb0d5051e030d12607413ea55f87b33c43'
REDUCER_SHA256 = '07c35d4b404a8f5db15cf5cec44618712d108444de39e2148dc62325d0727a72'
BASE_CONFIG_SHA256 = '9e1aeca99cf85d00e8de6af3ab7d619995cd20b897a96f076240ccec6753bc55'

def source_config_hash():
    if (hashlib.sha256(Path(reducer.__file__).read_bytes()).hexdigest() != REDUCER_SHA256
            or parent.config_hash(SPEC) != BASE_CONFIG_SHA256):
        raise ValueError('DOGE source configuration changed; explicit migration required')
    return SOURCE_CONFIG_SHA256

class StoreAPI:
    def __getattr__(self,name):return getattr(storage,name)

    def ingest(self,scope,decoded,bars,now,*,config_sha256,database_url=None):
        def action(state):
            incoming=deepcopy(decoded)
            for row in incoming['rows']:
                if row['eligible'] and any(reducer.near(row['target_price'],p['target_price'])for p in state.get('_admission_peers',[])):
                    row['eligible']=False
                    reducer.count(state,'CROSS_FORMULA_NEAR_TARGET_BLOCKED')
            return reducer.ingest(state,incoming,now,bars)
        return storage.transact(scope,now,config_sha256,action,database_url=database_url,admission_coin='DOGE')

    def advance(self,scope,bars,now,*,config_sha256,database_url=None):
        return storage.transact(scope,now,config_sha256,lambda s:reducer.advance(s,bars,now),database_url=database_url)

def render_alert(p):
    side='LONG'if p['direction']==1 else 'SHORT'
    when=parent.dt(p['fill_ms']).astimezone(ZoneInfo('Asia/Jerusalem')).strftime('%d.%m.%Y %H:%M')
    until=parent.dt(p['hold_until_ms']).astimezone(ZoneInfo('Asia/Jerusalem')).strftime('%d.%m.%Y %H:%M')
    return (f'🧪 <b>DOGE · כניסה נגדית ומימוש 75% · {side}</b>\n'
        f'ניסיוני · נוסחה: {RULE_ID}\n'
        f'נגיעה ברמת כניסה: <b>{p["fill_price"]:.8g}</b> · {when} בישראל\n'
        f'סטופ: <b>{p["stop_price"]:.8g}</b>\n'
        f'טייק ראשון — 75% מהכמות: <b>{p["partial_take_price"]:.8g}</b>\n'
        f'טייק סופי — 25% הנותרים: <b>{p["take_price"]:.8g}</b>\n'
        f'סיום המעקב בזמן: {until} בישראל, 24 שעות מהמילוי המדומה.\n'
        f'מחיר מקור: {p["source_price"]:.8g} · יעד MaxPain: {p["target_price"]:.8g} · טווח: {escape(p["timeframe"])}\n'
        'כניסה אחרי חצי המרחק נגד היעד; הסטופ עוד חצי מרחק. אין קידום סטופ.\n'
        'פוזיציה או המתנה אחת בנוסחה; חסימת יעדים בקרבה עד 0.2% משותפת עם DOGE MaxPain הקיימת.\n'
        'מקור: Hyperliquid Perpetual. הנוסחה מחוברת לבוט המסחר; ההתראה עצמה אינה אישור מילוי בבורסה. '
        'נתוני המחקר מביננס אינם אימות ביצועים למקור החדש.')

class DogePartialWorker(parent.SolProximityWorker):
    def __init__(self,**kwargs):
        digest=source_config_hash()
        kwargs.setdefault('cache',parent.ADDITIONAL_WORKERS['DOGE'].cache)
        super().__init__(spec=SPEC,signal_api=reducer,store_api=StoreAPI(),renderer=render_alert,config_digest=digest,**kwargs)

    def status(self):
        from experimental_execution_bridge import approved_enabled
        from experimental_execution_transport import APPROVED_MODE
        forwarding = approved_enabled() and os.environ.get('EXPERIMENTAL_EXECUTION_FORWARD_MODE') == APPROVED_MODE
        return {**super().status(),'pending_ttl_hours':24,'holding_time_limit':24,'holding_time_limit_unit':'hours',
            'operational_state_capacity':1,'partial_take_fraction':.75,'partial_take_progress':.75,
            'stop_source_distance_multiple':1.,'profit_lock':False,'shared_target_guard_with':'DOGE_MAXPAIN_DIST15_25_LONG_TF',
            'notification_only':not forwarding,
            'position_sizing':'RECEIVER_FORMULA_BANKROLL' if forwarding else 'NOT_APPLICABLE_ALERT_ONLY',
            'execution_forwarding':'APPROVED_ALERTS_CONFIGURED' if forwarding else 'DISABLED_BRIDGE'}

WORKER=DogePartialWorker()
