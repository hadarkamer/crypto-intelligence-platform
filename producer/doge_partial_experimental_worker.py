"""Requested DOGE experimental alerts; isolated partial-exit lifecycle, no orders."""
from copy import deepcopy
from pathlib import Path
import hashlib
from html import escape
from zoneinfo import ZoneInfo
import sol_proximity_experimental_worker as parent
import sol_proximity_experimental_store as storage
import doge_partial_experimental_signal as reducer
from maxpain_experimental_specs import FormulaSpec

RULE_ID = 'DOGE_MAXPAIN_ADVERSE_HALF_PART75_H24'
SPEC = FormulaSpec(RULE_ID,'ADVERSE0.5_TP80_STOP_SOURCE1D_H24__DIST_1_LT3__DOGE__BOTH__CAP1__PART75_AT_TP75_KEEP_STOP',
                   'DOGE',1,3,entry_adverse=.5,take_fraction=.8)

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
        f'ניסיוני, לא למסחר · נוסחה: {RULE_ID}\n'
        f'נגיעה ברמת כניסה: <b>{p["fill_price"]:.8g}</b> · {when} בישראל\n'
        f'סטופ: <b>{p["stop_price"]:.8g}</b>\n'
        f'טייק ראשון — 75% מהכמות: <b>{p["partial_take_price"]:.8g}</b>\n'
        f'טייק סופי — 25% הנותרים: <b>{p["take_price"]:.8g}</b>\n'
        f'סיום המעקב בזמן: {until} בישראל, 24 שעות מהמילוי המדומה.\n'
        f'מחיר מקור: {p["source_price"]:.8g} · יעד MaxPain: {p["target_price"]:.8g} · טווח: {escape(p["timeframe"])}\n'
        'כניסה אחרי חצי המרחק נגד היעד; הסטופ עוד חצי מרחק. אין קידום סטופ.\n'
        'פוזיציה או המתנה אחת בנוסחה; חסימת יעדים בקרבה עד 0.2% משותפת עם DOGE MaxPain הקיימת.\n'
        'מקור: Hyperliquid Perpetual. התראה ומעקב בלבד; אין הוראת מסחר או אישור מילוי בבורסה. '
        'נתוני המחקר מביננס אינם אימות ביצועים למקור החדש.')

class DogePartialWorker(parent.SolProximityWorker):
    def __init__(self,**kwargs):
        digest=hashlib.sha256(Path(reducer.__file__).read_bytes()+Path(__file__).read_bytes()+parent.config_hash(SPEC).encode()).hexdigest()
        kwargs.setdefault('cache',parent.ADDITIONAL_WORKERS['DOGE'].cache)
        super().__init__(spec=SPEC,signal_api=reducer,store_api=StoreAPI(),renderer=render_alert,config_digest=digest,**kwargs)

    def status(self):
        return {**super().status(),'pending_ttl_hours':24,'holding_time_limit':24,'holding_time_limit_unit':'hours',
            'operational_state_capacity':1,'partial_take_fraction':.75,'partial_take_progress':.75,
            'stop_source_distance_multiple':1.,'profit_lock':False,'shared_target_guard_with':'DOGE_MAXPAIN_DIST15_25_LONG_TF',
            'execution_forwarding':'DISABLED_NOTIFICATION_ONLY'}

WORKER=DogePartialWorker()
