import json,sys
import argparse
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument("--receiver-path",type=Path,required=True)
args=parser.parse_args();sys.path.insert(0,str(args.receiver_path.resolve()))
from decimal import Decimal
from collections import Counter
from hl_testnet_runtime.test_experimental_live_service import FullConnectionTests
from hl_testnet_runtime.test_experimental_execution_runtime import T
from hl_testnet_runtime.test_maxpain_execution import ARM
from experimental_execution_fixtures import r2732_message,hype_row71205_message
from hl_testnet_runtime.test_approved_alert_lifecycle import alert
from hl_testnet_runtime import request_budget,experimental_execution_runtime as core

def run(scenario):
 x=FullConnectionTests();x.setUp()
 try:
  settled=[]
  def instrument():
   original=x.raw.read
   def read(kind,*a,**kw):
    weight=request_budget.request_weight('/info',dict(type=kind))
    try:
     response=original(kind,*a,**kw)
     if kind in request_budget.SIZED and isinstance(response,list) and len(response)<=2000:
      weight=20+(len(response)+19)//20
     return response
    finally:settled.append(weight)
   x.raw.read=read
  if scenario=='parallel':
   x.oracle.t=ARM+10000;x.build(ARM-120000);x.oracle.instant_fraction=Decimal('.75')
   sol=alert(symbol='SOL',side='SHORT',approved_ms=ARM,cycle='independent-sol')
   account=x.release['routes']['short_account'];x.oracle.mark[core._lane(account,'SOL')]=sol['entry']
   instrument()
   msg=hype_row71205_message(entry=100,decision_ms=ARM-60000);x.seed(msg);x.service.tick()
   x.oracle.orders[x.oracle.oid('ENTRY')]['view']['status']='FILLED'
   x.oracle.t+=1001;x.service.tick();x.seed(sol)
  else:
   instrument()
   x.seed(r2732_message(entry=2.3,decision_ms=T-60000));x.service.tick()
   if scenario=='unknown':x.drop_stop=True
   x.service.tick()
  calls=[]
  for i in range(12):
   before=len(x.raw.calls);http=len(x.http);x.oracle.t+=1001;x.service.tick()
   calls.append((len(x.raw.calls)-before,len(x.http)-http))
  counter=Counter(c[0] for c in x.raw.calls)
  info=sum(request_budget.request_weight('/info',dict(type='orderStatus' if c[0]=='lookup' else c[0])) for c in x.raw.calls)
  exchange=sum(request_budget.request_weight('/exchange',dict(action=h['action'])) for h in x.http)
  return dict(info_calls=len(x.raw.calls),info_reserved_weight=info,wire_calls=len(x.http),wire_weight=exchange,
    total_reserved_weight=info+exchange,total_response_adjusted_weight=sum(settled)+2*counter['lookup']+exchange,kinds=dict(counter),last_four=calls[-4:],
    entries=[r['proposal']['symbol'] for r in x.oracle.requests if r['proposal']['operation']=='ENTRY'],
    rounds=[{k:r[k] for k in ('phase','read_calls','read_weight','round')} for r in x.store.load().get('position_recovery',{}).values()])
 finally:x.doCleanups()
print(json.dumps({s:run(s) for s in ('normal','unknown','parallel')},indent=2))
