"""Replay the nine actual offline task results through PR120's isolated app lab.

Requires trade-plan-pad at e3fd4d1 and its Node dependencies, plus PGLITE_ROOT.
The app's real relay, SQL, validation and React renderer run. Collector queue
transport and external providers remain fixtures. This does not claim a real
Render worker/Postgres or live-source acceptance.
"""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess

LAB_SHA='00a0e60c4f3fd38e1d0ef6d38bdb7c6f9c52739f83e154ce496117c6904a2010'


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--app',type=Path,required=True)
    parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args()
    original=args.app/'.github/labs/coinglass-cadence.mjs'
    data=original.read_bytes()
    if hashlib.sha256(data).hexdigest()!=LAB_SHA:raise RuntimeError('app_lab_revision_changed')
    for model in (1,2,3):
        for timeframe in ('12H','24H','48H'):
            if not (args.evidence/f'{model}-{timeframe}/check.json').is_file():
                raise RuntimeError('all_nine_capture_checks_required')
    source=data.decode()
    marker='  function savedResult(model, timeframe, id) {'
    start=source.index(marker);end=source.index('  const provider =',start)
    source=source[:start]+r'''  function savedResult(model, timeframe, id) {
    const file=path.join(process.env.CG3_OFFLINE_EVIDENCE,`${model}-${timeframe}/result.json`);
    const original=JSON.parse(readFileSync(file,"utf8"));
    const oldId=original.run_id,oldAt=original.captured_at,oldSource=original.hover_snapshot.source_time;
    const sourceTime=new Date(clock-60000).toISOString(),at=new Date(clock).toISOString();
    const oldStamp=oldSource.slice(0,16).replace("T"," "),newStamp=sourceTime.slice(0,16).replace("T"," ");
    // Explicit fixture-clock rebasing only; prices, quantities, PNG hash and
    // row membership come from the real offline capture/reader/task output.
    function rebase(value) {
      if(Array.isArray(value))return value.map(rebase);
      if(value&&typeof value==="object")return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,rebase(v)]));
      if(typeof value!=="string")return value;
      if(value===oldId)return id;
      if(value===oldAt)return at;
      if(value===oldSource)return sourceTime;
      return value.replaceAll(oldStamp,newStamp);
    }
    const value=rebase(original);
    assert.equal(value.heatmap_model,model);assert.equal(value.timeframe,timeframe);
    assert.equal(value.hover_snapshot.rows.length,40);
    assert.equal(value.evidence.sha256,original.evidence.sha256);
    return value;
  }
''' + source[end:]
    # Exercise the changed model's failed-horizon continuation and manual retry.
    assert source.count('failedMap = "2:24H";')==1
    assert source.count('m2 = fx.maps[1];')==1
    source=source.replace('failedMap = "2:24H";','failedMap = "3:24H";').replace('m2 = fx.maps[1];','m2 = fx.maps[2];')
    target=args.app/'.github/labs/coinglass-earlygate-temporary.mjs'
    if target.exists():raise RuntimeError('temporary_lab_already_exists')
    env={**os.environ,'CG3_OFFLINE_EVIDENCE':str(args.evidence.resolve()),'OPENAI_API_KEY':'',
         'DATABASE_URL':'','COINGLASS_COOKIE_HEADER':'','COINGLASS_STORAGE_STATE_JSON':''}
    try:
        target.write_text(source)
        result=subprocess.run(['node',str(target),'b'],env=env,timeout=180)
        if result.returncode:raise SystemExit(result.returncode)
    finally:
        target.unlink(missing_ok=True)

if __name__=='__main__':main()
