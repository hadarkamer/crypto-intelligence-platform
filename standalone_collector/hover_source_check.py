"""Explicit source-only acceptance check. No AI requests, queue or database writes."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent


def single(model,timeframe,directory):
    sys.path.insert(0,str(HERE/'runtime'))
    from market_vision.coinglass_heatmap_capture import capture_heatmaps
    from model1_execution import stage, failure_detail, classify
    directory.mkdir(parents=True,exist_ok=True)
    started=time.monotonic();stage('capture')
    report={'model':model,'timeframe':timeframe,'ai_calls':0,'database_writes':0}
    try:
        images=capture_heatmaps(directory,timeframes=(timeframe.lower(),))
        snapshot=images[0]['hover_snapshot']
        report.update(status='captured',rows=len(snapshot['rows']),unit=snapshot['unit'],
            total_value=snapshot['total_value'],source_time=snapshot['source_time'],
            hover_seconds=snapshot['duration_seconds'],coverage=snapshot['scope'])
        (directory/'hover.json').write_text(json.dumps(snapshot))
    except Exception as exc:
        report.update(status='failed',code=classify(exc),**failure_detail(exc))
    report['elapsed_seconds']=round(time.monotonic()-started,2)
    (directory/'report.json').write_text(json.dumps(report,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--single',nargs=2)
    args=parser.parse_args()
    if args.single:
        model,tf=args.single
        if model not in ('1','2','3') or tf not in ('12H','24H','48H'):parser.error('Invalid map identity')
        single(int(model),tf,args.output);return
    args.output.mkdir(parents=True,exist_ok=False)
    reports=[]
    for model in (1,2,3):
        for tf in ('12H','24H','48H'):
            dest=args.output/f'model{model}-{tf}'
            env={**os.environ,'COLLECTION_HEATMAP_MODEL':str(model),'OPENAI_API_KEY':'','DATABASE_URL':''}
            try:
                subprocess.run([sys.executable,str(Path(__file__).resolve()),'--single',str(model),tf,'--output',str(dest)],
                    env=env,check=True,timeout=180)
                report=json.loads((dest/'report.json').read_text())
            except (subprocess.SubprocessError,OSError,ValueError):
                report={'model':model,'timeframe':tf,'status':'failed','code':'probe_process_failed'}
            reports.append(report);print(json.dumps(report),flush=True)
            (args.output/'summary.json').write_text(json.dumps(reports,indent=2))
            if (report.get('source_capture_state') or {}).get('challenge_visible') or report.get('blocking_condition')=='challenge':
                return 1  # Never try other routes around a source access gate.
    return int(any(r['status']!='captured' for r in reports))


if __name__=='__main__':sys.exit(main())
