"""Offline replay on two original PNGs. No provider calls; input files pre-exist.

Usage: python replay_saved.py verified_fixtures.json output_directory
Fixtures contain per-image independently checked plot/legend boxes and axis ticks.
The observations listed below are historical INPUTS for audit, not extraction rules.
"""
import hashlib
import json
from pathlib import Path
import sys
from edge_zones import extract_current_cores, audit_saved_zones, public_report, Policy

KNOWN={
 '12H':{'sha':'3c6bc765fe70d0f2d6e7dc7ff4bf7bd709020e16737c0563206e38a859b979a2',
        'interval':[75930,75990],
        'old':[('above',76250,76650,'many'),('above',77250,77550,'many'),
               ('below',74950,75250,'many'),('below',74400,74750,'many')]},
 '48H':{'sha':'62a8d03a140fab6e92c574b759916b370db5d33bf95378493e1f32575bfad966',
        'interval':[75850,76000],
        'old':[('above',76750,77050,'normal'),('above',77650,77950,'many'),
               ('above',79750,80150,'many'),('below',74400,74650,'many'),
               ('below',75550,75750,'many'),('below',75000,75250,'normal')]},
}

def main():
    fixture_path=Path(sys.argv[1]);out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
    fixtures=json.loads(fixture_path.read_text())
    results={}
    for tf in ('12H','48H'):
        f=fixtures[tf];k=KNOWN[tf];raw=Path(f['image_path']).read_bytes()
        m=extract_current_cores(raw,expected_sha256=k['sha'],plot=f['plot'],legend=f['legend'],
            anchors=f['anchors'],price_interval=k['interval'],identity={'symbol':'BTC','model':1,'timeframe':tf})
        r=public_report(m)
        r['audit_saved']=audit_saved_zones(m,[dict(side=s,price_low=l,price_high=h,intensity=q) for s,l,h,q in k['old']])
        # Stability is reported, never solved by silently choosing a threshold.
        stable=[]
        for width in (12,20,28):
            trial=extract_current_cores(raw,expected_sha256=k['sha'],plot=f['plot'],legend=f['legend'],
                anchors=f['anchors'],price_interval=k['interval'],identity={'symbol':'BTC','model':1,'timeframe':tf},
                policy=Policy(stripe_width=width))
            stable.append({'stripe_width':width,'many_cores':[[b['price_low'],b['price_high']] for b in trial['bands'] if b['intensity']=='many']})
        r['stripe_sensitivity']=stable
        r['fixture_calibration']='manual image-specific anchors verified before replay; not automated tick reading'
        r['module_sha256']=hashlib.sha256(Path(__file__).with_name('edge_zones.py').read_bytes()).hexdigest()
        results[tf]=r
        (out/(tf+'_edge_report.json')).write_text(json.dumps(r,indent=2,ensure_ascii=False))
    m48=results['48H'];m12=results['12H']
    assertions={
        '48h_75550_75750_not_a_current_band':all(not (b['price_low']<75750 and b['price_high']>75550) for b in m48['bands']),
        '48h_missing_upper_bright_band_recovered':any(b['intensity']=='many' and b['price_low']<78500 and b['price_high']>78300 for b in m48['bands']),
        '48h_lower_bright_band_above_old_wrong_bounds':any(b['intensity']=='many' and b['price_high']>74650 and b['price_low']<74800 for b in m48['bands']),
        '12h_upper_bright_band_found':any(b['intensity']=='many' and b['price_low']<76770 and b['price_high']>76570 for b in m12['bands']),
        '12h_lower_bright_band_found':any(b['intensity']=='many' and b['price_low']<75270 and b['price_high']>75070 for b in m12['bands']),
        'input_hashes_unchanged':all(hashlib.sha256(Path(fixtures[tf]['image_path']).read_bytes()).hexdigest()==KNOWN[tf]['sha'] for tf in KNOWN),
    }
    summary={ 'version':'current-edge-core.v1','assertions':assertions,
              'passed':sum(assertions.values()),'total':len(assertions),
              'scan_calls':0,'model_calls':0,'sheet_writes':0,
              'calibration':'manually verified per-image fixtures',
              'results':{tf:{'source_sha256':r['source_sha256'],'axis_residual_px':r['axis']['residual_px'],
                    'band_count':len(r['bands']),'many_cores':[[b['price_low'],b['price_high']] for b in r['bands'] if b['intensity']=='many'],
                    'audit':r['audit_saved'],'sensitivity':r['stripe_sensitivity']} for tf,r in results.items()}}
    (out/'summary.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary,indent=2))
    if not all(assertions.values()):sys.exit(2)

if __name__=='__main__': main()
