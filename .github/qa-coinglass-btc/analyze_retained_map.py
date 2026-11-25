"""Process the already captured, private BTC evidence once with the normal scanner and normalizer."""
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from uuid import UUID

# Match the parent allowlist. Addresses, CoinGlass sessions, DB and bridge credentials
# are never visible to the analysis process. An existing AI binding is used in place.
ANALYSIS_ENV = {'PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','LD_LIBRARY_PATH','VIRTUAL_ENV',
                'SSL_CERT_FILE','SSL_CERT_DIR','REQUESTS_CA_BUNDLE','PYTHONDONTWRITEBYTECODE',
                'OPENAI_API_KEY','OPENAI_MARKET_SCANNER_MODEL'}

def analyze(run_id,sha):
    from retained_evidence import MAX_IMAGE, MAX_JSON, bounded_read, require, safe_root, unpack, write_once
    from modern_map_task import scan_schema, normalized, INSTRUCTIONS
    from market_vision.openai_heatmap_scanner import analyze_heatmap_images
    require(str(UUID(run_id))==run_id and bool(re.fullmatch('[0-9a-f]{64}',sha)))
    d=safe_root(); blob=bounded_read(d/(run_id+'.map'),MAX_IMAGE+MAX_JSON+520)
    require(sha256(blob).hexdigest()==sha)
    snap,image=unpack(blob,run_id)
    out=d/(run_id+'.result.json')
    if out.exists():
        result=json.loads(bounded_read(out,3*MAX_JSON)); require(result['run_id']==run_id and result['evidence']['sha256']==sha256(image).hexdigest());return 'already_saved'
    require(bool(os.environ.get('OPENAI_API_KEY')))
    candidates=[dict(candidate_id=f'c{i}',colour=g['colour'],side=g['side'],low=g['members'][0]['raw_price'],
                     high=g['members'][-1]['raw_price']) for i,g in enumerate(snap['groups'])]
    with tempfile.TemporaryDirectory(dir=d) as td:
        p=Path(td)/'image.png';p.write_bytes(image)
        marker=run_id+'.analysis-started'
        try:write_once(d,marker,sha.encode())
        except FileExistsError:return 'analysis_outcome_pending'
        raw=analyze_heatmap_images([dict(image=str(p),timeframe='12H',measure_candidates=candidates)],
              symbol='BTC',timeout_seconds=120,evidence_schema=scan_schema(),evidence_instructions=INSTRUCTIONS)
        result=normalized(raw,snap,run_id,image)
        write_once(d,out.name,json.dumps(result,allow_nan=False,separators=(',',':')).encode())
    return 'analysis_saved'

if __name__=='__main__':
    os.umask(0o077)
    clean={k:v for k,v in os.environ.items() if k in ANALYSIS_ENV}
    if clean!=dict(os.environ):os.execve(sys.executable,[sys.executable,'-B',str(Path(__file__).resolve()),*sys.argv[1:]],clean)
    sys.path.insert(0,str(Path(__file__).resolve().parent/'runtime'))
    try: print(analyze(*sys.argv[1:]))
    except Exception: print('analysis_preflight_or_result_failed');raise SystemExit(1)
