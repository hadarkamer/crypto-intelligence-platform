"""Local formatting only: no database, network, credentials or provider calls.

The service's existing job store decides where evidence is retained. These
helpers collect a bounded saved PNG plus a fixed numeric/enum-only diagnostic.
"""
import hashlib
import json
import math
from pathlib import Path

MAX_IMAGE=4*1024*1024
MAX_JSON=65536


def number(value):
    return value if type(value) in (int,float) and math.isfinite(value) and abs(value)<=1e15 else None


def safe_scan(source):
    if not isinstance(source,dict):return {}
    out={}
    enums={'observed_timeframe':('12h','24h','48h','unknown'),
        'observed_mode':('Symbol','Pair','unknown'),'observed_model':('Model 1','Model 2','Model 3','other','unknown'),
        'observed_symbol':('BTC','other','unknown'),'current_price_confidence':('high','medium','low'),
        'blocking_condition':('none','loading','login','blur','challenge','unknown')}
    for key,allowed in enums.items():
        if source.get(key) in allowed:out[key]=source[key]
    if type(source.get('readable')) is bool:out['readable']=source['readable']
    out['current_price_estimate']=number(source.get('current_price_estimate'))
    value=source.get('current_price_range')
    if isinstance(value,dict):
        interval={k:number(value.get(k)) for k in ('low','high','axis_low','axis_high')}
        if value.get('confidence') in ('high','medium','low'):interval['confidence']=value['confidence']
        if value.get('basis') in ('last_candle_axis_bracket','unreadable'):interval['basis']=value['basis']
        out['current_price_range']=interval
    axis=source.get('price_axis_range')
    if isinstance(axis,dict):
        out['price_axis_range']={k:number(axis.get(k)) for k in ('low','high')}
        if isinstance(axis.get('price_evidence'),str):out['price_axis_range']['price_evidence']=axis['price_evidence'][:500]
    levels=source.get('prominent_levels')
    if isinstance(levels,list):
        out['prominent_levels']=[]
        for level in levels[:40]:
            if not isinstance(level,dict):continue
            item={k:number(level.get(k)) for k in ('price_low','price_high','uncertainty_usd','strength_score')}
            for k,allowed in {'side':('above','below'),'shape':('point','range'),
                'intensity':('many','normal','few'),'row_colour':('yellow','green','blue'),
                'confidence':('high','medium','low'),'price_precision':('verified_label','axis_estimate'),
                'evidence_basis':('numeric_row_label','tooltip','axis_interpolation')}.items():
                if level.get(k) in allowed:item[k]=level[k]
            for k in ('continuous','weak_gap_present','right_edge_visible'):
                if type(level.get(k)) is bool:item[k]=level[k]
            if isinstance(level.get('price_evidence'),str):item['price_evidence']=level['price_evidence'][:500]
            if isinstance(level.get('prices'),list):item['prices']=[number(p) for p in level['prices'][:2]]
            out['prominent_levels'].append(item)
    for field in ('above_price','below_price'):
        group=source.get(field)
        if isinstance(group,dict):
            candidates=[group.get('main_zone')]
            if isinstance(group.get('secondary_zones'),list):candidates+=group['secondary_zones'][:4]
        elif isinstance(group,list):candidates=group[:5]
        else:continue
        out[field]=[]
        for zone in candidates:
            if not isinstance(zone,dict):continue
            item={k:number(zone.get(k)) for k in ('low_price','high_price')}
            if zone.get('confidence') in ('high','medium','low'):item['confidence']=zone['confidence']
            if zone.get('relative_strength') in ('very_strong','strong','medium','weak'):
                item['relative_strength']=zone['relative_strength']
            out[field].append(item)
    return out


def safe_detail(value):
    if not isinstance(value,dict):return None
    import re
    if any(not isinstance(value.get(k),str) or re.fullmatch('[a-f0-9]{64}',value[k]) is None
           for k in ('source_sha256','sha256')):return None
    out={k:value[k] for k in ('source_sha256','sha256')}
    for k,n in (('crop_box',4),('dimensions',2)):
        v=value.get(k)
        if not isinstance(v,(list,tuple)) or len(v)!=n or any(type(x) is not int or not 0<=x<=100000 for x in v):return None
        out[k]=list(v)
    if value.get('scale')!=2:return None
    out['scale']=2
    return out


def save_analysis_snapshot(raw,root,image):
    from price_detail_input import detail_provenance
    scans=raw.get('scans') if isinstance(raw,dict) else None
    scan=scans[0] if isinstance(scans,list) and len(scans)==1 else {}
    payload={'analysis':safe_scan(scan),'image_preprocessing':safe_detail(detail_provenance(image)),
             'assessment_validated':False}
    if isinstance(scans,list):payload['scan_count']=min(len(scans),100)
    if isinstance(scans,list) and len(scans)==1 and isinstance(scan,dict):
        payload['prominent_levels_present']='prominent_levels' in scan
    encoded=json.dumps(payload,allow_nan=False)
    if len(encoded.encode())>MAX_JSON:raise ValueError('Evidence metadata too large')
    path=Path(root)/'analysis_evidence.json'
    path.write_text(encoded,encoding='utf-8');path.chmod(0o600)


def evidence_payload(directory,timeframe):
    if timeframe not in ('12H','24H','48H'):raise ValueError('Unknown timeframe')
    root=Path(directory)
    image_path=root/'capture'/f'coinglass_btc_heatmap_{timeframe.lower()}.png'
    image=None
    diagnostic={'timeframe':timeframe,'assessment_validated':False,'image_retained':False}
    progress=root/'execution_progress.json'
    if progress.is_file() and not progress.is_symlink() and progress.stat().st_size<=8192:
        try:
            from model1_execution import safe_progress
            value=safe_progress(json.loads(progress.read_text(encoding='utf-8')))
        except (ValueError,OSError):value=None
        if value is not None:diagnostic['execution_progress']=value
    if image_path.is_file() and not image_path.is_symlink() and image_path.stat().st_size<=MAX_IMAGE:
        data=image_path.read_bytes()
        if data.startswith(b'\x89PNG\r\n\x1a\n'):
            image=data;diagnostic['image_retained']=True
            diagnostic['image_sha256']=hashlib.sha256(data).hexdigest()
    render=image_path.with_suffix('.render.json')
    if render.is_file() and not render.is_symlink() and render.stat().st_size<=8192:
        try:value=json.loads(render.read_text(encoding='utf-8'))
        except (ValueError,OSError):value={}
        if isinstance(value,dict):
            from capture_readiness import safe_readiness,safe_network
            diagnostic['source_readiness']=safe_readiness(value.get('source_readiness'))
            diagnostic['source_network']=safe_network(value.get('source_network'))
    snapshot=root/'analysis_evidence.json'
    if snapshot.is_file() and not snapshot.is_symlink() and snapshot.stat().st_size<=MAX_JSON:
        try:value=json.loads(snapshot.read_text())
        except (ValueError,OSError):value={}
        if isinstance(value,dict):
            diagnostic['analysis']=safe_scan(value.get('analysis'))
            if type(value.get('scan_count')) is int and 0<=value['scan_count']<=100:
                diagnostic['scan_count']=value['scan_count']
            if type(value.get('prominent_levels_present')) is bool:
                diagnostic['prominent_levels_present']=value['prominent_levels_present']
            detail=safe_detail(value.get('image_preprocessing'))
            if detail and detail['source_sha256']==diagnostic.get('image_sha256'):
                diagnostic['image_preprocessing']=detail
    return image,diagnostic
