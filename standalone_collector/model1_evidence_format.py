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
        'observed_mode':('Symbol','Pair','unknown'),'observed_model':('Model 1','other','unknown'),
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
    if image_path.is_file() and not image_path.is_symlink() and image_path.stat().st_size<=MAX_IMAGE:
        data=image_path.read_bytes()
        if data.startswith(b'\x89PNG\r\n\x1a\n'):
            image=data;diagnostic['image_retained']=True
            diagnostic['image_sha256']=hashlib.sha256(data).hexdigest()
    snapshot=root/'analysis_evidence.json'
    if snapshot.is_file() and not snapshot.is_symlink() and snapshot.stat().st_size<=MAX_JSON:
        try:value=json.loads(snapshot.read_text())
        except (ValueError,OSError):value={}
        if isinstance(value,dict):
            diagnostic['analysis']=safe_scan(value.get('analysis'))
            detail=safe_detail(value.get('image_preprocessing'))
            if detail and detail['source_sha256']==diagnostic.get('image_sha256'):
                diagnostic['image_preprocessing']=detail
    return image,diagnostic
