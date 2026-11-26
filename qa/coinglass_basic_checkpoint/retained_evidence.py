"""QA-only storage for a protected BTC/M1/12H market observation. No session data."""
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import tempfile
from uuid import UUID

MAX_IMAGE = 4 * 1024 * 1024
MAX_JSON = 1024 * 1024
def require(ok):
    if not ok:
        raise ValueError('pilot_evidence_invalid')

def checked_json(data):
    from modern_map_basic_numbers import validate_basic_snapshot
    validate_basic_snapshot(data)
    require(data.get('provenance') == 'live' and data.get('end_column_verified') is True
            and data.get('end_reference_verified') is True)
    raw=json.dumps(data,allow_nan=False,sort_keys=True,separators=(',',':')).encode()
    require(len(raw)<=MAX_JSON)
    return raw

def checked_png(raw):
    require(len(raw)<=MAX_IMAGE and raw.startswith(b'\x89PNG\r\n\x1a\n'))
    pos=8; types=[]
    while pos<len(raw):
        require(pos+12<=len(raw)); n=int.from_bytes(raw[pos:pos+4],'big'); typ=raw[pos+4:pos+8]
        require(typ in (b'IHDR',b'IDAT',b'IEND') and pos+n+12<=len(raw))
        types.append(typ);pos+=n+12
    require(types==[b'IHDR']+[b'IDAT']*(len(types)-2)+[b'IEND'] and len(types)>2)
    from io import BytesIO
    from PIL import Image
    with Image.open(BytesIO(raw)) as image:
        require(image.format=='PNG' and image.mode in ('RGB','RGBA') and 0<image.width<=1440 and 0<image.height<=1280)
        image.verify()
    return raw

def pack(data, image, run_id):
    require(str(UUID(run_id))==run_id)
    raw=checked_json(data); png=checked_png(image)
    require(data['source_sha256'] == hashlib.sha256(png).hexdigest())
    header=json.dumps(dict(run_id=run_id, png_sha256=hashlib.sha256(png).hexdigest(),
                           snapshot_sha256=hashlib.sha256(raw).hexdigest()),separators=(',',':')).encode()
    return struct.pack('!II',len(header),len(raw))+header+raw+png

def unpack(blob, run_id):
    require(8<len(blob)<=MAX_IMAGE+MAX_JSON+520)
    h,n=struct.unpack('!II',blob[:8]); require(0<h<=512 and 0<n<=MAX_JSON)
    data=json.loads(blob[8+h:8+h+n]);png=blob[8+h+n:]
    require(pack(data,png,run_id)==blob)
    return data,png

def safe_root():
    folder=Path('/tmp')/f'cg-native-btc-pilot-{os.getuid()}'
    folder.mkdir(mode=0o700,exist_ok=True)
    require_private_dir(folder)
    return folder

def require_private_dir(folder):
    st=folder.lstat()
    require(stat.S_ISDIR(st.st_mode) and st.st_uid==os.getuid() and stat.S_IMODE(st.st_mode) in (0o700,0o2700))

def bounded_read(file, limit):
    with os.fdopen(os.open(file,os.O_RDONLY|os.O_NOFOLLOW),'rb') as stream:
        st=os.fstat(stream.fileno()); require(stat.S_ISREG(st.st_mode) and st.st_uid==os.getuid() and stat.S_IMODE(st.st_mode)==0o600 and 0<st.st_size<=limit)
        data=stream.read(limit+1); require(len(data)==st.st_size); return data

def write_once(directory,name,data):
    require_private_dir(directory)
    fd,tmp=tempfile.mkstemp(prefix='.pending-',dir=directory)
    try:
        with os.fdopen(fd,'wb') as out:
            out.write(data);out.flush();os.fsync(out.fileno())
        os.link(tmp,directory/name)
    finally:
        os.unlink(tmp)

def retain(run_id,blob):
    unpack(blob,run_id)
    write_once(safe_root(),run_id+'.map',blob)
    return hashlib.sha256(blob).hexdigest()
