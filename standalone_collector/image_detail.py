"""Pure image transformation: preserve an original PNG and magnify a rectangle.

No browser, network, authentication or application data operations.
Nearest-neighbour scaling repeats existing pixels; it does not generate detail.
"""
from __future__ import annotations
from io import BytesIO
import hashlib
from PIL import Image

MAX_BYTES=4*1024*1024
MAX_PIXELS=16_000_000


def enlarge_png(source: bytes, box: tuple[int,int,int,int]) -> tuple[bytes,dict]:
    if not isinstance(source,bytes) or len(source)>MAX_BYTES:
        raise ValueError('Invalid image size')
    if not source.startswith(b'\x89PNG\r\n\x1a\n'):
        raise ValueError('PNG required')
    if not isinstance(box,(list,tuple)) or len(box)!=4 or any(type(v) is not int for v in box):
        raise ValueError('Integer crop rectangle required')
    left,top,right,bottom=box
    with Image.open(BytesIO(source)) as original:
        if original.format!='PNG' or original.width*original.height>MAX_PIXELS:
            raise ValueError('Image exceeds pixel limit')
        if not (0<=left<right<=original.width and 0<=top<bottom<=original.height):
            raise ValueError('Crop must be inside original image')
        if (right-left)*(bottom-top)*4>8_000_000:
            raise ValueError('Enlarged image exceeds pixel limit')
        with original.crop(box) as crop:
            with crop.resize((crop.width*2,crop.height*2),Image.Resampling.NEAREST) as detail:
                target=BytesIO();detail.save(target,format='PNG');result=target.getvalue()
                dimensions=list(detail.size)
    if len(result)>MAX_BYTES:
        raise ValueError('Enlarged PNG exceeds byte limit')
    return result,{'source_sha256':hashlib.sha256(source).hexdigest(),
        'sha256':hashlib.sha256(result).hexdigest(),'crop_box':list(box),
        'scale':2,'dimensions':dimensions,'method':'nearest-neighbour'}
