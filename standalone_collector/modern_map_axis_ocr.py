"""Read only the price axis and last-price tag of the supported Firefox chart font."""
from functools import lru_cache
from itertools import groupby
import re

from PIL import Image, ImageDraw

from modern_map_axis import axis_scale

# Row masks of printed digits, drawn from filtered market images on 24 and 26
# November. The two charts use the same small price font. There are no price
# strings, positions or monetary values in the alphabet. Unknown masks abstain.
_GLYPHS = {
    '0': '5x9:70f09898989898f070 5x9:70f89898989898f870 5x9:70f8c8c8c8c8c8f870 6x9:7878cccccccccc7878',
    '1': '3x9:60e0a0202020202020 3x9:60e0e0606060606060',
    '2': '5x9:70f8c808183060e0f8',
    '3': '5x9:70f88818381888f870',
    '4': '6x9:1030306060e8fc7c0c',
    '5': '5x9:f8c080f0d81898f8f0',
    '6': '5x9:206040f0d89898f070 5x9:206060c0f0d098f070 5x9:30206040f0d898f870',
    '7': '5x9:f89890303020604040 5x9:f8d818181030302060',
    '8': '5x9:70d898f0f09888d870 5x9:70d8c8d878d8c8d870',
    '9': '5x9:70d8c8d87810303060',
}


def _bits(mask):
    bitmap = mask.resize((12,18), Image.Resampling.NEAREST).convert('1')
    return int.from_bytes(bitmap.tobytes(),'big')


@lru_cache(maxsize=1)
def _alphabet():
    out = {}
    for digit, examples in _GLYPHS.items():
        masks = []
        for example in examples.split():
            shape, pixels = example.split(':')
            size = tuple(map(int,shape.split('x')))
            masks.append(_bits(Image.frombytes('1',size,bytes.fromhex(pixels))))
        out[digit] = masks
    return out


def _runs(flags):
    for state, items in groupby(enumerate(flags),key=lambda pair:pair[1]):
        if state:
            indices = [i for i,_ in items]
            yield indices[0],indices[-1]+1


def _printed(mask):
    pieces=[]
    for left,right in _runs(mask.crop((x,0,x+1,mask.height)).getbbox() is not None for x in range(mask.width)):
        col=mask.crop((left,0,right,mask.height))
        bounds=col.getbbox()
        pieces.append((col.crop(bounds),bounds[1],bounds[3]))
    if not 5 <= len(pieces) <= 12:
        return None
    baseline=max((bottom for g,_,bottom in pieces if g.height>=7), default=0)
    text=''
    for glyph,top,bottom in pieces:
        if glyph.height<=4 and glyph.width<=2 and bottom>baseline and top>=baseline-3:
            text+=','
            continue
        # The current chart font has nine-pixel numerals. Do not force text
        # in a different font, zoom, style or layout through the decoder.
        if glyph.height!=9 or not 2<=glyph.width<=7 or abs(bottom-baseline)>1:
            return None
        probe=_bits(glyph)
        ranked=sorted((min((probe ^ ref).bit_count() for ref in refs),digit)
                      for digit,refs in _alphabet().items())
        if ranked[0][0]>8 or ranked[1][0]-ranked[0][0]<7:
            return None
        text+=ranked[0][1]
    return text if re.fullmatch(r'[1-9]\d{0,2}(?:,\d{3})+',text) else None


def axis_scale_live(image,box,bounds,marks):
    """Prefer DOM/Canvas marks and use the clean price pixels only when needed."""
    left,top,right,bottom=bounds
    end=min(image.width,box[0]+box[2])
    if not (0<=left<right<end<=image.width and 0<=top<bottom<image.height and end-right>=14):
        return None
    if image.mode!='RGB':
        image=image.convert('RGB')
    if marks:
        scale=axis_scale(image,box,bounds,marks)
        if scale is not None:
            return scale
    if end-right>150:
        return None
    # Work only on the axis to the right of the verified plot. The source's
    # light tag is distinct from its light-on-dark ordinary price ticks.
    strip=image.crop((right+1,top,end,bottom+1)).convert('L')
    bright=strip.point(lambda p:255 if p>178 else 0)
    light_rows=[bright.crop((0,y,strip.width,y+1)).histogram()[255]>strip.width*.35 for y in range(strip.height)]
    bands=[(a,b) for a,b in _runs(light_rows) if 5<=b-a<=min(45,strip.height*.15)]
    if len(bands)!=1:
        return None
    y0,y1=bands[0]
    visible=bright.crop((0,y0,strip.width,y1)).getbbox()
    x0,x1=visible[0]+3,visible[2]-3
    if x0>=x1 or y1-y0<10:
        return None
    tag=strip.crop((x0,y0+3,x1,y1-3)).point(lambda p:255 if p<120 else 0)
    reference=_printed(tag)
    if not reference:
        return None
    mid_x=(right+1+end)/2
    decoded=[(reference,mid_x,top+(y0+y1-1)/2)]
    ink=strip.point(lambda p:255 if p>66 else 0)
    ImageDraw.Draw(ink).rectangle((0,y0-3,strip.width,y1+3), fill=0)
    for a,b in _runs(ink.crop((0,y,ink.width,y+1)).getbbox() is not None for y in range(ink.height)):
        if not 8<=b-a<=14:
            continue
        raw=_printed(ink.crop((0,a,ink.width,b)))
        if raw:
            # Tick centres use the printed digit baseline, excluding the comma's tail.
            decoded.append((raw,mid_x,top+a+4.5))
    # The original verifier requires ≥4 linear ticks spanning ≥60% of the
    # plot, the independently printed tag at its predicted y, and a dashed
    # plot line there. Any contradictory retained DOM mark remains decisive.
    return axis_scale(image,box,bounds,[*marks,*decoded])
