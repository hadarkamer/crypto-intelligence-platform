"""Synthetic offline regressions. No credentials, network, browser or model."""
import copy
import hashlib
from io import BytesIO
import unittest
from PIL import Image, ImageDraw
from edge_zones import (extract_current_cores, EvidenceError, Policy, calibrate,
                        audit_saved_zones, public_report)

PLOT=(80,20,480,340)
LEGEND=(30,20,45,340)
ANCHORS=[(40,79000),(140,78000),(240,77000),(330,76100)]
# Known fixture truth: price(y)=79400-10*y.
IDENTITY={'symbol':'BTC','model':1,'timeframe':'12H'}
STOPS=[(68,1,84),(59,82,139),(33,145,140),(94,201,98),(242,232,5)]

def color(level):
    at=max(0,min(0.999999,level))*4; i=int(at); f=at-i
    return tuple(round(a*(1-f)+b*f) for a,b in zip(STOPS[i],STOPS[i+1]))

def fixture(bands=(), changes=None):
    im=Image.new('RGB',(540,370),'white'); d=ImageDraw.Draw(im)
    d.rectangle((80,20,479,339), fill=color(0))
    for y in range(20,340):d.line((30,y,44,y),fill=color((339-y)/319))
    for top,bottom,level,start,end in bands:
        d.rectangle((start,top,end-1,bottom-1),fill=color(level))
    if changes:changes(im,d)
    out=BytesIO();im.save(out,format='PNG');return out.getvalue()

def run(raw,**kwargs):
    opts=dict(expected_sha256=hashlib.sha256(raw).hexdigest(),plot=PLOT,legend=LEGEND,
              anchors=ANCHORS,price_interval=[77350,77400],identity=IDENTITY)
    opts.update(kwargs);return extract_current_cores(raw,**opts)

class Tests(unittest.TestCase):
    def test_real_core_comes_from_pixels_not_suggested_price(self):
        raw=fixture([(100,108,.95,100,480)])
        b=run(raw)['bands'][0]
        self.assertEqual(b['pixel_y'],[100,108]);self.assertEqual(b['side'],'above')
        self.assertEqual((b['price_low'],b['price_high']),(78300,78450))
        self.assertEqual(b['intensity'],'many')
    def test_historical_bright_band_is_excluded(self):
        self.assertEqual(run(fixture([(100,108,.99,100,420)]))['bands'],[])
    def test_near_right_but_missing_last_column_is_rejected(self):
        self.assertEqual(run(fixture([(100,108,.99,100,477)]))['bands'],[])
    def test_separated_bands_do_not_bridge_dark_gap(self):
        bs=run(fixture([(100,108,.95,100,480),(113,118,.95,100,480)]))['bands']
        self.assertEqual(sorted(b['pixel_y'] for b in bs),[[100,108],[113,118]])
    def test_peak_core_distinct_from_surrounding_envelope(self):
        bs=run(fixture([(90,120,.5,100,480),(100,108,.95,100,480)]))['bands']
        self.assertEqual(bs[0]['pixel_y'],[100,108]);self.assertEqual(bs[0]['envelope_pixel_y'],[90,120])
    def test_bright_distant_band_is_not_omitted_by_old_model_topn(self):
        bs=run(fixture([(40,48,.95,100,480),(100,108,.95,100,480),(265,273,.98,100,480)]))['bands']
        self.assertEqual(len(bs),3);self.assertEqual(sum(b['intensity']=='many' for b in bs),3)
    def test_candlestick_only_is_not_horizontal_liquidity(self):
        self.assertEqual(run(fixture([(100,125,.85,475,480)]))['bands'],[])
    def test_white_grid_is_not_palette(self):
        raw=fixture(changes=lambda im,d:d.line((80,120,479,120),fill=(220,220,220)))
        self.assertEqual(run(raw)['bands'],[])
    def test_red_candle_is_not_palette(self):
        raw=fixture(changes=lambda im,d:d.rectangle((400,110,479,120),fill=(225,55,82)))
        self.assertEqual(run(raw)['bands'],[])
    def test_current_price_overlap_is_omitted(self):
        m=run(fixture([(198,208,.95,100,480)]));self.assertFalse(m['bands']);self.assertEqual(len(m['omitted']),1)
    def test_medium_palette_is_not_many(self):
        b=run(fixture([(100,108,.65,100,480)]))['bands'][0]
        self.assertEqual(b['intensity'],'normal')
    def test_weak_visible_palette_is_retained_as_few(self):
        b=run(fixture([(100,108,.45,100,480)]))['bands'][0]
        self.assertEqual(b['intensity'],'few')
    def test_one_pixel_uncertain_line_not_promoted(self):
        self.assertFalse(run(fixture([(100,101,.99,100,480)]))['bands'])
    def test_sha_binding_rejects_other_image(self):
        with self.assertRaisesRegex(EvidenceError,'hash'):run(fixture(),expected_sha256='0'*64)
    def test_geometry_outside_source_rejected(self):
        with self.assertRaises(EvidenceError):run(fixture(),plot=(80,20,600,340))
    def test_missing_axis_anchors_rejected(self):
        with self.assertRaises(EvidenceError):run(fixture(),anchors=ANCHORS[:2])
    def test_reversed_prices_rejected(self):
        with self.assertRaises(EvidenceError):run(fixture(),anchors=[(40,76000),(140,77000),(240,78000)])
    def test_nonlinear_or_misread_tick_rejected(self):
        bad=copy.deepcopy(ANCHORS);bad[1]=(140,78300)
        with self.assertRaisesRegex(EvidenceError,'linear'):run(fixture(),anchors=bad)
    def test_boolean_nan_prices_rejected(self):
        for val in (True,float('nan'),float('inf')):
            with self.subTest(value=str(val)),self.assertRaises(EvidenceError):run(fixture(),price_interval=[val,77400])
    def test_wrong_palette_rejected(self):
        def c(im,d):d.rectangle((30,20,44,339),fill='white')
        with self.assertRaisesRegex(EvidenceError,'palette'):run(fixture(changes=c))
    def test_obscured_edge_rejected(self):
        def c(im,d):d.rectangle((435,20,479,339),fill='white')
        with self.assertRaisesRegex(EvidenceError,'readable'):run(fixture(changes=c))
    def test_scaled_geometry_has_same_price_interval(self):
        raw=fixture([(100,108,.95,100,480)]);m=run(raw)
        im=Image.open(BytesIO(raw)).resize((1080,740),Image.Resampling.NEAREST)
        out=BytesIO();im.save(out,format='PNG')
        n=run(out.getvalue(),plot=tuple(v*2 for v in PLOT),legend=tuple(v*2 for v in LEGEND),
              anchors=[(y*2,p) for y,p in ANCHORS],policy=Policy(stripe_width=40,terminal_width=8,border_inset=2))
        self.assertEqual(m['bands'][0]['price_low'],n['bands'][0]['price_low'])
        self.assertEqual(m['bands'][0]['price_high'],n['bands'][0]['price_high'])
    def test_audit_unsupported_saved_range_is_explicit(self):
        m=run(fixture([(100,108,.95,100,480)]))
        z=[{'side':'above','price_low':78000,'price_high':78100,'intensity':'many'}]
        self.assertEqual(audit_saved_zones(m,z)[0]['verdict'],'unsupported_current_edge')
    def test_audit_mixed_band_reports_problem_not_new_value(self):
        m=run(fixture([(100,108,.6,100,480)]))
        z=[{'side':'above','price_low':78300,'price_high':78450,'intensity':'many'}]
        r=audit_saved_zones(m,z)[0]
        self.assertEqual(r['verdict'],'no_high_intensity_support');self.assertEqual(r['price_low'],78300)
    def test_unsupported_identity_fails(self):
        for ident in (dict(IDENTITY,model=4),dict(IDENTITY,model=True),dict(IDENTITY,timeframe='1H'),dict(IDENTITY,symbol='ETH')):
            with self.assertRaises(EvidenceError):run(fixture(),identity=ident)
    def test_model_identity_propagates_no_new_map_assumption(self):
        for model in (1,2,3):
            for tf in ('12H','24H','48H'):
                ident=dict(IDENTITY,model=model,timeframe=tf);m=run(fixture(),identity=ident)
                self.assertEqual(m['identity'],ident)
    def test_never_mutates_input_or_claims_calls(self):
        raw=fixture([(100,108,.95,100,480)]);digest=hashlib.sha256(raw).hexdigest();m=run(raw)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),digest)
        self.assertEqual((m['scan_calls'],m['model_calls'],m['sheet_writes']),(0,0,0))
        self.assertNotIn('_profile',public_report(m))
    def test_bad_threshold_configuration_rejected(self):
        with self.assertRaises(EvidenceError):run(fixture(),policy=Policy(min_support=.1))

if __name__=='__main__':unittest.main()
