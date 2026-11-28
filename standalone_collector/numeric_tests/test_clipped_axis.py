"""Calibrate only from independently printed, unoccluded on-screen prices."""
from unittest import TestCase
from unittest.mock import patch

from PIL import Image, ImageDraw

import modern_map_axis_ocr as axis
from modern_map_checkpoint import _numeric_price_receipt


class ClippedAxisTests(TestCase):
    def setUp(self):
        self.image=Image.new('RGB',(180,191),(18,21,27))
        self.addCleanup(self.image.close)
        self.paint=ImageDraw.Draw(self.image)
        self.paint.rectangle((101,78,177,92),fill=(230,230,230))
        for x in range(0,98,8):
            self.paint.line((x,85,x+3,85),fill=(200,200,200))
        self.ticks=[self.word(str(value),190-10*value) for value in (2,5,9,12,15,18)]

    def word(self,text,y,width=8,height=8,confidence=95,left=5):
        # As in the real whole-axis TSV, these boxes are local to the crop at x=100.
        return axis.PrintedWord(text,left,y-height/2,width,height,confidence)

    def read(self,words,marks=(),pointer=None,cache=None,reference=None,tag='10.5'):
        def primary(strip,timeout,audit):
            audit['ocr_calls']+=1
            return words,''
        def local(png,timeout,audit,*,segmentation):
            self.assertEqual(segmentation,7)
            audit['ocr_calls']+=1
            return ('level\tleft\ttop\twidth\theight\tconf\ttext\n5\t36\t24\t88\t28\t97\t'+tag+'\n'),''
        receipt={}
        with patch.object(axis,'_ocr_marks',side_effect=primary),patch.object(axis,'_tsv',side_effect=local):
            scale=axis.axis_scale_live(self.image,(0,0,180,191),(0,0,99,190),marks,
                pointer_y=pointer,reference=reference,cache=cache,observe=receipt.update)
        return scale[0] if scale else None,receipt

    def test_only_one_physically_covered_low_confidence_tick_can_be_omitted(self):
        token=self.word('1.000',75,width=24,height=10,confidence=12)
        words=[*self.ticks,token,self.word('10.5',85)]
        result,receipt=self.read(words)
        self.assertEqual((result,receipt['ocr_calls']),('10.5',1))
        self.assertEqual([x['text'] for x in receipt['occluded_ticks']],['1.000'])
        real_decimal=self.word('11.50',75,width=24,height=10,confidence=12)
        _,receipt=self.read([*self.ticks,real_decimal,self.word('10.5',85)])
        self.assertEqual(receipt['condition'],'verified')
        self.assertNotIn('occluded_ticks',receipt)
        for other in (token._replace(confidence=95),token._replace(top=66),token._replace(text='1000')):
            with self.subTest(other=other):
                self.assertIsNone(self.read([*self.ticks,other,self.word('10.5',85)])[0])
        self.assertIsNone(self.read(words,marks=[('1.000',110,75)])[0])

    def test_blank_pointer_centre_and_all_neighbouring_ink_must_be_explained(self):
        self.paint.line((105,136,110,136),fill=(135,145,155))
        words=[*self.ticks,self.word('10.5',85)]
        def capture(w=words):
            return self.read(w,[('5.6',106,134)],134)
        result,receipt=capture()
        self.assertEqual(result,'10.5')
        self.assertEqual(receipt['pointer_evidence'][0]['reason'],'blank_pointer_row_adjacent_to_tick')
        self.assertIsNone(capture([*words,self.word('5.5',140)])[0])
        self.paint.point((143,136),fill=(135,145,155))
        self.assertIsNone(capture()[0])
        self.paint.point((143,136),fill=(18,21,27))
        self.paint.point((106,134),fill=(135,145,155))
        self.assertIsNone(capture()[0])

    def render_ink(self):
        # Three distinct foreground groups entirely in the white tag.
        for a,b in [(106,110),(115,118),(124,127)]:
            self.paint.rectangle((a,82,b,88),fill=(25,25,25))

    def test_adjacent_tick_antialias_connects_to_the_tick_without_crossing_the_pointer(self):
        words=[*self.ticks,self.word('10.5',85)]
        self.paint.line((107,136,107,139),fill=(140,150,160))
        price,receipt=self.read(words,[('5.6',106,135)],135)
        self.assertEqual(price,'10.5')
        self.assertEqual(receipt['pointer_evidence'][0]['printed'],'5')
        self.paint.point((110,136),fill=(100,110,120))
        self.assertIsNone(self.read(words,[('5.6',106,135)],135)[0])
        self.paint.point((110,136),fill=(18,21,27))
        self.paint.point((107,135),fill=(100,110,120))
        self.assertIsNone(self.read(words,[('5.6',106,135)],135)[0])

    def test_only_a_continuous_tag_rim_explains_nearby_ink(self):
        words=[*self.ticks,self.word('10.5',85)]
        self.paint.line((105,77,174,77),fill=(75,77,79))
        result,receipt=self.read(words,[('11.5',106,75)],75)
        self.assertEqual(result,'10.5')
        clean=_numeric_price_receipt(receipt|{'status':'ok'})
        self.assertEqual(clean['pointer_evidence'][0]['reason'],'blank_pointer_row_adjacent_to_tag')
        self.paint.point((136,77),fill=(18,21,27))
        self.assertIsNone(self.read(words,[('11.5',106,75)],75)[0])
        self.paint.point((136,77),fill=(75,77,79))
        self.paint.point((106,74),fill=(75,77,79))
        self.assertIsNone(self.read(words,[('11.5',106,75)],75)[0])

    def test_isolate_a_contaminating_outer_glyph_only_after_axis_rejection(self):
        self.render_ink()
        border=self.word('90.5',85,left=0,width=31,height=17,confidence=0)
        suspect=[*self.ticks,border]
        cache={}
        for reads in (2,0):
            price,receipt=self.read(suspect,cache=cache)
            self.assertEqual((price,receipt['ocr_calls']),('10.5',reads))
            self.assertEqual(receipt['tag_refinement']['reason'],'tag_border_in_word_box')
        for isolated in ('90.5','10.6','10','110.5'):
            with self.subTest(isolated=isolated):
                self.assertIsNone(self.read(suspect,tag=isolated)[0])
        self.assertIsNone(self.read(suspect,reference='10.3')[0])
        for changed in (border._replace(confidence=95),border._replace(left=4,width=26,top=80,height=11)):
            result,receipt=self.read([*self.ticks,changed])
            self.assertIsNone(result)
            self.assertEqual(receipt['ocr_calls'],1)
        # Do not spend another OCR call on a chart which already verifies.
        self.assertEqual(self.read([*self.ticks,border._replace(text='10.5')])[1]['ocr_calls'],1)

    def test_a_tight_crop_cannot_set_an_outer_digit_the_scale_cannot_distinguish(self):
        self.assertFalse(axis._outer_digit_is_unique('10.9','10.5',lambda px:190-10*px,85))
        self.assertTrue(axis._outer_digit_is_unique('90.5','10.5',lambda px:190-10*px,85))

    def test_local_word_covers_complete_ink_and_failed_reads_are_also_cached(self):
        self.render_ink()
        words=[*self.ticks,self.word('10',85)]
        for text,expected in [('10.5','10.5'),('7',None),('1',None)]:
            with self.subTest(text=text):
                cache={}
                price,first=self.read(words,cache=cache,tag=text)
                again,second=self.read(words,cache=cache,tag=text)
                self.assertEqual((price,again),(expected,expected))
                self.assertEqual((first['ocr_calls'],second['ocr_calls']),(2,0))
                self.assertTrue(second['tag_refinement']['cache_hit'])
                if expected is None:self.assertEqual(first['condition'],'reference_tag_incomplete')
        price,r=self.read(words,reference='10.317')
        self.assertIsNone(price)
        self.assertEqual(r['condition'],'reference_conflict')

    def test_unexplained_ink_touching_crop_is_never_accepted_as_partial(self):
        self.render_ink()
        self.paint.line((175,80,175,85),fill=(25,25,25))
        # At this scale a near prefix could otherwise pass the reference residual.
        result,receipt=self.read([*self.ticks,self.word('10.5',85)])
        self.assertIsNone(result)
        self.assertEqual(receipt['condition'],'reference_tag_incomplete')
        self.assertEqual(receipt['ocr_calls'],1)
        self.assertIn(receipt['tag_refinement']['status'],('ink_touches_crop','tag_shape_unconfirmed'))

    def test_split_tag_must_account_for_all_digits_in_spatial_order(self):
        self.render_ink()
        ticks=[w._replace(text=str(10*int(w.text))) for w in self.ticks]
        left=self.word('10',85,width=12,left=6)
        right=self.word('5',85,width=5,left=23)
        result,receipt=self.read([*ticks,right,left],tag='105')
        self.assertEqual((result,receipt['ocr_calls']),('105',2))
        self.assertEqual(receipt['tag_refinement']['reason'],'tag_words_split')
        for parts in ([left,right._replace(text='6')], [left._replace(text='1'),right],
                      [left._replace(left=20),right], [left._replace(left=50),right]):
            with self.subTest(parts=parts):
                self.assertIsNone(self.read([*ticks,*parts],tag='105')[0])
        self.assertIsNone(self.read([*ticks,right,left],tag='105',reference='106')[0])

    def test_missing_tag_needs_independent_glyphs_when_an_adjacent_price_fits(self):
        self.render_ink()
        # The line fits both 10.5 and 10.4. A reliable word box alone is insufficient
        # here: no independent printed tick glyphs are present in this fake image.
        price,receipt=self.read(self.ticks)
        self.assertIsNone(price)
        self.assertEqual(receipt['condition'],'reference_tag_ambiguous')
        public=_numeric_price_receipt(receipt|{'status':'failed'})
        self.assertEqual(public['tag_refinement']['reason'],'tag_word_missing')
        self.assertNotIn('partial',public['tag_refinement'])

    def test_obscured_integer_tick_requires_actual_clipped_strokes(self):
        words=[*self.ticks,self.word('1',76,width=27,height=8,confidence=1),self.word('10.5',85)]
        self.assertIsNone(self.read(words)[0])
        for x in (107,116,127):
            self.paint.rectangle((x,75,x+1,77),fill=(140,150,160))
        result,receipt=self.read(words)
        self.assertEqual((result,receipt['ocr_calls']),('10.5',1))
        self.assertEqual([a['text'] for a in receipt['occluded_ticks']],['1'])
        self.paint.line((101,77,177,77),fill=(18,21,27))
        self.assertIsNone(self.read(words)[0])

    def test_public_receipt_keeps_only_typed_market_provenance(self):
        receipt=_numeric_price_receipt(dict(status='failed',condition='pointer_price_unverified',
            pointer_evidence=[{'text':'1,661','x':5,'y':8,'reason':'blank_pointer_row_adjacent_to_tick','printed':'1,660'},
                              {'text':'oops','x':5,'y':8,'reason':'blank_pointer_row_adjacent_to_tick'}],
            occluded_ticks=[{'text':'60.000','x':6,'y':4},{'text':'private','x':0,'y':0}],
            tag_refinement={'status':'read','partial':'1','text':'1,653','confidence':96.5,'cache_hit':False,'unused':'drop'}))
        self.assertEqual(len(receipt['pointer_evidence']),1)
        self.assertEqual(receipt['occluded_ticks'],[{'text':'60.000','x':6,'y':4}])
        self.assertNotIn('unused',receipt['tag_refinement'])
