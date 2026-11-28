"""Run against the assembled numerical runtime on PYTHONPATH."""
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw
import modern_map_axis_ocr as ocr
import modern_map_basic as basic


class PriceAxisProvenanceTests(TestCase):
    box = (0, 0, 180, 191)
    bounds = (0, 0, 99, 190)

    def setUp(self):
        self.image = Image.new('RGB', (180, 191), (18, 21, 27))
        self.addCleanup(self.image.close)
        draw = ImageDraw.Draw(self.image)
        draw.rectangle((101, 78, 177, 92), fill=(230, 230, 230))
        for x in range(0, 98, 8):
            draw.line((x, 85, x + 3, 85), fill=(200, 200, 200))
        # Native scale y = 190 - 10*price. Word boxes are local to the strip.
        self.words = [self.word(str(price), 190 - 10 * price) for price in (2, 5, 9, 12, 15, 18)]
        self.words.append(self.word('10.5', 85, confidence=6))
        # Render actual pointer-off foreground at the price-9 tick.
        draw.line((106, 99, 110, 101), fill=(125, 135, 145))

    def word(self, text, y, *, width=8, height=8, left=5, confidence=95):
        return ocr.PrintedWord(text, left, y - height / 2, width, height, confidence)

    def read(self, marks=(), *, words=None, pointer_y=None, cache=None):
        receipt = {}
        def inspect(strip, timeout, audit):
            audit['ocr_calls'] = 1
            return list(self.words if words is None else words), ''
        with patch.object(ocr, '_ocr_marks', side_effect=inspect):
            scale = ocr.axis_scale_live(self.image, self.box, self.bounds, marks, pointer_y=pointer_y,
                                        cache=cache, observe=receipt.update)
        return (scale[0] if scale else None), receipt

    def test_clipped_tag_fragment_and_full_single_digit_have_separate_evidence(self):
        # The tiny sliver is adjacent to the opaque band; the real 9 is full height.
        fragment = self.word('9', 93.75, width=30, height=2, confidence=0)
        price, audit = self.read(words=[*self.words, fragment])
        self.assertEqual(price, '10.5')
        self.assertEqual([w['text'] for w in audit['tag_occluded_fragments']], ['9'])
        self.assertIn({'text': '9', 'y': 100.0}, audit['ticks'])
        intact = self.word('9', 120)
        self.assertEqual(self.read(words=[*self.words, intact])[1]['condition'], 'tick_position_conflict')
        uncovered = fragment._replace(left=60, width=17)
        ImageDraw.Draw(self.image).rectangle((160, 89, 179, 93), fill=(18, 21, 27))
        self.assertEqual(self.read(words=[*self.words, uncovered])[1]['condition'], 'tick_position_conflict')

    def test_pointer_off_pixels_discriminate_transient_fixed_and_ambiguous_rows(self):
        price, audit = self.read([('1.11', 105, 140)], pointer_y=140)
        self.assertEqual(price, '10.5')
        self.assertEqual(audit['pointer_evidence'][0]['reason'], 'absent_on_pointer_off_axis')
        price, audit = self.read([('9', 105, 100)], pointer_y=100)
        self.assertEqual(price, '10.5')
        self.assertEqual(audit['pointer_evidence'][0]['reason'], 'fixed_label_on_pointer_off_axis')
        price, audit = self.read([('8.6', 105, 100)], pointer_y=100)
        self.assertEqual(price, '10.5')
        self.assertEqual(audit['pointer_evidence'][0]['printed'], '9')
        # The same contrary read has no authority when the active pointer was elsewhere.
        self.assertEqual(self.read([('8.6', 105, 100)], pointer_y=132)[1]['condition'], 'axis_position_conflict')
        for other in (self.word('9', 100, confidence=5), self.word('8.5', 100)):
            words = ([w for w in self.words if w.text != '9'] if other.confidence == 5 else self.words)
            with self.subTest(other=other):
                self.assertEqual(self.read([('8.6', 105, 100)], pointer_y=100, words=[*words, other])[1]
                                 ['condition'], 'pointer_price_unverified')
        self.assertEqual(self.read([('8.6', 105, 85)], pointer_y=85)[1]['condition'], 'reference_conflict')

    def test_cache_only_reuses_word_boxes_and_revalidates_current_pixels(self):
        cache = {}
        price, first = self.read(cache=cache)
        again, second = self.read(cache=cache)
        self.assertEqual((price, again, first['ocr_calls'], second['ocr_calls']), ('10.5', '10.5', 1, 0))
        self.assertTrue(second['ocr_cache_hit'])
        ImageDraw.Draw(self.image).line((0, 85, 98, 85), fill=(18, 21, 27))
        missing, third = self.read(cache=cache)
        self.assertIsNone(missing)
        self.assertEqual((third['condition'], third['ocr_calls']), ('reference_line_missing', 0))

    def test_normal_reader_passes_only_the_actual_successful_pointer_height(self):
        stamp = '11/26 16:05'
        source = datetime(2026, 11, 26, 16, 8, tzinfo=timezone.utc)
        args = (Mock(), self.box, self.bounds, source, Mock(), Mock())
        kw = dict(base_only=True, period='5m', opening=True)
        position = {}
        sample={'pointer_y':95}
        prior={'marks':[]}
        current={'stamp':stamp,'marks':[]}
        with patch.object(basic,'read_cursor_pair',return_value=(prior,current,sample)):
            basic.displayed_column(*args,pointer_position=position,timeframe='12H',**kw)
        self.assertEqual(position['y'],sample['pointer_y'])
        position.clear()
        with patch.object(basic,'read_cursor_pair',side_effect=basic.StageFailure('source_not_readable')):
            with self.assertRaises(basic.StageFailure):
                basic.displayed_column(*args,pointer_position=position,timeframe='12H',**kw)
        self.assertEqual(position,{})
