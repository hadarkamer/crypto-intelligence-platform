"""Acceptance of the packaged reader on original Firefox market-only captures."""
import errno
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from PIL import Image, ImageDraw
import modern_map_axis_ocr as ocr
from test_modern_map_axis import MARKS

FIXTURES=Path(__file__).parent/'fixtures'
BOX=(0,0,140,774)
PLOT=(0,0,65,773)


class PrintedPriceTests(TestCase):
    def read(self,image,marks=(),**kwargs):
        receipt={}
        result=ocr.axis_scale_live(image,BOX,PLOT,marks,observe=receipt.update,**kwargs)
        return result,receipt

    def test_packaged_engine_reads_real_market_tags_and_ticks_in_one_pass(self):
        for stamp,shown in [('1343','60,861'),('1500','60,677'),('1608','60,652'),('1841','60,588')]:
            with self.subTest(stamp=stamp),Image.open(FIXTURES/f'btc_12h_{stamp}_axis_edge.png') as image:
                scale,record=self.read(image)
                self.assertIsNotNone(scale,record)
                self.assertEqual((scale[0],record['reference'],record['ocr_calls'],record['exit_code']),
                                 (shown,shown,1,0))
                self.assertGreaterEqual(len(record['ticks']),11)

    def test_direct_source_or_chart_marks_skip_ocr_and_conflicts_abstain(self):
        with Image.open(FIXTURES/'btc_12h_1343_axis_edge.png') as im, patch.object(ocr,'_tsv') as engine:
            self.assertEqual(self.read(im,MARKS)[0][0],'60,861')
            self.assertEqual(self.read(im,MARKS[:-1],reference='60,861')[0][0],'60,861')
            self.assertIsNone(self.read(im,MARKS,reference='60,862')[0])
            self.assertIsNone(self.read(im,MARKS,reference='61,000')[0])
            engine.assert_not_called()

    def test_unreadable_reference_absent_ticks_or_reference_line_abstain(self):
        with Image.open(FIXTURES/'btc_12h_1500_axis_edge.png') as image:
            for rect,fill in [((66,387,139,407),(237,237,237)),
                              ((66,0,139,383),(20,22,26)),((0,390,65,405),(20,22,26))]:
                altered=image.copy()
                ImageDraw.Draw(altered).rectangle(rect,fill=fill)
                result,trace=self.read(altered)
                self.assertIsNone(result,trace)
                self.assertLessEqual(trace['ocr_calls'],1)

    def test_resized_translated_axis_and_a_single_missing_digit(self):
        with Image.open(FIXTURES/'btc_12h_1841_axis_edge.png') as image:
            enlarged=image.resize((175,968),Image.Resampling.LANCZOS)
            frame=Image.new('RGB',(220,1000))
            frame.paste(enlarged,(15,12))
            trace={}
            result=ocr.axis_scale_live(frame,(15,12,175,968),(15,12,97,979),[],observe=trace.update)
            # Double resampling materially distorts the small printed tag. The
            # OCR output must never be accepted unless it is the real quote.
            if result is not None:
                self.assertEqual(result[0],'60,588')
            else:
                self.assertNotEqual(trace['condition'],'verified')
            moved=Image.new('RGB',(180,806))
            moved.paste(image,(15,12))
            result=ocr.axis_scale_live(moved,(15,12,140,774),(15,12,80,785),[])
            self.assertEqual(result[0],'60,588')
            # Remove a printed digit while leaving the tag, dashed line and the
            # rest of the chart intact. Axis arithmetic must not fill the hole.
            hidden=image.copy()
            ImageDraw.Draw(hidden).rectangle((72,378,78,394),fill=(239,239,239))
            self.assertIsNone(self.read(hidden)[0])

    def test_cached_transcription_rechecks_current_price_line(self):
        with Image.open(FIXTURES/'btc_12h_1841_axis_edge.png') as image:
            cache={}
            first,receipt=self.read(image,cache=cache)
            self.assertEqual((first[0],receipt['ocr_calls']),('60,588',1))
            replay,again=self.read(image,cache=cache)
            self.assertEqual((replay[0],again['ocr_calls'],again['ocr_cache_hit']),('60,588',0,True))
            changed=image.copy()
            ImageDraw.Draw(changed).rectangle((0,378,65,395),fill=(20,22,26))
            failed,record=self.read(changed,cache=cache)
            self.assertIsNone(failed)
            self.assertEqual((record['ocr_calls'],record['condition']),(0,'reference_line_missing'))

    def test_ocr_process_is_killed_and_reaped_when_deadline_expires(self):
        with tempfile.TemporaryDirectory() as work:
            script=Path(work)/'stall'
            pidfile=Path(work)/'pid'
            script.write_text(f'#!/bin/sh\necho $$ > {pidfile}\nexec /bin/sleep 30\n')
            script.chmod(0o700)
            trace={}
            with patch.object(ocr,'BINARY',script):
                value,error=ocr._tsv(b'',.25,trace)
            self.assertEqual((value,error,trace['ocr_calls']),(None,'ocr_timed_out',1))
            self.assertEqual(trace['exit_code'],-9)
            with self.assertRaises(OSError) as caught:
                os.kill(int(pidfile.read_text()),0)
            self.assertEqual(caught.exception.errno,errno.ESRCH)

    def test_ocr_runtime_error_is_a_single_recorded_failure(self):
        trace={}
        with patch.object(ocr,'BINARY',Path('/missing/coinglass-ocr')):
            value,error=ocr._tsv(b'',2,trace)
        self.assertEqual((value,error,trace['ocr_calls']),(None,'ocr_process_failed',1))
        self.assertNotEqual(trace['exit_code'],0)
