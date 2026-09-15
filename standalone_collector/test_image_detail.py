"""Offline synthetic-image tests. No network, browser or account operations."""
from io import BytesIO
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from PIL import Image
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from image_detail import enlarge_png
import price_detail_input as detail
from model1_execution import StageFailure
G={'x':300,'y':200,'width':1100,'height':650,'page_width':1600}

class DetailTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'chart.png'
        with Image.new('RGB',(1600,1000),(70,10,80)) as image:
            image.putpixel((1100,500),(255,20,1));image.save(self.path)
    def tearDown(self):self.temp.cleanup()
    def record(self):return {'image':str(self.path),'price_detail':detail.make_detail_file(self.path,G)}
    def test_recent_candles_and_axis_rectangle(self):
        self.assertEqual(self.record()['price_detail']['crop_box'],[960,188,1520,862])
    def test_original_image_unchanged(self):
        before=self.path.read_bytes();self.record();self.assertEqual(self.path.read_bytes(),before)
    def test_exact_pixel_repetition(self):
        record=self.record()
        with Image.open(record['price_detail']['path']) as image:
            self.assertEqual(image.size,(1120,1348))
            for x in (280,281):
                for y in (624,625):self.assertEqual(image.getpixel((x,y)),(255,20,1))
    def test_out_of_bounds_rectangle_rejected(self):
        with self.assertRaises(ValueError):enlarge_png(self.path.read_bytes(),(-1,0,20,20))
    def test_non_png_rejected(self):
        with self.assertRaises(ValueError):enlarge_png(b'fixture',(0,0,20,20))
    def test_missing_geometry_rejected(self):
        with self.assertRaises(StageFailure):detail.make_detail_file(self.path,None)
    def test_invalid_numeric_geometry_rejected(self):
        for key,value in [('width',True),('y',9000),('page_width',float('nan'))]:
            with self.subTest(key=key),self.assertRaises(StageFailure):detail.make_detail_file(self.path,{**G,key:value})
    def test_original_hash_preserved(self):
        record=self.record()
        self.assertEqual(record['price_detail']['source_sha256'],hashlib.sha256(self.path.read_bytes()).hexdigest())
    def test_mismatched_source_rejected(self):
        record=self.record();record['price_detail']['source_sha256']='0'*64
        with self.assertRaises(StageFailure):detail.detail_content(record)
    def test_crop_tampering_rejected(self):
        record=self.record();Path(record['price_detail']['path']).write_bytes(b'fixture')
        with self.assertRaises(StageFailure):detail.detail_content(record)
    def test_single_auxiliary_view_and_one_scan_instruction(self):
        content=detail.detail_content(self.record())
        self.assertEqual([v['type'] for v in content],['input_text','input_image'])
        self.assertIn('ONE scan',content[0]['text'])
    def test_provenance_without_path(self):
        provenance=detail.detail_provenance(self.record())
        self.assertNotIn('path',provenance);self.assertEqual(provenance['scale'],2)
    def test_legacy_full_only_input(self):
        self.assertEqual(detail.detail_content({'image':'unused_fixture'}),[])
    def test_private_crop_file(self):
        record=self.record()
        self.assertEqual(Path(record['price_detail']['path']).stat().st_mode&0o777,0o600)

if __name__=='__main__':unittest.main()
