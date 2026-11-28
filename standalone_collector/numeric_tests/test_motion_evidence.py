import hashlib
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from PIL import Image
from modern_map_checkpoint import retain_motion_evidence


def png(size, colour):
    with Image.new('RGB', size, colour) as image, BytesIO() as stream:
        image.save(stream, format='PNG')
        return stream.getvalue()


class MotionEvidenceTests(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'image.png'
        self.original = png((18, 20), 'navy')
        self.path.write_bytes(self.original)
        self.frames = {state: dict(phase='opening', column_time='2026-11-27T12:00:00Z',
            clip_css=[14, 3, 4, 8], pointer_css=[13, y], png=png((4, 8), colour),
            marks=[dict(text='0.83', x=15, y=8), dict(text='unrelated page label', x=15, y=7)])
            for state, y, colour in [('y1', 8, 'red'), ('y2', 11, 'green'), ('off', 0, 'blue')]}

    def test_current_crops_are_recoverable_in_order_and_original_pixels_are_preserved(self):
        receipt = retain_motion_evidence(self.path, self.frames)
        self.assertEqual(receipt['original_png_sha256'], hashlib.sha256(self.original).hexdigest())
        with Image.open(self.path) as sheet, Image.open(BytesIO(self.original)) as original:
            self.assertEqual(sheet.crop((0, 0, *original.size)).tobytes(), original.tobytes())
            self.assertEqual([m['state'] for m in receipt['frames']], ['y1','y2','off'])
            for m in receipt['frames']:
                x, y, w, h = m['panel_xywh']
                with sheet.crop((x, y, x+w, y+h)) as panel:
                    self.assertEqual(hashlib.sha256(panel.tobytes()).hexdigest(), m['rgb_sha256'])
                self.assertEqual(m['marks'], [dict(text='0.83', x=15, y=8)])

    def test_cross_column_frames_cannot_modify_the_original(self):
        self.frames['off']['column_time'] = '2026-11-27T12:05:00Z'
        self.assertIsNone(retain_motion_evidence(self.path, self.frames))
        self.assertEqual(self.path.read_bytes(), self.original)
        self.assertIsNone(retain_motion_evidence(self.path, {}))
