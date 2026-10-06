from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
import unittest

import research_archive_admin as admin


class ArchiveAdminTests(unittest.TestCase):
    def test_upload_identity_is_strict_and_route_defaults_closed(self):
        self.assertEqual(admin.MAX_CHUNK_BYTES, 8 * 1024 * 1024)
        self.assertFalse(admin.ENABLED and not admin.SECRET)
        self.assertTrue(admin._HEX64.fullmatch("a" * 64))
        self.assertIsNone(admin._HEX64.fullmatch("../archive"))

    def test_sha256_stream(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x"
            path.write_bytes(b"reviewed")
            self.assertEqual(admin._sha256(path), hashlib.sha256(b"reviewed").hexdigest())


if __name__ == "__main__":
    unittest.main()
