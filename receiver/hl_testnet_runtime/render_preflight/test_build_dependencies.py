"""Offline contracts for the isolated build dependency; no downloads/builds."""
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import build_postgres as build


class BuildDependenciesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='preflight-build-tests-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_flex_requires_both_digests(self):
        archive = self.root / 'fixture.tar.gz'
        raw = b'local fixture, never extracted'
        archive.write_bytes(raw)
        sha256, sha1 = hashlib.sha256(raw).hexdigest(), hashlib.sha1(raw).hexdigest()
        with patch.object(build, 'FLEX_SHA256', sha256), patch.object(build, 'FLEX_SHA1', sha1):
            build.verify_flex_source(archive)
        for bad256, bad1 in (('0' * 64, sha1), (sha256, '0' * 40)):
            with patch.object(build, 'FLEX_SHA256', bad256), patch.object(build, 'FLEX_SHA1', bad1):
                with self.assertRaisesRegex(RuntimeError, 'FLEX_SOURCE_CHECKSUM_MISMATCH'):
                    build.verify_flex_source(archive)

    def test_flex_size_limit_precedes_digest(self):
        archive = self.root / 'fixture.tar.gz'
        archive.write_bytes(b'12345')
        with patch.object(build, 'MAX_FLEX_BYTES', 4):
            with self.assertRaisesRegex(RuntimeError, 'FLEX_SOURCE_TOO_LARGE'):
                build.verify_flex_source(archive)

    def test_only_flex_executable_target_is_built(self):
        commands = build.flex_commands(self.root / 'tools')
        self.assertEqual(commands[0], ['./configure', '--prefix=' + str(self.root / 'tools'),
                                       '--disable-shared', '--disable-nls'])
        self.assertEqual(commands[1], ['make', '-C', 'src', '-j2', 'flex'])
        self.assertNotIn('install', commands[1])
        self.assertNotIn('doc', str(commands))

    def test_missing_m4_fails_before_download_or_build(self):
        with patch.object(build.shutil, 'which', return_value=None), \
                patch.object(build, 'download') as download, patch.object(build.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'FLEX_BUILD_REQUIRES_M4'):
                build.ensure_flex(self.root)
            download.assert_not_called()
            run.assert_not_called()

    def test_corrupt_flex_archive_never_extracts_or_builds(self):
        def fixture(_url, path, _limit):
            path.write_bytes(b'wrong bytes')
        with patch.object(build.shutil, 'which', return_value='/usr/bin/m4'), \
                patch.object(build, 'download', side_effect=fixture), \
                patch.object(build.tarfile, 'open') as extract, patch.object(build.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'FLEX_SOURCE_CHECKSUM_MISMATCH'):
                build.ensure_flex(self.root)
            extract.assert_not_called()
            run.assert_not_called()

    def test_postgres_build_steps_all_receive_absolute_flex(self):
        root = self.root / 'project'
        root.mkdir()
        module_path = root / 'hl_testnet_runtime' / 'render_preflight' / 'build_postgres.py'
        flex = root / '.render_preflight_tools' / 'bin' / 'flex'
        def fixture(_url, path, _limit):
            path.write_bytes(b'fixture')
        class Archive:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def extractall(self, tmp, *, filter):
                self.assert_filter = filter
                (Path(tmp) / ('postgresql-' + build.VERSION)).mkdir()
        def run(command, **kwargs):
            self.assertEqual(kwargs['env']['FLEX'], str(flex.resolve()))
            self.assertEqual(kwargs['timeout'], 1200)
            self.assertTrue(kwargs['check'])
            if command == ['make', 'install']:
                (root / '.render_preflight_pg').mkdir()
        with patch.object(build, '__file__', str(module_path)), \
                patch.object(build, 'ensure_flex', return_value=flex), \
                patch.object(build, 'download', side_effect=fixture), \
                patch.object(build, 'verify_source'), patch.object(build.tarfile, 'open', return_value=Archive()), \
                patch.object(build.subprocess, 'run', side_effect=run) as calls:
            build.main()
        self.assertEqual([call.args[0] for call in calls.call_args_list],
                         build.postgres_commands(root / '.render_preflight_pg'))

    def test_download_is_size_bounded(self):
        with patch.object(build.urllib.request, 'urlopen', return_value=io.BytesIO(b'12345')):
            with self.assertRaisesRegex(RuntimeError, 'BUILD_SOURCE_TOO_LARGE'):
                build.download('https://example.invalid/fixture', self.root / 'fixture', 4)


if __name__ == '__main__':
    unittest.main()
