"""Build an unmodified, checksum-pinned PostgreSQL for isolated Render tests.

This is a build-time dependency only. It neither initializes a database nor
connects to any database, and never runs from the production bot entrypoint.
"""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.request

VERSION = '18.6'
SOURCE_URL = 'https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.bz2'
SOURCE_SHA256 = '555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f'
MAX_SOURCE_BYTES = 32 * 1024 * 1024
FLEX_VERSION = '2.6.4'
FLEX_URL = 'https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz'
FLEX_SHA256 = 'e87aae032bf07c26f85ac0ed3250998c37621d95f8bd748b31f15b33c45ee995'
FLEX_SHA1 = 'fafece095a0d9890ebd618adb1f242d8908076e1'
MAX_FLEX_BYTES = 8 * 1024 * 1024


def download(url, destination, maximum):
    deadline = time.monotonic() + 120
    with urllib.request.urlopen(url, timeout=60) as response, destination.open('wb') as output:
        total = 0
        while block := response.read(1024 * 1024):
            total += len(block)
            if total > maximum:
                raise RuntimeError('BUILD_SOURCE_TOO_LARGE')
            if time.monotonic() > deadline:
                raise RuntimeError('BUILD_SOURCE_DOWNLOAD_TIMEOUT')
            output.write(block)


def verify_source(path):
    if path.stat().st_size > MAX_SOURCE_BYTES:
        raise RuntimeError('POSTGRES_SOURCE_TOO_LARGE')
    if hashlib.sha256(path.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise RuntimeError('POSTGRES_SOURCE_CHECKSUM_MISMATCH')


def verify_flex_source(path):
    if path.stat().st_size > MAX_FLEX_BYTES:
        raise RuntimeError('FLEX_SOURCE_TOO_LARGE')
    raw = path.read_bytes()
    if (hashlib.sha256(raw).hexdigest() != FLEX_SHA256
            or hashlib.sha1(raw).hexdigest() != FLEX_SHA1):
        raise RuntimeError('FLEX_SOURCE_CHECKSUM_MISMATCH')


def flex_commands(prefix):
    return [
        ['./configure', '--prefix=' + str(prefix), '--disable-shared', '--disable-nls'],
        ['make', '-C', 'src', '-j2', 'flex'],
    ]


def ensure_flex(root):
    """Build only the verified scanner executable, never documentation/install targets."""
    m4 = shutil.which('m4')
    if not m4:
        raise RuntimeError('FLEX_BUILD_REQUIRES_M4')
    prefix = root / '.render_preflight_tools'
    executable = prefix / 'bin' / 'flex'
    marker = prefix / 'verified_flex_sources'
    expected_marker = FLEX_SHA256 + '\n' + FLEX_SHA1 + '\n'
    if executable.is_file() and os.access(executable, os.X_OK) and marker.is_file():
        if marker.read_text() == expected_marker:
            check = subprocess.run([str(executable), '--version'], check=True,
                                   capture_output=True, text=True, timeout=10)
            if check.stdout.strip() == 'flex ' + FLEX_VERSION:
                return executable
    with tempfile.TemporaryDirectory(prefix='render-preflight-flex-build-') as tmp:
        archive = Path(tmp) / 'flex.tar.gz'
        download(FLEX_URL, archive, MAX_FLEX_BYTES)
        verify_flex_source(archive)
        with tarfile.open(archive) as source:
            source.extractall(tmp, filter='data')
        source_dir = Path(tmp) / ('flex-' + FLEX_VERSION)
        build_env = {**os.environ, 'M4': str(Path(m4).resolve())}
        for command in flex_commands(prefix):
            subprocess.run(command, cwd=source_dir, env=build_env, check=True, timeout=600)
        built = source_dir / 'src' / 'flex'
        check = subprocess.run([str(built), '--version'], env=build_env, check=True,
                               capture_output=True, text=True, timeout=10)
        if check.stdout.strip() != 'flex ' + FLEX_VERSION:
            raise RuntimeError('FLEX_BUILD_VERSION_MISMATCH')
        executable.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(built, executable)
        executable.chmod(0o755)
        marker.write_text(expected_marker)
    print('PREFLIGHT_FLEX_BUILD: verified source ' + FLEX_VERSION, flush=True)
    return executable


def postgres_commands(prefix):
    return [
        ['./configure', '--prefix=' + str(prefix), '--without-readline',
         '--without-zlib', '--without-icu', '--without-lz4', '--without-zstd',
         '--without-ssl'],
        ['make', '-j2'],
        ['make', 'install'],
    ]


def main():
    root = Path(__file__).resolve().parents[2]
    prefix = root / '.render_preflight_pg'
    existing = prefix / 'bin' / 'postgres'
    marker = prefix / 'verified_source_sha256'
    required = ('postgres', 'initdb', 'pg_ctl', 'createdb')
    complete = all((prefix / 'bin' / name).is_file()
                   and os.access(prefix / 'bin' / name, os.X_OK) for name in required)
    if complete and marker.is_file() and marker.read_text().strip() == SOURCE_SHA256:
        check = subprocess.run([str(existing), '--version'], check=True,
                               capture_output=True, text=True, timeout=10)
        if check.stdout.strip().endswith(VERSION):
            print('PREFLIGHT_POSTGRES_BUILD: cached verified source ' + VERSION, flush=True)
            return
    flex = ensure_flex(root)
    build_env = {**os.environ, 'FLEX': str(flex.resolve())}
    with tempfile.TemporaryDirectory(prefix='render-preflight-pg-build-') as tmp:
        archive = Path(tmp) / 'postgresql.tar.bz2'
        download(SOURCE_URL, archive, MAX_SOURCE_BYTES)
        verify_source(archive)
        with tarfile.open(archive) as source:
            source.extractall(tmp, filter='data')
        source_dir = Path(tmp) / ('postgresql-' + VERSION)
        for command in postgres_commands(prefix):
            subprocess.run(command, cwd=source_dir, env=build_env, check=True, timeout=1200)
        marker.write_text(SOURCE_SHA256 + '\n')
    print('PREFLIGHT_POSTGRES_BUILD: unmodified verified source ' + VERSION, flush=True)


if __name__ == '__main__':
    main()
