"""Build an unmodified, checksum-pinned PostgreSQL for isolated Render tests.

This is a build-time dependency only. It neither initializes a database nor
connects to any database, and never runs from the production bot entrypoint.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import urllib.request

VERSION = '18.6'
SOURCE_URL = 'https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.bz2'
SOURCE_SHA256 = '555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f'
MAX_SOURCE_BYTES = 32 * 1024 * 1024


def verify_source(path):
    if path.stat().st_size > MAX_SOURCE_BYTES:
        raise RuntimeError('POSTGRES_SOURCE_TOO_LARGE')
    if hashlib.sha256(path.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise RuntimeError('POSTGRES_SOURCE_CHECKSUM_MISMATCH')


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
    with tempfile.TemporaryDirectory(prefix='render-preflight-pg-build-') as tmp:
        archive = Path(tmp) / 'postgresql.tar.bz2'
        with urllib.request.urlopen(SOURCE_URL, timeout=60) as response, archive.open('wb') as output:
            total = 0
            while block := response.read(1024 * 1024):
                total += len(block)
                if total > MAX_SOURCE_BYTES:
                    raise RuntimeError('POSTGRES_SOURCE_TOO_LARGE')
                output.write(block)
        verify_source(archive)
        with tarfile.open(archive) as source:
            source.extractall(tmp, filter='data')
        source_dir = Path(tmp) / ('postgresql-' + VERSION)
        commands = [
            ['./configure', '--prefix=' + str(prefix), '--without-readline',
             '--without-zlib', '--without-icu', '--without-lz4', '--without-zstd',
             '--without-ssl'],
            ['make', '-j2'],
            ['make', 'install'],
        ]
        for command in commands:
            subprocess.run(command, cwd=source_dir, check=True, timeout=1200)
        marker.write_text(SOURCE_SHA256 + '\n')
    print('PREFLIGHT_POSTGRES_BUILD: unmodified verified source ' + VERSION, flush=True)


if __name__ == '__main__':
    main()
