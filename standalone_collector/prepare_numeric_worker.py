"""Install the fixed-cycle worker beside the existing standalone runtime during its normal build."""
from hashlib import sha256
from importlib.metadata import version
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig
import venv

HERE = Path(__file__).resolve().parent
BASE = '81505afa9a235edaeac95ede7beca3850beaf1c6'
OVERLAYS = ('modern_map_cycle.py', 'modern_map_cycle_child.py', 'scoped_entry.py')
SHARED_PACKAGES = {
    'aiohttp': '3.11.11', 'psycopg': '3.2.3', 'psycopg-binary': '3.2.3',
    'playwright': '1.55.0', 'Pillow': '12.3.0', 'requests': '2.32.5',
}


def install_sources(output):
    inputs = HERE / 'numeric_inputs'
    manifest = json.loads((inputs / 'manifest.json').read_bytes())
    if manifest['base'] != BASE:
        raise RuntimeError('Incorrect numerical collector source version')
    output.mkdir(parents=True, exist_ok=False)
    for module, entry in manifest['modules'].items():
        source = HERE / entry['path']
        data = source.read_bytes()
        if sha256(data).hexdigest() != entry['sha256']:
            raise RuntimeError(f'Frozen numerical input changed: {module}')
        (output / module).write_bytes(data)
    for module in OVERLAYS:
        shutil.copyfile(HERE / 'numeric_integration' / module, output / module)
    subprocess.run([sys.executable, '-I', '-B', str(inputs / 'prepare_ocr_runtime.py'),
                    '--destination', str(output / 'ocr_runtime')], check=True, timeout=120)


def prepare():
    for package, expected in SHARED_PACKAGES.items():
        if version(package) != expected:
            raise RuntimeError(f'Unexpected legacy runtime dependency: {package}')
    root = HERE / 'numeric'
    if root.is_symlink():
        raise RuntimeError('Invalid numerical build output')
    if root.exists():
        shutil.rmtree(root)
    root.mkdir()
    runtime = root / 'runtime'
    install_sources(runtime)
    env_dir = root / 'venv'
    venv.EnvBuilder(with_pip=True).create(env_dir)
    python = env_dir / 'bin/python'
    # Share only the already pinned dependency installation. Local numerical modules
    # run in a separate process, with an independent module cache and import root.
    library = Path(subprocess.check_output([str(python), '-I', '-c',
        "import sysconfig;print(sysconfig.get_path('purelib'))"], text=True).strip())
    packages = sorted({sysconfig.get_path('purelib'), sysconfig.get_path('platlib')})
    (library / 'standalone-dependencies.pth').write_text(''.join(os.path.relpath(p, library) + '\n' for p in packages))
    subprocess.run([str(python), '-I', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-deps',
                    'pycryptodome==3.21.0'], check=True, timeout=180)
    subprocess.run([str(python), '-I', '-m', 'playwright', 'install', 'firefox'], check=True, timeout=240)
    # No account details or source URLs are needed to verify browser startup and reader imports.
    allowed = ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL', 'LD_LIBRARY_PATH',
               'PLAYWRIGHT_BROWSERS_PATH', 'XDG_CACHE_HOME', 'XDG_RUNTIME_DIR', 'FONTCONFIG_PATH')
    smoke_env = {name: os.environ[name] for name in allowed if name in os.environ}
    subprocess.run([str(python), '-I', '-B', '-c',
        "import sys;sys.path.insert(0,sys.argv[1]);"
        "import scoped_entry,modern_map_cycle,modern_map_cycle_child,modern_map_basic,session_runtime;"
        "from importlib.metadata import version;assert version('pycryptodome')=='3.21.0';"
        "from playwright.sync_api import sync_playwright;"
        "p=sync_playwright().start();b=p.firefox.launch(headless=True);"
        "assert b.new_page().url=='about:blank';b.close();p.stop()", str(runtime)],
        check=True, timeout=45, env=smoke_env)
    (root / '.ready').write_text(BASE + '\n')
    print('COINGLASS_FIXED_RUNTIME_READY source_scans=0 firefox=ready ocr=5.5.0', flush=True)


if __name__ == '__main__':
    prepare()
