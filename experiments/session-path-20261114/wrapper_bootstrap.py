"""Disposable-venv test hook; no production or wrapper environment additions."""
import json
import os
from pathlib import Path
import sys

# An installed .pth imports this module in each real Python process. Other
# commands (including the disabled app/worker and Playwright driver) are untouched.
if Path(sys.argv[0]).name in ('approved_entry.py', 'observation_supervisor.py', 'collector_browser_fixture.py'):
    try:
        config_path = Path(__file__).with_name('active-wrapper.json')
        assert not config_path.is_symlink() and config_path.stat().st_uid == os.geteuid()
        assert config_path.stat().st_mode & 0o077 == 0 and config_path.stat().st_size < 4096
        config = json.loads(config_path.read_text())
        package = Path(config['package']).resolve(strict=True)
        assert Path(sys.argv[0]).resolve().parent == package
        from check_wrapper import install_process_fixture
        install_process_fixture(config)
    except BaseException:
        # Python's .pth normally logs an error and carries on: fail closed.
        os._exit(97)
