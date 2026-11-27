"""Isolated QA wrapper for modern_map_cycle_child. Takes exactly the same directory and inherited fd arguments."""
import asyncio
import json
from pathlib import Path
import sys
import shutil

from capacity_meter import BrowserMeter

def stage_copy(source, dest):
    """Instrument a new disposable QA runtime copy. Refuse to alter the supplied product runtime."""
    source, dest = Path(source).resolve(), Path(dest).resolve()
    if dest.exists() or dest.is_relative_to(source) or source.is_relative_to(dest):
        raise ValueError('separate_new_qa_destination_required')
    entry = source/'modern_map_cycle.py'
    token = "str(here/'modern_map_cycle_child.py')"
    original = entry.read_text()
    if original.count(token) != 1 or not (source/'modern_map_cycle_child.py').is_file():
        raise ValueError('unknown_browser_child_entrypoint')
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns('__pycache__'), symlinks=False)
    for name in ('capacity_child_probe.py','capacity_meter.py','resource_sample.py'):
        shutil.copy2(Path(__file__).with_name(name), dest/name)
    (dest/entry.name).write_text(original.replace(token,"str(here/'capacity_child_probe.py')"))

def main():
    if len(sys.argv) == 4 and sys.argv[1] == '--prepare-qa-copy':
        stage_copy(sys.argv[2], sys.argv[3])
        return
    # This script is staged alongside the QA build's runtime modules, with
    # the same interpreter, sandbox environment and normal child fd list.
    from modern_map_cycle_child import serve
    def emit(receipt):
        print('COINGLASS_CAPACITY_BROWSER '+json.dumps(receipt,separators=(',',':')), flush=True)
    with BrowserMeter(emit).installed():
        asyncio.run(serve(Path(sys.argv[1]), [int(number) for number in sys.argv[2:]]))

if __name__ == '__main__': main()
