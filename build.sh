#!/usr/bin/env bash
set -euo pipefail

# Restore the canonical dependency file before every Render build.
cp requirements.canonical.txt requirements.txt
python -m pip install -r requirements.canonical.txt
python -m playwright install chromium
