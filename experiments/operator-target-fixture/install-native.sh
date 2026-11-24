#!/bin/sh
# Invoke once, only on a newly approved disposable 64-bit Debian 13 VM.
# This file stages the neutral fixture. It never starts a browser or a service.
set -eu
test "$(id -u)" = 0
. /etc/os-release
test "$ID" = debian
test "$VERSION_ID" = 13
test "$(dpkg --print-architecture)" = amd64
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd)
test "$#" -eq 1
case "$1" in ''|*[!0123456789abcdef]*) exit 2;; esac
test "${#1}" -eq 40
test "$(git -C "$REPO" rev-parse HEAD)" = "$1"
git -C "$REPO" diff --quiet HEAD -- standalone_collector experiments/operator-target-fixture
DEST=/opt/coinglass-window-fixture
VENV=/usr/local/lib/coinglass-window-fixture
POLICY=/etc/chromium/policies/managed/operator.json
UNIT=/etc/systemd/system/coinglass-window-fixture.service
ACCOUNT=cg-window-fixture
for p in "$DEST" "$VENV" "$POLICY" "$UNIT"; do
  test ! -e "$p" && test ! -L "$p" || { echo "Existing fixture path; refusing to replace it" >&2; exit 1; }
done
if getent passwd "$ACCOUNT" >/dev/null || getent group "$ACCOUNT" >/dev/null; then
  echo "Fixture account already exists" >&2; exit 1
fi
test -d /run/systemd/system
install -d -m 0755 "$DEST" "$DEST/originals" "$DEST/standalone_collector" "$DEST/qa"
# Pin and compile before installing system packages or starting any services.
for f in operator_desktop.py operator_desktop_child.py operator_chromium_policy.json; do
  install -m 0644 "$REPO/standalone_collector/$f" "$DEST/originals/$f"
done
python3 "$HERE/prepare.py" "$DEST/originals" "$DEST/overlay"
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  bubblewrap ca-certificates chromium chromium-sandbox fonts-liberation novnc openbox socat \
  tigervnc-standalone-server wmctrl x11-utils python3-venv
python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install --no-cache-dir \
  aiohttp==3.11.11 playwright==1.49.1 pycryptodome==3.21.0 Pillow==12.3.0
# Product source, static assets and private-child dependencies only. No worker entry point,
# user settings, production database credentials, browser profile or service environment.
for f in operator_login.py operator_window.py operator_viewer.html operator_viewer.css \
  operator_viewer.js session_capture.py session_state.py session_runtime.py modern_map_reader.py \
  modern_map_evidence.py heatmap_hover.py model1_execution.py collection_metrics.py \
  capture_failure_diagnostics.py capture_failure_observer.py capture_readiness.py heatmap_models.py \
  image_detail.py model1_price_range.py price_detail_input.py; do
  install -m 0644 "$REPO/standalone_collector/$f" "$DEST/standalone_collector/$f"
done
for f in operator_desktop.py operator_desktop_child.py operator_chromium_policy.json; do
  install -m 0644 "$DEST/overlay/$f" "$DEST/standalone_collector/$f"
done
for f in server.py prepare.py; do install -m 0644 "$HERE/$f" "$DEST/qa/$f"; done
install -d -m 0755 /etc/chromium/policies/managed
install -m 0644 "$DEST/overlay/operator_chromium_policy.json" "$POLICY"
groupadd --system "$ACCOUNT"
useradd --system --gid "$ACCOUNT" --no-create-home --home-dir /var/lib/coinglass-window-fixture \
  --shell /usr/sbin/nologin "$ACCOUNT"
install -d -m 0700 /run/coinglass-fixture-config
install -m 0644 "$HERE/coinglass-window-fixture.service" "$UNIT"
systemctl daemon-reload
