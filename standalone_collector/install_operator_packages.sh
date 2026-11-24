#!/bin/sh
# Shared by the candidate and the inert QA image. Only distro-signed packages.
set -eu
apt-get update
apt-get install -y --no-install-recommends bubblewrap ca-certificates chromium \
    chromium-sandbox fonts-liberation novnc openbox socat tigervnc-standalone-server wmctrl x11-utils
rm -rf /var/lib/apt/lists/*
useradd --create-home --uid 10001 --user-group collector
useradd --create-home --uid 10002 --user-group cg-window
