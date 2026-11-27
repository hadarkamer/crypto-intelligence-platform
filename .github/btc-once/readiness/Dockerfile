FROM python:3.12.8-bookworm
WORKDIR /work
COPY . .
ENV RENDER_SERVICE_NAME=cg-btc-once-20261127-readiness RENDER_GIT_BRANCH=qa/coinglass-btc-once-20261127
RUN python -I -B .github/btc-once/qa_boot.py build
RUN python -m playwright install-deps firefox
RUN cp .github/btc-once/readiness/offline_browser.py .github/btc-once/readiness/local-source.html .github/btc-once/unpacked/product/runtime/ && python - <<'PY'
from pathlib import Path
p=Path('.github/btc-once/unpacked/product/runtime/capacity_child_probe.py')
s=p.read_text()
assert s.count('    from modern_map_cycle_child import serve')==1
p.write_text(s.replace('    from modern_map_cycle_child import serve','    import offline_browser\n    offline_browser.install()\n    from modern_map_cycle_child import serve'))
PY
ENV PLAYWRIGHT_BROWSERS_PATH=/work/.github/btc-once/browsers
CMD ["python","-u",".github/btc-once/readiness/launch.py"]
