FROM python:3.12.8-bookworm
WORKDIR /work
COPY . .
ENV RENDER_SERVICE_NAME=cg-btc-eth-ops-20261127-readiness RENDER_GIT_BRANCH=qa/coinglass-btc-eth-operational-20261127
RUN python -I -B .github/btc-once/qa_boot.py build
RUN python -m playwright install-deps firefox
ENV PLAYWRIGHT_BROWSERS_PATH=/work/.github/btc-once/browsers
CMD ["python","-u",".github/btc-once/readiness/launch.py"]
