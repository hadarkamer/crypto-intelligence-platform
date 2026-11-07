# syntax=docker/dockerfile:1
# Temporary fixture worker only. No production entrypoint is inherited or run.
FROM --platform=linux/amd64 postgres:16.10-bookworm@sha256:94f23d40fdaf5e60cb2fd8a98c22f02a7b8724949f310d95a0ddf075e8c8b208 AS postgres_native

FROM --platform=linux/amd64 node:24.10.0-bookworm-slim@sha256:a22207f28e3e25751471503a7c3859a88adb5c71d8992304f77b4ab9ba48458a AS node_dependencies
COPY packaging/node-dependencies/package.json packaging/node-dependencies/package-lock.json /opt/lab-node/
RUN npm ci --prefix /opt/lab-node --ignore-scripts --no-audit --no-fund \
 && npm install --prefix /opt/bun --ignore-scripts --no-audit --no-fund @oven/bun-linux-x64@1.4.4
COPY app/package.json app/bun.lock /opt/app-dependencies/
RUN /opt/bun/node_modules/@oven/bun-linux-x64/bin/bun install --cwd /opt/app-dependencies --frozen-lockfile --ignore-scripts

FROM --platform=linux/amd64 python:3.12.12-slim-bookworm@sha256:2986c55feb36e6cae00fa1fefb454283e4b33f35e75ff8bdd123b134130be301 AS fixture_worker
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright \
    QUEUE_LAB_NODE=/opt/lab-node \
    PATH=/usr/lib/postgresql/16/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

# PostgreSQL is copied from its pinned official image; its Docker entrypoint,
# exposed-port metadata and automatic database initialization are not inherited.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash ca-certificates tini locales passwd libedit2 libgssapi-krb5-2 libicu72 \
      libldap-2.5-0 liblz4-1 libpam0g libreadline8 libssl3 libsystemd0 \
      libuuid1 libxml2 libxslt1.1 libzstd1 zlib1g \
 && rm -rf /var/lib/apt/lists/*
COPY --from=postgres_native /usr/lib/postgresql/16/ /usr/lib/postgresql/16/
COPY --from=postgres_native /usr/share/postgresql/16/ /usr/share/postgresql/16/
COPY --from=postgres_native /usr/lib/x86_64-linux-gnu/libpq.so* /usr/local/lib/
COPY --from=node_dependencies /usr/local/bin/node /usr/local/bin/node
COPY --from=node_dependencies /opt/bun/node_modules/@oven/bun-linux-x64/bin/bun /usr/local/bin/bun
COPY --from=node_dependencies /opt/lab-node/ /opt/lab-node/
COPY packaging/requirements.txt /opt/lab-requirements.txt
RUN ldconfig \
 && python -m pip install --no-cache-dir -r /opt/lab-requirements.txt \
 && python -m playwright install --with-deps chromium \
 && rm -rf /var/lib/apt/lists/* \
 && python -m pip freeze > /opt/lab-python-installed.txt \
 && postgres --version \
 && initdb --version \
 && psql --version \
 && node --version \
 && bun --version \
 && test -x /opt/playwright/chromium-1187/chrome-linux/chrome \
 && useradd --create-home --uid 10001 --shell /bin/bash lab \
 && mkdir -p /lab/evidence /home/lab/.ssh \
 && chmod 0700 /home/lab/.ssh \
 && chown -R lab:lab /lab /home/lab

COPY --chown=lab:lab app/ /lab/app/
COPY --chown=lab:lab collector/ /lab/collector/
COPY --chown=lab:lab packaging/ /lab/packaging/
COPY --chown=lab:lab manifests/ /lab/manifests/
COPY --from=node_dependencies --chown=lab:lab /opt/app-dependencies/node_modules/ /lab/app/node_modules/
WORKDIR /lab
USER lab:lab
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "/lab/packaging/worker_idle.py"]
