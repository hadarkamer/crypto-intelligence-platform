# Official linux/amd64 images resolved on 2026-10-08. No private inputs at build time.
FROM node:22-bookworm-slim@sha256:efd0ab5780c2d9ab1f0f869571a00d5edb17793bff4cce4a2792e3eb0ffc7562 AS node
FROM python:3.12-slim-bookworm@sha256:2ed6491b93cd49272ee6de2b5a38440c3448360322c089fc23e370722d74179d

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libatomic1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/bin/node /usr/local/bin/node
WORKDIR /app
COPY deploy/research-worker/requirements.txt /app/worker-requirements.txt
RUN pip install --no-cache-dir -r /app/worker-requirements.txt
COPY . /app
RUN python -c "import research_no_horizon_registered_runner, research_no_horizon_scheduler; import research_no_horizon_acquisition_store" \
    && node --version \
    && chmod +x /app/tools/no_horizon_guarded_node.py

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 NO_HORIZON_SUPERVISOR_ENABLED=0
STOPSIGNAL SIGTERM
CMD ["python", "research_no_horizon_worker_supervisor.py", "--checkpoint", "/var/data/no-horizon/inputs/baseline/prospective_discovery", "--adapter-dir", "/var/data/no-horizon/inputs/integration/oct6_future_execution", "--work-dir", "/var/data/no-horizon/work", "--state-dir", "/var/data/no-horizon/supervisor", "--source-id", "crypto-research-source", "--node", "/usr/local/bin/node", "--interval-seconds", "300", "--shutdown-seconds", "240"]
