# Isolated DOGE execution verification

These six test modules and their synthetic fixture are stored separately from
the application branches. The runner reads the receiver and producer checkouts
without copying tests into them or changing application files.

Run with an existing environment containing both projects' test dependencies:

```sh
python run_isolated.py \
  --receiver /absolute/path/to/receiver \
  --producer /absolute/path/to/producer \
  --python /absolute/path/to/test-venv/bin/python
```

Each suite runs in a fresh interpreter with a fixed environment containing no
inherited credentials or database URLs. The test-only guard blocks Python
sockets and native psycopg connections and records attempted access in a
temporary log. Any attempt makes the runner fail. This is a software guard,
not an operating-system network sandbox. Existing test doubles supply synthetic
accounts, orders and fills; no live exchange requests are needed.

The expected local result is 37 receiver tests passed, 10 producer tests passed,
and **one PostgreSQL test skipped**. That database concurrency/restart test needs
the separate disposable-loopback PostgreSQL verification environment; this
all-network-blocked runner deliberately does not accept a database URL. A local
pass must not be reported as a PostgreSQL pass.
