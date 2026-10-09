# BTC report state loading

The existing report worker checks the exact published report's Sheet delivery
before it advances a checkpoint or starts another generation. A completed
generation also waits until its existing refresh time before new work starts.
Polling used to deserialize `source` and `pending_job` even on these waiting
passes. The published source alone measured 16,491,316 bytes as PostgreSQL JSON
text on 2026-10-09; that is not a measurement of Python heap use.

The worker now reads the report, timestamps, pending-job truthiness and the
row's `xmin` first. It preserves the exact report digest, missing/pending ACK
checks and withdrawal checks. Waiting passes do not return the source or
checkpoint payload to Python. An acknowledged pending checkpoint still resumes
before the next-generation refresh time.

After both applicable gates pass, one deferred query loads the payload using
the original worker key and `xmin`. A changed row fails closed before source
reads, calculation or writes; the existing polling loop retries. The session
advisory lock continues to serialize cooperating workers. The short-lived
version guard does not replace that lock or protect against an uncoordinated
writer after the deferred read. No schema change or persistent digest cache is
introduced.

The SQL pending marker matches Python truthiness for SQL NULL, JSON null,
false, numeric zero, empty string, empty array and empty object. Report/source
generation, atomic outbox publication, checkpoint recovery and late-source
correction remain unchanged.

Run `research_btc_wave_report_worker_selftest.py` and, against an isolated test
database, `research_btc_wave_report_postgres_selftest.py`. The full repository
CI runs both, with PostgreSQL 18 for the database tests. The tests cover waiting
without payload reads, exact ACK priority, checkpoint resume, JSON truthiness,
concurrent version changes, publication and rollback.

This removes a demonstrated unnecessary load. It does not establish the cause
of the earlier service OOM or prove long-term memory stability. Existing
bounded runtime samples remain necessary; restarts are not comparable before/
after measurements by themselves. Research registrations, formulas, data
populations, startup flags and schedules are unchanged.

PostgreSQL's row-version semantics are documented at
https://www.postgresql.org/docs/current/ddl-system-columns.html.
