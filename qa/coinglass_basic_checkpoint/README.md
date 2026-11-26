# BTC BASIC recovery fixture

This directory holds an offline QA adapter for collector commit `05e87d62d344bce9fd0bf7a587f011663e8db2e7`. The previous live QA commit is `e498814f01f318eb28f3aa922907ef3ff26b4342`.

The reader emits `verified_basic` after completing its closing source checks on the same document. `single_source.py` verifies the active account, binds the already-filtered PNG and snapshot to the job, and stores their existing encrypted market object. A local job receipt identifies the object. `single_host.py` uses a fresh owner-only database connection via `numeric_publish.py` to reopen and authenticate that object after source worker failure. The local receipt alone cannot authorize publication. Replacement of the entire ephemeral host also removes its local job receipt.

Run the four recovery cases from the repository root using the collector's Python dependencies, including Pillow.

```sh
python -m unittest discover -s qa/coinglass_basic_checkpoint -p test_after_checkpoint.py -v
```

The tests use the repository's filtered market fixture and simulated closing observations. Database lookup and source worker execution are mocked. They check committed source recovery, a missing object, a missing job receipt, and a receipt for another job.

For a separately authorized future acceptance package, the changed overlay files are `single_source.py`, `single_host.py` and `market_vault.py`. The comparison base is the archive at `standalone_collector/qa_single_pilot.zip` in the historical live QA commit. `qa_job_server.py`, `retained_evidence.py` and `numeric_publish.py` are included for offline test reproducibility.
