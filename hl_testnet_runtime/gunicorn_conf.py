"""One startup task: staging persistence OR legacy read-only checks; no orders."""
workers = 1
worker_class = 'sync'
accesslog = None
errorlog = '-'
loglevel = 'warning'
timeout = 30
preload_app = False


def post_worker_init(worker):
    import os
    if (os.environ.get('HL_TESTNET_JOURNAL_BACKEND') == 'staging_postgres_v1'
            and os.environ.get('HL_TESTNET_RUNTIME_MODE', 'read_only') == 'read_only'):
        import threading
        from hl_testnet_runtime.persistent_execution import startup_storage_check
        threading.Thread(target=startup_storage_check, daemon=True,
                         name='testnet-storage-check').start()
    else:
        from hl_testnet_runtime.app import start_read_only_check
        start_read_only_check()
