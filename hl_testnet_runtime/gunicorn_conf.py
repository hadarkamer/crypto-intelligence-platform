"""Read-only defaults; explicit Testnet entry test and registered-order monitoring."""
workers = 1
worker_class = 'sync'
accesslog = None
errorlog = '-'
loglevel = 'warning'
timeout = 30
preload_app = False


def post_worker_init(worker):
    import os
    mode = os.environ.get('HL_TESTNET_RUNTIME_MODE','read_only')
    if mode == 'cancel_monitor_testnet_v1':
        from hl_testnet_runtime.pending_cancel_monitor import start
        start()
    elif mode in ('single_testnet_attempt_v1','inspect_testnet_attempt_v1'):
        import threading
        from hl_testnet_runtime.controlled_attempt import startup_single_attempt
        threading.Thread(target=startup_single_attempt,daemon=True,name='explicit-single-testnet').start()
    elif (mode == 'read_only' and os.environ.get('HL_TESTNET_JOURNAL_BACKEND') == 'staging_postgres_v1'
            and os.environ.get('HL_TESTNET_CANCELLATION_RULE_REVIEW')):
        import threading
        from hl_testnet_runtime.half_threshold_cancel import startup_review
        threading.Thread(target=startup_review,daemon=True,name='cancel-rule-review-only').start()
    elif (os.environ.get('HL_TESTNET_JOURNAL_BACKEND') == 'staging_postgres_v1' and mode == 'read_only'):
        import threading
        from hl_testnet_runtime.persistent_execution import startup_storage_check
        threading.Thread(target=startup_storage_check,daemon=True,name='testnet-storage-check').start()
    else:
        from hl_testnet_runtime.app import start_read_only_check
        start_read_only_check()


def worker_exit(server, worker):
    from hl_testnet_runtime.pending_cancel_monitor import stop
    stop()
