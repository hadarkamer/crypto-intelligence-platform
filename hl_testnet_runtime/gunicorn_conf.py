"""Read-only defaults; explicitly selected Testnet tasks only."""
workers = 1
worker_class = 'sync'
accesslog = None
errorlog = '-'
loglevel = 'warning'
timeout = 30
preload_app = False


def post_worker_init(worker):
    import os
    import json
    from hl_testnet_runtime.risk_policy import CURRENT_RISK_USD, VERSION
    print(json.dumps({'testnet_risk_policy': {'version': VERSION,
        'new_entry_planned_risk_usd': CURRENT_RISK_USD,
        'fees_funding_slippage_included': False,
        'resizes_existing_orders': False, 'mainnet_enabled': False}}), flush=True)
    mode = os.environ.get('HL_TESTNET_RUNTIME_MODE','read_only')
    if os.environ.get('HL_TESTNET_CARD_SYNC') == 'registered_readonly_v1':
        from hl_testnet_runtime.card_sync import start as start_card_sync
        start_card_sync()
    if (os.environ.get('HL_TESTNET_CONNECTION_REVIEW') == 'two_account_no_orders_v1'
            and mode in ('read_only','cancel_monitor_testnet_v1')):
        import threading
        from hl_testnet_runtime.connection_review_startup import startup as connection_review
        threading.Thread(target=connection_review,daemon=True,name='connection-no-orders-review').start()
    if (os.environ.get('HL_TESTNET_CARDS_PHASE1') == 'record_only_v1'
            and mode in ('read_only', 'cancel_monitor_testnet_v1')):
        import threading
        from hl_testnet_runtime.trade_cards_startup import startup as cards_startup
        threading.Thread(target=cards_startup,daemon=True,name='cards-record-only-probe').start()
    if mode in ('cancel_rehearsal_testnet_v1','inspect_cancel_rehearsal_testnet_v1'):
        import threading
        from hl_testnet_runtime.cancel_rehearsal import startup
        threading.Thread(target=startup,daemon=True,name='explicit-cancel-rehearsal').start()
    elif mode == 'cancel_monitor_testnet_v1':
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
    from hl_testnet_runtime.card_sync import stop as stop_card_sync
    stop_card_sync()
