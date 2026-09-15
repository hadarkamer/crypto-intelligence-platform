"""One read-only check per boot, no request logging and no public controls."""
workers = 1
worker_class = 'sync'
accesslog = None
errorlog = '-'
loglevel = 'warning'
timeout = 30
preload_app = False


def post_worker_init(worker):
    from hl_testnet_runtime.app import start_read_only_check
    start_read_only_check()
