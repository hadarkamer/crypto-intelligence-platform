"""Read persisted Testnet trade observations for a private reporting surface.

No venue requests, wallets, signers or trading methods are instantiated.
"""
import os

from .filled_dispatch_store import DispatchStore
from .long_stream_runtime import observed_trades
from .postgres_journal import PostgresJournal
from .trade_cards import account_routes

ROLES = {'L': 'long_account', 'S': 'short_account'}


def load_trades(env=None):
    env = dict(os.environ if env is None else env)
    routes = account_routes(env)
    store = DispatchStore(PostgresJournal.from_env(env))
    class Reader:
        pass
    reader = Reader()
    reader.store = store
    reader.venue = Reader()
    result = {}
    for key, role in ROLES.items():
        route = routes[role]
        if route['status'] != 'CONFIGURED_NOT_VERIFIED' or not route['account']:
            raise ValueError('ACCOUNT_REPORT_MAPPING_UNAVAILABLE')
        result[key] = sorted(observed_trades(reader, route, role=role, historical=True),
                             key=lambda row: row['first_entry_at_ms'], reverse=True)
    return result
