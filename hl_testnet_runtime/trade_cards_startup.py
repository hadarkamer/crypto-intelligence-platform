"""Explicit Phase-1 storage probe beside the existing cancellation monitor.

No timer, source polling, public endpoint, app push, key read or order submission.
This initializes record storage and reviews previously saved sources only.
"""
import json


def run(env, *, journal=None):
    report = dict(status='DISABLED', phase='cards_phase1_record_only',
        entry_sending_enabled=False, order_requests_sent=0,
        signing_tested=False, app_delivery_enabled=False, live_alert_feed_connected=False)
    if env.get('HL_TESTNET_CARDS_PHASE1') != 'record_only_v1':
        return report
    try:
        from . import trade_cards as cards
        from .trade_card_store import CardStore
        from .postgres_journal import PostgresJournal, JournalError
        if (env.get('RENDER_SERVICE_ID') != 'srv-dakptbh594qs7395460g'
                or env.get('HL_TESTNET_RUNTIME_MODE') not in ('read_only','cancel_monitor_testnet_v1')
                or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'):
            raise JournalError('CARD_PHASE1_SERVICE_OR_MODE_NOT_ALLOWED')
        # Four PUBLIC address fields only; legacy single-account settings are not guessed.
        routes = cards.account_routes(env)
        report['account_slots'] = {role: value['status'] for role,value in routes.items()}
        store = CardStore(PostgresJournal.from_env(env) if journal is None else journal)
        report['schema_created'] = store.initialize()
        report.update(store.probe())
        reviewed = store.import_legacy_reviews()
        report.update(status='CARD_STORAGE_AND_ROUTING_REVIEW_PASSED',
            historical_sources_reviewed=len(reviewed),
            new_review_cards=sum(r['created'] for r in reviewed),
            duplicate_card_checks=sum(r['replay_verified'] for r in reviewed),
            historical_sources_are_not_new_orders=True)
    except Exception:
        # Do not log remote exceptions, addresses, source data, keys or database URL.
        report['status'] = 'CARD_PHASE1_REQUIRES_REVIEW'
    return report


def startup():
    import os
    print(json.dumps({'testnet_trade_cards':run(os.environ)}, sort_keys=True), flush=True)
