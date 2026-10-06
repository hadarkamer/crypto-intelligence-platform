"""Display-only projection boundary. No app client, callback, orders or secrets.

Only a copy of locally verified lifecycle data may leave for the journal UI.
There is deliberately no inverse function: display data cannot become an alert,
execution binding, recovery policy, acknowledgement from the venue or an order.
The actual authenticated application delivery and read-only DB role are not
installed by this module; those must be verified before connecting the UI.
"""
from copy import deepcopy
from . import card_lifecycle as life

VERSION = 'testnet-card-display-only-v1'
FIELDS = ('card_id', 'role', 'state', 'entry_quantity', 'exit_quantity',
          'remaining_quantity', 'fees_by_token', 'gross_pnl_usdc',
          'net_before_funding_usdc', 'funding_usdc', 'final_net_usdc',
          'closure_verified', 'issues')


def project(bindings, snapshot, *, revision, now_ms):
    if type(revision) is not int or revision <= 0:
        raise life.LifecycleError('DISPLAY_REVISION_REQUIRED')
    report = life.review(bindings, snapshot, now_ms=now_ms)
    by_id = {b['card_id']: b for b in bindings}
    cards = []
    for view in report['cards']:
        b = by_id[view['card_id']]
        value = {name: deepcopy(view[name]) for name in FIELDS}
        value.update(symbol=b['symbol'], direction=b['side'], prices=deepcopy(b['prices']))
        cards.append(value)
    body = dict(version=VERSION, environment='testnet', read_only=True,
        revision=revision, observed_at_ms=snapshot['at_ms'],
        bucket_issues=deepcopy(report['bucket_issues']), cards=cards)
    # Independent from execution request IDs, addresses and client references.
    return {**body, 'delivery_id': life.digest(body)}


def receipt_matches(receipt, delivery_id):
    """Accept a data-delivery receipt only. Never instructions or market facts.

    A receipt is not trusted to confirm an exchange action or to alter any
    execution state. Retries belong only to the future display delivery queue.
    """
    life.ident(delivery_id, r'[0-9a-f]{64}')
    life.shape(receipt, 'delivery_id received')
    if receipt['delivery_id'] != delivery_id or receipt['received'] is not True:
        raise life.LifecycleError('DISPLAY_RECEIPT_MISMATCH')
    return True
