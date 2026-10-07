"""Current Testnet MARK from the official ``metaAndAssetCtxs`` response.

The fixed-host, shared-budget InfoReader owns transport and captures the clock
before its request. The response contains [metadata, asset contexts]; context
indices correspond to metadata.universe, not to an account or a supplied coin.
``activeAssetData`` contains account capacity and leverage but no MARK price.
Only the validated market snapshot below joins MARK into that internal input
for existing pure sizing checks. No HTTP, subscription or history proof occurs.
"""
from copy import deepcopy

from . import card_lifecycle as life, checks

MAX_AGE_MS = 15000


class MarketContextError(ValueError):
    """Fixed codes only; never echo exchange or account data."""


class MarketSnapshot:
    def __init__(self, raw, *, observed_at_ms):
        life.moment(observed_at_ms)
        if (not isinstance(raw, list) or len(raw) != 2
                or not isinstance(raw[0], dict) or not isinstance(raw[1], list)):
            raise MarketContextError('COMPLETE_META_AND_ASSET_CONTEXTS_REQUIRED')
        universe = raw[0].get('universe')
        if (not isinstance(universe, list) or not 1 <= len(universe) <= 10000
                or len(raw[1]) != len(universe)):
            raise MarketContextError('EXACT_UNIVERSE_CONTEXT_ALIGNMENT_REQUIRED')
        names = set()
        for asset, context in zip(universe, raw[1]):
            # Other perpetuals can have mixed-case names; reject ambiguous
            # indexing without restricting unrelated exchange-listed symbols.
            name = asset.get('name') if isinstance(asset, dict) else None
            if (not isinstance(name, str) or not 1 <= len(name) <= 80
                    or name in names or not isinstance(context, dict)):
                raise MarketContextError('UNIQUE_UNIVERSE_CONTEXT_BINDING_REQUIRED')
            names.add(name)
        self._metadata = deepcopy(raw[0])
        self._contexts = deepcopy(raw[1])
        self._at_ms = observed_at_ms

    @property
    def metadata(self):
        return deepcopy(self._metadata)

    def _mark(self, symbol, now_ms):
        life.ident(symbol, r'[A-Z][A-Z0-9]{0,19}')
        life.moment(now_ms)
        if not 0 <= now_ms - self._at_ms <= MAX_AGE_MS:
            raise MarketContextError('FRESH_TESTNET_MARK_CONTEXT_REQUIRED')
        matched = [(index, asset) for index, asset in enumerate(self._metadata['universe'])
                   if asset['name'] == symbol]
        if len(matched) != 1:
            raise MarketContextError('EXACT_TRADABLE_MARK_ASSET_REQUIRED')
        index, asset = matched[0]
        if (asset.get('isDelisted', False) is not False
                or type(asset.get('szDecimals')) is not int
                or not 0 <= asset['szDecimals'] <= 6
                or type(asset.get('maxLeverage')) is not int
                or not 1 <= asset['maxLeverage'] <= 1000):
            raise MarketContextError('EXACT_TRADABLE_MARK_ASSET_REQUIRED')
        context = self._contexts[index]
        # The official context is positional. If a transport supplies identity
        # fields as well, conflicting identity must never be silently ignored.
        if any(context[key] != symbol for key in ('coin', 'name') if key in context):
            raise MarketContextError('MARK_CONTEXT_SYMBOL_MISMATCH')
        try:
            return life.text(life.number(context.get('markPx'), positive=True))
        except ValueError:
            raise MarketContextError('VALID_TESTNET_MARK_PRICE_REQUIRED') from None

    def mark(self, *, account, symbol, now_ms):
        life.address(account)
        return dict(environment='testnet', account=account, symbol=symbol,
            at_ms=self._at_ms, mark_price=self._mark(symbol, now_ms))

    def active_asset_data(self, raw, *, account, symbol, now_ms):
        """Join independently checked current MARK with raw account capacity.

        Preserve all exchange capacity/leverage terms. The extra markPx belongs
        to this internal sizing structure; it is not a field attributed to the
        activeAssetData endpoint. The snapshot timestamp is never refreshed.
        """
        if (not isinstance(raw, dict) or raw.get('coin') != symbol
                or checks.address(raw.get('user')) != checks.address(account)):
            raise MarketContextError('CAPACITY_ACCOUNT_OR_ASSET_MISMATCH')
        mark = self._mark(symbol, now_ms)
        if 'markPx' in raw:
            raise MarketContextError('RAW_ACCOUNT_CAPACITY_MUST_NOT_SUPPLY_MARK')
        result = deepcopy(raw)
        result['markPx'] = mark
        checks.capacity(result, account, symbol)
        return result
