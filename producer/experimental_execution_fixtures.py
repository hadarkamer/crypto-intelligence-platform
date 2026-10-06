"""Synthetic contract fixtures for offline receiver tests; never live signals."""
import experimental_execution_contract as c

BASE = 1791298800000  # 2026-10-06T15:00:00Z; fixed deterministic fixture only.


def r2732_message(*, entry=100., decision_ms=BASE, as_of_ms=None, kind='PLAN', cancel_reason=None):
    entry_at = decision_ms + 60_000
    as_of = entry_at + 10_000 if as_of_ms is None else as_of_ms
    stop = entry * 1.005
    risk = abs(entry - stop)
    value = dict(version=c.VERSION, kind=kind, family='r2732',
        rule_id='R2732_XRP_SHORT_NY_WEEKDAYS_LOCK', symbol='XRP', side='SHORT',
        source_environment='mainnet', execution_environment='testnet', source_price_kind='TRADE_1M',
        source_at=c.iso_ms(entry_at), created_at=c.iso_ms(entry_at), arm_at=c.iso_ms(entry_at),
        expires_at=c.iso_ms(entry_at + 90_000), entry=c.price(entry), stop=c.price(stop),
        take_profit=c.price(entry * .92), original_target=None,
        policy=dict(name='R2732_CLOSED_MINUTE_LOCK_V1', original_risk_distance=c.price(risk),
            lock_trigger_price=c.price(entry - 2*risk), locked_stop=c.price(entry - .5*risk),
            trigger_R='2', profit_R='0.5', effective='NEXT_MINUTE', barrier_precedence='OLD_STOP_TAKE_FIRST',
            source_price='HYPERLIQUID_XRP_PERPETUAL_TRADE_1M'),
        proof=dict(source_contract_version=c.SOURCE_CONTRACT, source_config_sha256='a'*64,
                   decision_at=c.iso_ms(decision_ms)),
        source_sequence=as_of * 10 + c.RANK[kind], source_as_of=c.iso_ms(as_of),
        source_state='ENDED' if kind == 'CANCEL' else 'PENDING',
        valid_until=c.iso_ms(min(entry_at + 90_000, as_of + c.LEASE_MS)), cancel_reason=cancel_reason)
    value['occurrence_id'] = c.occurrence_id(value)
    return c.validate(value)


def maxpain_message(*, created_ms=BASE+10_000, as_of_ms=None, kind='PLAN', cancel_reason=None):
    as_of = created_ms if as_of_ms is None else as_of_ms
    arm = (created_ms // 60_000 + 1) * 60_000
    value = dict(version=c.VERSION, kind=kind, family='maxpain', rule_id='HYPE_MAXPAIN_DIST05_15_LONG_TF',
        symbol='HYPE', side='LONG', source_environment='mainnet', execution_environment='testnet',
        source_price_kind='TRADE_1M', source_at=c.iso_ms(created_ms), created_at=c.iso_ms(created_ms),
        arm_at=c.iso_ms(arm), expires_at=c.iso_ms(arm+86400_000),
        entry='98.0', stop='95.0', take_profit='100.5', original_target='101.0',
        policy=dict(name='MAXPAIN_LIMIT_PRETOUCH_V1', entry_adverse='2.0', take_fraction='0.5',
            stop_distance_multiplier='5', overlap_target_fraction='0.002', liquidity_growth=True,
            source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M'),
        proof=dict(source_contract_version=c.SOURCE_CONTRACT, source_config_sha256='a'*64,
            cycle_id=str(created_ms), episode_key='101', episode_generation=1, episode_first_ms=created_ms,
            timeframe='3d', source_side='SHORT', source_observed_ms=created_ms,
            source_quote=dict(price_source='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M', price_market='perpetual',
                              price_pair='HYPE-PERP', price_instrument='HYPE'),
            liquidation_amount='100', cluster_comparisons=[], range24_low=None, range24_high=None, source_price='100.0'),
        source_sequence=as_of*10+c.RANK[kind], source_as_of=c.iso_ms(as_of),
        source_state='ENDED' if kind == 'CANCEL' else 'PENDING',
        valid_until=c.iso_ms(min(arm+86400_000, as_of+c.LEASE_MS)), cancel_reason=cancel_reason)
    value['occurrence_id'] = c.occurrence_id(value)
    return c.validate(value)


def hype_row71205_message(*, entry=100., decision_ms=BASE, as_of_ms=None, kind='PLAN', cancel_reason=None):
    value = r2732_message(entry=entry, decision_ms=decision_ms, as_of_ms=as_of_ms,
                          kind=kind, cancel_reason=cancel_reason)
    value.update(family='hype_row71205', rule_id='HYPE_ROW71205_SHORT', symbol='HYPE',
        take_profit=c.price(entry * .98), policy=dict(name='HYPE_ROW71205_MARKET_NEXT_MINUTE_V1',
        entry_kind='MARKET_REFERENCE', capacity=1, holding_timeout=None,
        source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M'))
    value['occurrence_id'] = c.occurrence_id(value)
    return c.validate(value)


def sol_g65_message(*, reference=100., decision_ms=BASE, as_of_ms=None, kind='PLAN', cancel_reason=None):
    reference_at = decision_ms + 60_000
    as_of = reference_at + 10_000 if as_of_ms is None else as_of_ms
    entry, take = reference * 1.005, reference * .9825
    distance = entry - take
    value = dict(version=c.VERSION, kind=kind, family='sol_g65', rule_id='SOL_G65_K49_PROFIT_LOCK',
        symbol='SOL', side='SHORT', source_environment='mainnet', execution_environment='testnet',
        source_price_kind='TRADE_1M', source_at=c.iso_ms(reference_at), created_at=c.iso_ms(reference_at),
        arm_at=c.iso_ms(reference_at), expires_at=None, entry=c.price(entry),
        stop=c.price(reference*1.0125), take_profit=c.price(take), original_target=None,
        proof=dict(source_contract_version=c.SOURCE_CONTRACT, source_config_sha256='a'*64,
            decision_at=c.iso_ms(decision_ms), reference_price=c.price(reference)),
        policy=dict(name='SOL_G65_CLOSED_MINUTE_LOCK_V1', entry_kind='LIMIT_PRETOUCH', capacity=1,
            pending_timeout=None, holding_timeout=None, pending_cancel_price=c.price(reference*.99),
            original_take_distance=c.price(distance), lock_trigger_price=c.price(entry-.75*distance),
            locked_stop=c.price(entry-.25*distance), trigger_take_fraction='0.75', profit_take_fraction='0.25',
            effective='NEXT_MINUTE', entry_bar_trigger='CLOSE_ONLY', later_bar_trigger='LOW',
            barrier_precedence='OLD_STOP_TAKE_FIRST', source_price='HYPERLIQUID_SOL_PERPETUAL_TRADE_1M'),
        source_sequence=as_of*10+c.RANK[kind], source_as_of=c.iso_ms(as_of),
        source_state='ENDED' if kind=='CANCEL' else 'PENDING', valid_until=c.iso_ms(as_of+c.LEASE_MS),
        cancel_reason=cancel_reason)
    value['occurrence_id'] = c.occurrence_id(value)
    return c.validate(value)
