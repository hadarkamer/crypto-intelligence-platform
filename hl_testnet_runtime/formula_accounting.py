"""Per-formula realized capital in the existing execution journal.

Only trades carrying an entry_risk belong to this accounting epoch. Earlier
orders, including ones still open at deployment, retain their original terms
and do not retroactively change the new $40 portfolios. No exchange writes.
All reducers run under the execution state's existing transaction/lock.
"""
from copy import deepcopy
from decimal import Decimal, localcontext

from . import card_lifecycle as life

VERSION = 'formula-realized-risk-5pct-v1'
INITIAL_USD = '40'
FRACTION = '0.05'
DAY_MS = 86400000
FINAL = frozenset(('CLOSED', 'CANCELED_WITHOUT_FILL', 'MANUALLY_CLOSED'))


class AccountingError(ValueError):
    pass


def validate_risk(value, formula=None):
    try:
        life.shape(value, 'policy formula balance_usd revision fraction planned_usd')
        life.ident(value['formula'])
        balance = life.number(value['balance_usd'], positive=True)
        risk = life.number(value['planned_usd'], positive=True)
        with localcontext() as ctx:
            ctx.prec = 60
            valid = risk == balance * Decimal(FRACTION)
        if (value['policy'] != VERSION or value['fraction'] != FRACTION or not valid
                or type(value['revision']) is not int or value['revision'] < 0
                or formula is not None and value['formula'] != formula):
            raise ValueError()
        return risk
    except (ValueError, KeyError, TypeError):
        raise AccountingError('EXACT_FORMULA_RISK_REQUIRED') from None


def risk_for(state, source):
    formula = source['rule_id']
    life.ident(formula)
    for trade in state['trades'].values():
        if (trade.get('entry_risk', {}).get('formula') == formula
                and not trade.get('settlement')
                and (trade['phase'] in FINAL or trade.get('manual_management'))):
            raise AccountingError('FORMULA_NET_SETTLEMENT_PENDING')
    ledger = state.setdefault('formula_accounting', dict(version=VERSION, portfolios={}))
    if ledger['version'] != VERSION:
        raise AccountingError('FORMULA_ACCOUNTING_VERSION_MISMATCH')
    account = ledger['portfolios'].setdefault(formula,
        dict(balance_usd=INITIAL_USD, realized_net_usd='0', revision=0))
    balance = life.number(account['balance_usd'], signed=True)
    with localcontext() as ctx:
        ctx.prec = 60
        if (balance != Decimal(INITIAL_USD) + life.number(account['realized_net_usd'], signed=True)
                or type(account['revision']) is not int or account['revision'] < 0):
            raise AccountingError('FORMULA_CAPITAL_INTEGRITY_FAILURE')
    if balance <= 0:
        raise AccountingError('FORMULA_CAPITAL_EXHAUSTED')
    with localcontext() as ctx:
        ctx.prec = 60
        risk = dict(policy=VERSION, formula=formula, balance_usd=account['balance_usd'],
            revision=account['revision'], fraction=FRACTION,
            planned_usd=life.text(balance * Decimal(FRACTION)))
    validate_risk(risk, formula)
    return risk


def _closed(state, trade):
    """Final bot-owned quantities and orders, never a planned exit or manual P/L."""
    if trade.get('manual_management'):
        raise AccountingError('FORMULA_EXTERNAL_ACTIVITY_UNALLOCATED')
    if trade['phase'] not in ('CLOSED', 'CANCELED_WITHOUT_FILL'):
        raise AccountingError('FORMULA_TRADE_NOT_FINAL')
    from .experimental_execution_reporting import _project_rows
    return _project_rows(dict(trades={trade['cid']: trade}, requests=state['requests'],
                              events=state['events']))[0]


def retain_fees(state, context):
    """Reuse already collected canonical fills; never query fees separately."""
    trades = [t for t in state['trades'].values() if t.get('entry_risk') and not t.get('settlement')]
    if not trades:
        return
    checkpoints = {**state.get('collector_checkpoints', {}),
                   **context.get('collector_checkpoints', {})}
    rows = [f for checkpoint in checkpoints.values() for f in checkpoint.get('fills', [])]
    rows += [f for account in state.get('external_activity', {}).get('accounts', {}).values()
             for f in account.get('fills', [])]
    for trade in trades:
        facts = trade.setdefault('fee_facts', {})
        fills = {**trade['entry_fills'], **trade['exit_fills']}
        for row in rows:
            fid = row['fill_id']
            if row['account'] != trade['account'] or row['symbol'] != trade['symbol'] or fid not in fills:
                continue
            fill = fills[fid]
            fact = {k: row[k] for k in ('oid', 'quantity', 'price', 'at_ms', 'side', 'fee', 'fee_token')}
            expected_side = ('B' if trade['side'] == 'LONG' else 'A') if fid in trade['entry_fills'] else (
                'A' if trade['side'] == 'LONG' else 'B')
            if (fact['oid'] != fill['order_id'] or fact['side'] != expected_side
                    or any(fact[k] != fill[k] for k in ('quantity', 'price', 'at_ms'))
                    or fid in facts and facts[fid] != fact):
                # Durable conflict is never cleared by a later conveniently
                # matching response. It closes accounting, not protection.
                trade['accounting_conflict'] = 'FORMULA_FILL_COST_CONFLICT'
                continue
            facts[fid] = deepcopy(fact)


def _fees(trade):
    if trade.get('accounting_conflict'):
        raise AccountingError(trade['accounting_conflict'])
    fills = {**trade['entry_fills'], **trade['exit_fills']}
    facts = trade.get('fee_facts', {})
    if set(facts) != set(fills):
        raise AccountingError('FORMULA_ACTUAL_FEES_PENDING')
    total = Decimal(0)
    for fid, fact in facts.items():
        fill = fills[fid]
        if fact['oid'] != fill['order_id'] or any(fact[k] != fill[k] for k in ('quantity','price','at_ms')):
            raise AccountingError('FORMULA_FILL_COST_CONFLICT')
        if fact['fee_token'] != 'USDC':
            raise AccountingError('FORMULA_FEE_CURRENCY_REQUIRES_REVIEW')
        total += life.number(fact['fee'], signed=True)  # Negative maker rebates are income.
    return total


def funding_task(state, now):
    """One bounded background page per idle cycle, only for completed trades."""
    for cid, trade in sorted(state['trades'].items(), key=lambda item: (
            item[1].get('funding_scan', {}).get('last_attempt_ms', 0), item[0])):
        if (not trade.get('entry_risk') or trade.get('settlement')
                or trade['phase'] != 'CLOSED' or trade.get('manual_management')):
            continue
        try:
            row = _closed(state, trade)
            _fees(trade)
        except ValueError:
            continue
        scan = trade.get('funding_scan', {})
        if scan.get('complete') or now < scan.get('retry_at_ms', 0):
            continue
        start = scan.get('next_ms', row['first_fill_at_ms'])
        end = min(row['last_exit_fill_at_ms'], start + DAY_MS)
        return dict(cid=cid, account=trade['account'], symbol=trade['symbol'], start=start, end=end,
            final_ms=row['last_exit_fill_at_ms'], basis=life.digest(trade))
    return None


def funding_page(task, raw):
    """HL /info userFunding: inclusive time windows, 500-row/block pagination.

    Keep the last timestamp on a full page so ties cannot be skipped. A full
    page stuck at one timestamp is incomplete, never silently treated as zero.
    """
    if not isinstance(raw, list):
        raise AccountingError('FORMULA_FUNDING_RESPONSE_INVALID')
    rows = {}
    times = []
    for row in raw:
        try:
            at = life.moment(row['time']); delta = row['delta']
            life.ident(row['hash'], r'0x[0-9a-fA-F]{64}')
            life.ident(delta['coin'])
            if delta['type'] != 'funding' or not task['start'] <= at <= task['end']:
                raise ValueError()
            cash = life.number(delta['usdc'].strip(), signed=True)
            size = life.number(delta['szi'].strip(), signed=True)
            if not size:
                raise ValueError()
            times.append(at)
            if delta['coin'] != task['symbol']:
                continue
            fact = dict(at_ms=at, usdc=life.text(cash), signed_quantity=life.text(size))
            identity = life.digest([task['account'], delta['coin'], row['hash'].lower(), at])
            if identity in rows and rows[identity] != fact:
                raise ValueError()
            rows[identity] = fact
        except (ValueError, KeyError, TypeError, AttributeError):
            raise AccountingError('FORMULA_FUNDING_RESPONSE_INVALID') from None
    full = len(raw) >= 500
    if full and max(times) <= task['start']:
        raise AccountingError('FORMULA_FUNDING_PAGE_INCOMPLETE')
    next_ms = max(times) if full else task['end'] + 1
    return dict(rows=rows, next_ms=next_ms, complete=next_ms > task['final_ms'])


def record_funding(state, task, page, now, error=None):
    trade = state['trades'].get(task['cid'])
    if trade is None or life.digest(trade) != task['basis']:
        return False  # Concurrent observation changed ownership; collect again.
    scan = deepcopy(trade.get('funding_scan', dict(rows={})))
    scan['last_attempt_ms'] = now
    if error:
        scan.update(retry_at_ms=now + 60000, error=error)
    else:
        for identity, fact in page['rows'].items():
            if identity in scan['rows'] and scan['rows'][identity] != fact:
                trade['accounting_conflict'] = 'FORMULA_FUNDING_FACT_CHANGED'
                return False
            scan['rows'][identity] = deepcopy(fact)
        scan.update(next_ms=page['next_ms'], complete=page['complete'])
        scan.pop('error', None); scan.pop('retry_at_ms', None)
    trade['funding_scan'] = scan
    return True


def _funding(trade):
    scan = trade.get('funding_scan', {})
    if not scan.get('complete'):
        raise AccountingError('FORMULA_ACTUAL_FUNDING_PENDING')
    total = Decimal(0)
    sign = Decimal(1 if trade['side'] == 'LONG' else -1)
    fills = [(f, sign) for f in trade['entry_fills'].values()]
    fills += [(f, -sign) for f in trade['exit_fills'].values()]
    for row in scan['rows'].values():
        # Same-millisecond fills have no reliable ordering against funding.
        # Require exact owned quantity strictly before the funding event.
        if any(f['at_ms'] == row['at_ms'] for f, _ in fills):
            raise AccountingError('FORMULA_FUNDING_OWNERSHIP_AMBIGUOUS')
        owned = sum((life.number(f['quantity'], positive=True) * side
                     for f, side in fills if f['at_ms'] < row['at_ms']), Decimal(0))
        if not owned or owned != life.number(row['signed_quantity'], signed=True):
            raise AccountingError('FORMULA_FUNDING_OWNERSHIP_AMBIGUOUS')
        total += life.number(row['usdc'], signed=True)
    return total


def settle(state, now):
    """Balance and trade receipt commit together; archived receipts stay final."""
    for cid, trade in sorted(state['trades'].items()):
        if not trade.get('entry_risk') or trade.get('settlement') or trade['phase'] not in FINAL:
            continue
        try:
            with localcontext() as ctx:
                ctx.prec = 60
                row = _closed(state, trade)
                fees = _fees(trade)
                funding = _funding(trade) if trade['phase'] == 'CLOSED' else Decimal(0)
                gross = life.number(row['gross_pnl_before_costs'] or '0', signed=True)
                net = gross - fees + funding
                formula = trade['source']['rule_id']
                validate_risk(trade['entry_risk'], formula)
                portfolio = state['formula_accounting']['portfolios'][formula]
                before = life.number(portfolio['balance_usd'], signed=True)
                realized = life.number(portfolio['realized_net_usd'], signed=True)
                if before != Decimal(INITIAL_USD) + realized:
                    raise AccountingError('FORMULA_CAPITAL_INTEGRITY_FAILURE')
                after = before + net
                updated_realized = life.text(realized + net)
                # Validate serialized bounds before committing any balance.
                life.number(life.text(after), signed=True)
                receipt = dict(formula=formula, at_ms=now, gross_usd=life.text(gross),
                    fees_usd=life.text(fees), funding_usd=life.text(funding), net_usd=life.text(net),
                    balance_before_usd=life.text(before), balance_after_usd=life.text(after),
                    revision=portfolio['revision'] + 1)
            portfolio.update(balance_usd=life.text(after), realized_net_usd=updated_realized,
                             revision=receipt['revision'])
            trade['settlement'] = receipt
            trade.pop('accounting_pending', None)
        except (ValueError, KeyError, TypeError) as exc:
            reason = str(exc)
            trade['accounting_pending'] = reason if reason.startswith('FORMULA_') else 'FORMULA_NET_RECONCILIATION_REQUIRED'
