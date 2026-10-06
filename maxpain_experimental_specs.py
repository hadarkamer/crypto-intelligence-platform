"""Frozen requested research definitions; notification-only prospective activation."""
from dataclasses import asdict, dataclass
import json

TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')

@dataclass(frozen=True)
class FormulaSpec:
    rule_id: str
    research_id: str
    coin: str
    lower_pct: float
    upper_pct: float
    entry_adverse: float = 2.0
    take_fraction: float = 1.0
    direction: str = 'BOTH'
    timeframes: tuple = TIMEFRAMES
    require_range24: bool = False
    liquidity_growth: bool = False
    legacy_score: bool = False

    def canonical(self):
        return json.dumps(asdict(self), sort_keys=True, separators=(',', ':')).encode()

LEGACY_SOL = FormulaSpec('SOL_MAXPAIN_PROXIMITY_GT15', 'BASE_SOL_BOTH_CONCURRENT_GT15',
                        'SOL', 0, 100, legacy_score=True)
SOL_RANGE24 = FormulaSpec('SOL_MAXPAIN_DIST1_3_RANGE24', '7815ee3bcf3531fb', 'SOL', 1, 3,
                          require_range24=True)
HYPE_LONG_TF = FormulaSpec('HYPE_MAXPAIN_DIST05_15_LONG_TF', '66e5f77c438ba155', 'HYPE', .5, 1.5,
                          take_fraction=.5, timeframes=TIMEFRAMES[3:], liquidity_growth=True)
DOGE_LONG_TF = FormulaSpec('DOGE_MAXPAIN_DIST15_25_LONG_TF', 'c39e5acaacf1247f', 'DOGE', 1.5, 2.5,
                          take_fraction=.5, timeframes=TIMEFRAMES[3:])
XRP_SHORT_TF = FormulaSpec('XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF', '8e6bdb02709aa0cc', 'XRP', 2, 4,
                          entry_adverse=.5, direction='LONG', timeframes=TIMEFRAMES[:3], liquidity_growth=True)
ETH_LONG = FormulaSpec('ETH_MAXPAIN_LONG_DIST1_3', '88a0ed51ab391d6e', 'ETH', 1, 3,
                       take_fraction=.5, direction='LONG', liquidity_growth=True)
SPECS = {s.coin: s for s in (SOL_RANGE24, HYPE_LONG_TF, DOGE_LONG_TF, XRP_SHORT_TF, ETH_LONG)}
