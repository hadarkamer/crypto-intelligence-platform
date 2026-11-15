# New HYPE source adapter — staged, not live enabled

Operational identity: `HYPE_WATCH_LIQ4050_24H_INVERSE`. It does not alias or inherit the old HYPE formula's events, accounting, allocation, or manual choices.

The immutable source specification is `Formula_Activation_Handoff_20261115.json`. Existing three-formula production admission remains unchanged by this staged code. The new identity is absent from the enabled roster until its missing evidence and explicit live risk decision are resolved.

## Captured source and event selection

The immediate Watch decision bundle does not contain the required BTC parent membership. The adapter reads the existing `research_watch_scan_intakes`, `research_watch_scan_tf_formula_samples`, `research_watch_scan_measurements` and `research_watch_scan_observations` route instead. No new collector, scheduler or market call is added. Measurement membership must be LIVE under `btc-parent-close-reversal-200bps-v1`; the overall measurement need not be READY after its 24-hour research outcome horizon.

The adapter binds consumer/population/version, snapshot identity, HYPE, source hashes, original availability, measurement entry clock and Hyperliquid price route. It revalidates the exact captured 24h raw row and score slots with the existing `research_watch_scan_formula_timeframe` selection/liquidity helpers. That validator has the handoff's SHA256 `135e96e317cccf57d2a8a6397a244032a5e78db8b98d1b88b6fb92586d454953`. The SQL projection deliberately omits unrelated timeframe evaluations; it does not claim to recompute the hash of the complete projected-away feature payload. It verifies original intake bundle/parent bindings and independently revalidates all new-HYPE relevant raw selection, amount, score/component, direction and predicate fields.

The raw selected SHORT side is an upper target, maps to source LONG, then executes inverse SHORT. The predicate is `40 <= near_share_pct < 50`, with verified finite raw amounts and matching ratio; there are no OI/CVD or additional score thresholds. Missing/unverified inputs are UNKNOWN. A side not selected by the real scorer is NOT_APPLICABLE.

All available observations in each relevant existing BTC parent wave are read, including observations before formula activation and missing/non-READY timeframe rows. The first MATCH is ordered by original usability then snapshot ID. Earlier or simultaneous UNKNOWN blocks that representative; later UNKNOWN does not. A first match rejected for capacity, pre-activation or an expired entry window cannot be replaced with a later match from that wave. Missing parent identity cannot be assigned to an invented group.

The new identity has its own `hype_activated_ms` fence in the existing state. It is set once, under the existing row lock, only after both source policy and execution admission explicitly enable the new identity. The old shared activation time, histories and configuration hash remain unchanged. Pre-activation captures cannot become new HYPE approvals after enablement or restart.

Wave consumption is stored in the existing selected-formula state. Open waves survive long gaps and restart. Consumed-wave metadata is compacted only after the existing parent table proves the wave ended more than the unchanged source freshness window ago; permanent outbox receipts and historical plans are retained. The existing candidate bound also prevents this state from growing until independent formulas cannot save.

## Planned reference and proposed live adaptation

The original reference minute is strictly after `max(intake usable time, timeframe sample created time, timeframe sample updated time)`. A later poll or measurement update never moves that clock. Its exact Hyperliquid HYPE perpetual OPEN is the planned E; SL is E×1.02 and TP E×0.98. The target is not the Max Pain target.

The staged contract is `NEXT_FULL_MINUTE_IOC`, with one manager-owned IOC attempt at the frozen E during that original minute. Admission expires at that minute's end; it never becomes a late GTC order and is not repriced to obtain a fill. This is an explicit conservative live adaptation, not proof of a market fill at the research OPEN. Actual exchange fills and their clocks remain authoritative. A 1,440-minute position limit belongs to the manager from its first proved actual fill; it is separate from this one-minute admission/entry deadline.

The existing measurement worker can make membership available only after the research entry minute has closed. Such a source remains expired; the adapter does not shift its clock. This is an operational readiness constraint to measure before activation, not permission to silently lose every HYPE event or invent an earlier receipt.

## Evidence boundary

Synthetic product-shaped unit and isolated PostgreSQL integration fixtures are distinct from frozen historical research cases. The supplied archive references `Seven_Formulas_Update_20261115_Evidence.zip` and `Formula_Research_Complete_Evidence_20261114.zip` were inaccessible in this session. The required 32 included cases, six UNKNOWN-blocked waves and two capacity rejections have therefore not been replayed. No live risk amount is supplied by this adapter, and research $5 is not used as an allocation.
