# AI Research Analytical Method — Candidate

Status: research specification. It does not authorize automatic strategy changes.

## Objective

The AI research layer must use analytical judgment, not only summarize individual indicators. Its main goal is to discover robust combinations, sequences, repetitions, timing relationships and inverse relationships that are associated with a high probability of movement toward a direction or target.

Preferred discoveries are patterns that combine:
- high directional success;
- low adverse movement before the expected move;
- short time to first meaningful progress;
- strong/consistent progress toward the expected target;
- repeatability across enough observations and, where appropriate, across coins or market regimes.

The AI should spend more research effort on strong combinations that repeatedly worked and on improving useful directional indicators, while still analyzing failures and counterexamples so weak conditions and invalidation patterns are understood.

## Evidence families that must be available to research

Research must be able to include both standalone alerts and compound confirmations, including at minimum:
- Futures CVD alerts and their values/scores;
- Spot CVD alerts and their values/scores;
- Price + OI alerts/regimes and values;
- Magnet alerts;
- Magnet Confirmation;
- Strong Magnet Confirmation;
- Combined Confirmation and each independent component that formed it;
- Max Pain Confirmation;
- Strong Max Pain Confirmation;
- Max Pain score/components and target/distance data;
- repeated occurrences of the same alert and repeated/clustered occurrences of different alerts;
- current bot-defined Max Pain averages/baselines where they exist;
- additional derived averages/baselines created by the AI research layer, as candidate research features rather than production rules.

Future timestamped external context may also be included, such as exchange/derivatives conditions, ETF flows, macro data and global news, provided it was available at or before the researched decision time.

## Required analytical behavior

### 1. Cross-signal analysis

Do not evaluate each signal in isolation. Search for interactions such as:
- two or more indicators agreeing;
- one strong indicator offsetting one weaker opposite indicator;
- a particular component becoming useful only when another component is present;
- contradictory combinations that systematically precede reversal, delay or failure;
- different combinations for different coins, regimes or time horizons.

### 2. Sequence and timing analysis

Order matters. Research sequences such as:
- alert A followed by alert B within a specific time window;
- a strong alert weakening and then becoming a better delayed entry/confirmation later;
- a new opposite indicator appearing after an earlier alert and acting as an inverse signal;
- repeated alerts accumulating evidence over time;
- the time gap between repeated or different alert types.

Do not collapse events that occurred at different times into one static snapshot.

### 3. Repetition / density analysis

Measure whether the same alert, related alerts or independent confirming alerts occurring multiple times within a bounded interval materially changes the outcome probability.

Candidate features may include:
- repeat count;
- number of distinct alert families;
- time between repetitions;
- weighted recency of repeated signals;
- whether repeated alerts strengthened, weakened or changed direction.

### 4. Inverse-signal discovery

Explicitly search for indicators or transitions that are more useful in the opposite direction than their original interpretation suggests.

Examples to test, never assume:
- a new signal that frequently precedes failure of the active direction;
- exhaustion after an unusually strong reading;
- a strong alert whose best opportunity occurs only after its own strength decays;
- a transition from one category/state to another that acts as a reversal warning.

Any inverse interpretation must be supported by historical evidence, sample size and validation; it must not be inferred from a few anecdotes.

### 5. Max Pain target progress

Do not classify a Max Pain alert only as exact target hit vs miss.

Research must preserve:
- initial target;
- initial distance to target;
- maximum progress toward the target;
- percentage of initial target distance completed;
- minimum remaining distance to target;
- whether the target was reached exactly/within an approved tolerance;
- time to first meaningful progress;
- time to closest approach;
- adverse movement before progress.

A move substantially toward the Max Pain target can therefore be informative even if the exact target was not touched.

No final tolerance or success threshold is hard-coded by this document; those values are research candidates until Yoni freezes them.

### 6. Path quality and speed

For every alert/formula candidate, evaluate not only final direction but the path taken after the event.

Important outcome measures include:
- return at fixed horizons (for example 1h/4h/12h/24h when relevant);
- MFE: maximum favorable excursion;
- MAE: maximum adverse excursion;
- time to first meaningful favorable movement;
- time to MFE / target / closest target approach;
- directional progress before a defined adverse move;
- Max Pain target-progress ratio when a Max Pain target exists.

Preferred formulas are those that achieve strong favorable progress quickly and with relatively little movement in the opposite direction.

### 7. Formula / candidate-pattern discovery

The AI may propose candidate formulas that combine features, for example:
- alert types/categories;
- component scores;
- direction agreement/disagreement;
- repeat counts and recency;
- sequence/order;
- averages/baselines;
- Max Pain distance/progress;
- time-to-event features;
- market/external context once available.

A candidate formula must be reported with its conditions in reproducible terms rather than as vague prose.

For each proposed formula, report when possible:
- sample count;
- directional hit rate or other success definition used;
- median/average favorable movement;
- median/average adverse movement;
- median/average time to meaningful progress;
- target/partial-target performance where applicable;
- relevant coins/regimes/timeframes;
- strongest counterexamples/failure conditions;
- comparison against a reasonable baseline.

### 8. Strong-pattern priority without ignoring failure

Research prioritization should favor:
1. strong repeated patterns that worked;
2. improvements/filters that make useful directional indicators more reliable;
3. combinations producing fast progress and low adverse excursion;
4. inverse or delayed-use patterns with strong evidence;
5. failure analysis used to refine conditions, exclusions and invalidation logic.

The system should not spend most of its compute merely cataloguing weak patterns that do not improve decision quality.

## Statistical / research integrity

The AI must distinguish pattern discovery from proof.

Required safeguards:
- always report sample size;
- avoid conclusions from tiny samples;
- prevent look-ahead bias: use only evidence available at the event time when evaluating a decision rule;
- preserve strategy/code version because historical alerts may have been generated by different logic;
- keep successful and failed events;
- segment by coin/regime/timeframe when pooling would hide meaningful differences;
- compare candidate formulas to simpler baselines;
- whenever enough data exists, validate discoveries on later/out-of-sample data or a holdout period before recommending implementation;
- treat AI-created averages/thresholds/formulas as Candidate research outputs until separately approved and tested.

Correlation alone is not permission to change production logic.

## Research Event requirements implied by this methodology

Each compact Research Event must preserve the non-reconstructable state needed to reproduce the alert later, including where available:
- exact UTC alert timestamp;
- symbol, direction, timeframe and alert type/category;
- headline/overall score;
- all component scores used at that moment;
- independent alert flags active at that moment;
- Confirmation / Strong Confirmation states;
- Magnet / Magnet Confirmation / Strong Magnet Confirmation states;
- Combined Confirmation and the exact components that formed it;
- Price+OI state/score identifiers;
- Futures CVD and Spot CVD state/score identifiers;
- Max Pain target, current price, distance/proximity and relevant score/components;
- internal averages/baselines that cannot be reconstructed safely after strategy changes;
- strategy version and code version;
- deterministic fingerprint for deduplication.

Large raw time series, news bodies and external-market histories must not be duplicated into each event. They remain timestamped in their own data stores and are joined to the event by time/source/symbol when researched.

## Safety boundary

The AI may discover, rank and explain candidate formulas and recommend Candidate tests. It must not autonomously alter production scores, thresholds, confirmation rules, strategy logic or trading behavior.
