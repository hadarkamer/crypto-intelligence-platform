PROJECT: Crypto Intelligence Platform

GENERAL
- Make only minimal, targeted changes.
- Do not refactor unrelated code.
- Preserve backward compatibility unless a task explicitly requires otherwise.
- Never assume a discussed feature exists; verify it in the current repository.
- The current repository code is the primary source of truth.

SAFETY / STABILITY
- Before changing scheduling or startup logic, inspect all related tasks and call paths.
- Avoid duplicate scheduled tasks, duplicate loops, race conditions, deadlocks and overlapping collectors.
- Database/schema initialization and DDL must not run inside recurring jobs.
- Initialization should happen only in the appropriate startup flow.
- Never introduce a new recurring task without checking for an existing equivalent task.
- Do not silently change collector intervals, alert thresholds, scoring thresholds or data freshness rules.

BOT ARCHITECTURE
Whenever a change may affect one of these areas, check its impact on the others:
- Max Pain / liquidity
- Magnet Engine
- OI
- Futures CVD
- Spot CVD
- Price+OI regime
- Derivatives logic
- Confirmations / Strong Confirmations
- timeframe families
- alerts
- watch systems
- Telegram output
- database
- collectors
- scheduling/concurrency
- data providers/fallbacks

SCORING
- Do not change scoring formulas, weights, thresholds or normalization unless explicitly requested.
- Avoid double-counting correlated signals.
- Keep LONG and SHORT computations separated until the intended selection stage.
- If data is missing, stale, or unavailable, report that state; never fabricate substitute evidence.

TESTING
- Before delivering a code change, run the existing relevant tests.
- Identify which tests are relevant before editing code.
- If behavior changes and an appropriate regression test is missing, propose or add a targeted test.
- Do not weaken an existing test simply to make a change pass.

DEPENDENCIES
- Do not add or upgrade production dependencies without explicit approval.
- Do not change environment variables or deployment configuration silently.

ZIP / RELEASE RULES
If asked to generate a ZIP:
- Project root files must appear directly at ZIP root.
- Never add an extra wrapper directory.
- Exclude __pycache__, .pyc, caches, temp files and system files.
- Do not create a new data directory unless explicitly required.

DELIVERY
After any implementation task report:
1. files changed
2. exact behavioral changes
3. tests executed and results
4. commands added or changed
5. environment variable changes
6. deployment/Render/DB/Telegram implications
7. unresolved risks or validation still required

CODE REVIEW RULES
- Flag duplicate background tasks or scheduler loops.
- Flag DB initialization or DDL inside recurring work.
- Flag changes that alter scoring/threshold behavior unintentionally.
- Flag changes that cause one market-data failure to crash unrelated engines.
- Flag hidden fallbacks that change semantics without being surfaced.
- Flag concurrency changes that may block collectors or alert processing.

USER COMMUNICATION
- All user-facing explanations, summaries, questions, recommendations, risk descriptions, test results, and handoff notes must be written in Hebrew.
- Keep code, filenames, function names, commands, logs, stack traces, API names, and technical identifiers in their original English.
- When using technical terminology, explain it in clear and practical Hebrew when needed.
- For unfamiliar workflows or consequential actions, explain the next step in small, clear steps and do not rush ahead.
- Do not perform a production-impacting or irreversible action without clearly explaining it first.
