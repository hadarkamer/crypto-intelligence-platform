# Historical Magnet source recovery

`research_telegram_archive_magnet_recovery.py` adds an isolated extraction
contract, `telegram-magnet-source-recovery-v1`. It does not modify the original
per-message v1 parser, reconstructed SQLite runs, HYPE supplement, price source
contracts, production tables or formula eligibility.

The parser reads an exact upper/lower heading as the target price direction:
UPPER is LONG and LOWER is SHORT. Max Pain's hurt-side inversion does not apply.
It records the printed Magnet Quality, Liquidity Edge, spread, range, timeframes,
confirmation text and signed Price+OI/Futures CVD/Spot scores with their exact
source lines. It does not recompute historical confirmation using today's rules.
Conflicting printed assets, headings, scores or ranges remain blocked.

An asset must appear explicitly in this message as `נכס BTC` or `נכס: BTC`.
A nearby header, matching price scale, previous import association and Telegram's
visual `joined` CSS class are not accepted as asset evidence. This policy cannot
be overridden with a context parameter. Source-only recovery creates no outcomes
and does not make records eligible for formula training or live alerts.

## Run against an existing reviewed intake

```bash
python research_telegram_archive_magnet_recovery.py \
  --stage-dir /path/to/prepared_stage \
  --expected-stage-digest REVIEWED_STAGE_SHA256 \
  --output-dir /path/to/magnet_recovery
```

The full stage digest is checked before parsing. At most 20,000 source messages
are accepted. The output consists of a deterministic, versioned per-event JSONL
and a report that counts recovered fields and every remaining block reason.
Each event retains the original source identity, revision, time, source-file
references and v1 event key. The original intake and research run remain intact.

## Verified September 7 intake result

For stage digest
`92939e9facb691381fd14d4117b85377903cabc130c2945d060cb9eafda48c56`:

| Observation | Source messages |
| --- | ---: |
| All intake messages checked | 14,354 |
| Magnet targets checked | 2,241 |
| Printed upper/ LONG direction recovered | 997 |
| Printed lower/ SHORT direction recovered | 1,244 |
| Quality, edge, spread, target range, timeframes and three derivative scores present | 2,241 |
| Explicit asset in target message | 0 |
| Still blocked from price measurement for missing asset evidence | 2,241 |
| Legacy associations preserved but not treated as proof | 50 |
| New outcomes or production rows | 0 |

The source header for `message21506` prints BTC, but the following target
`message21507` does not. In original `messages19.html`, both carry the same
`joined` CSS class as an earlier SOL Combined message, proving that this class
does not identify a single-asset report. The inspected HTML pages 19–31 contain
no standard reply or explicit grouping/link markers. Pages 32–34 were not
structurally audited: downloading the original HTML/ZIP returned HTTP 502.
All 2,241 target texts were nevertheless inspected from the verified staging
artifact. This is an explicit limitation, not a claim that unavailable original
source evidence can never be recovered.

The report emitter must print the asset on **every** Magnet target to avoid
creating the same gap in future exports. Its separate introductory header may
remain for readability.

## Validation

```bash
python research_telegram_archive_magnet_recovery_selftest.py
```

The focused tests cover exact direction, component totals, zero values, source
immutability, distinct versioned identity, missing assets, rejected adjacency
and legacy links, malformed/conflicting evidence, and stage digest protection.
