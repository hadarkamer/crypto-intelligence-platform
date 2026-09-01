# PREVIEW_ONLY staging activation checklist

This checklist prepares the private experimental Telegram route without
activating delivery, Stage 6, public opt-in, research evidence, or any LIVE
effect.

## Fixed staging target

| Field | Value |
| --- | --- |
| Render service | `crypto-ai-agent-candidate` |
| Service ID | `srv-da3bd1lg1s2s73d867qg` |
| Branch | `ai-production-analytics` |
| Start command | `python ai_candidate_main.py` |

Do not apply these settings to either production service.

## Safe configuration-only state

Use the Render Dashboard and choose **Save only**. Do not rebuild, deploy, or
restart the service during this step.

Set only these non-secret safety controls:

```text
FORMULA_PREVIEW_STAGING_ENABLED=0
FORMULA_PREVIEW_STAGING_KILL_SWITCH=1
FORMULA_PREVIEW_STAGING_OWNER_APPROVED=0
```

Leave the following values unset until their exact approved values are known:

```text
FORMULA_PREVIEW_STAGING_TEST_CHAT_ID
FORMULA_PREVIEW_STAGING_RUNTIME_COMMIT
FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_ID
```

Never invent, guess, or copy a production chat ID into the staging test-chat
field.

## Values required before any later activation decision

| Environment variable | Required value | Validation |
| --- | --- | --- |
| `FORMULA_PREVIEW_STAGING_TEST_CHAT_ID` | Exact private staging chat ID | Non-zero integer |
| `FORMULA_PREVIEW_STAGING_RUNTIME_COMMIT` | Commit actually deployed to the staging service | 40 lowercase hexadecimal characters |
| `FORMULA_PREVIEW_STAGING_ACTIVATION_APPROVAL_ID` | Separately approved activation record ID | 64 lowercase hexadecimal characters |

These values only complete prerequisites. The current code contract is
`CONFIGURE_ONLY_ACTIVATION_FORBIDDEN`, so even a complete configuration keeps
`effective_enabled`, connector registration, and delivery forced off.

## Verification after a separately approved deployment

The `/health` response must report a `preview_staging` object with all of these
safety properties:

```text
effective_enabled=false
kill_switch_engaged=true
connector_registration_allowed=false
delivery_allowed=false
public_opt_in=false
stage6_activated=false
research_evidence_effect=NONE
live_effect=NONE
```

The health response may expose whether a test chat is configured and its hash,
but it must not expose the raw chat ID or activation approval ID.

## Separate authorization boundary

Changing the configuration-only state does not authorize a deployment, a Bot
API call, a Telegram message, activation, or Stage 6. Each of those requires a
later explicit decision. The Stage 5 READY/WAITING_DATA check remains independent
and cannot activate this route automatically.
