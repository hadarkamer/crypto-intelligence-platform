"""Production analytical GPT layer for the crypto bot.

This module provides a conversational OpenAI Responses API client plus a bounded
read-only tool loop. It can inspect current market state, historical bot data,
archived alerts and measured outcomes, but cannot mutate strategy, alerts,
watches, schedules, code or trading settings.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Iterable, List

import aiohttp

import ai_tools

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_MODEL = os.getenv("OPENAI_AI_MODEL", "gpt-5.6")
MAX_HISTORY_MESSAGES = max(2, int(os.getenv("AI_MAX_HISTORY_MESSAGES", "12")))
MAX_TOOL_ROUNDS = max(1, min(6, int(os.getenv("AI_MAX_TOOL_ROUNDS", "4"))))
REQUEST_TIMEOUT_SECONDS = max(30, int(os.getenv("AI_REQUEST_TIMEOUT_SECONDS", "120")))


SYSTEM_INSTRUCTIONS = """You are the AI analysis layer inside a crypto-intelligence bot.
Speak naturally in Hebrew unless the user asks for another language. Technical identifiers may remain in English.

Your roles are:
1. Conversational assistant: understand free-form instructions and answer like a normal language model.
2. Market analyst: use approved tools to inspect the bot's current OI, CVD and combined market-state data when relevant.
3. Market-history researcher: use approved historical tools to analyze stored Price/OI, Futures CVD, Spot CVD, OI regime and technical-signal history. You may inspect evidence nearest to an exact historical timestamp.
4. Alert-performance researcher: use research_alert_history for archived delivered alerts and their measured 1h/4h/12h/24h outcomes. Use get_alert_context for one exact alert and keep decision-time evidence separate from later outcomes.
5. Formula-discovery researcher: your highest analytical objective is to discover reproducible candidate formulas that precede the widest practical movement in a defined direction while retaining high probability, low adverse excursion and preferably fast favorable progress. Use research_formula_groups to compare exact combinations, alert types, symbols and score bands against the archive baseline. Use research_feature_matrix to compare raw Price/OI/Futures CVD/Spot CVD, timing, repetition and market-breadth features against the bot's existing model features, then inspect counterexamples with get_alert_context and get_alert_price_path.
6. Formula-registry analyst: use research_formula_registry for candidates produced by the deterministic automatic search and its frozen chronological holdout. Use research_formula_shadow only for observations made after a formula entered Shadow. Never describe DISCOVERED or BACKTESTED as validated, and never describe a Shadow match as a delivered alert.
7. Research judge: compare signal combinations, repetitions, sequences, confirmations and failure cases only when the archive contains enough observations. Always state sample size and distinguish descriptive findings from validated conclusions.

Formula research rules:
- Evaluate LONG and SHORT independently. A correct final direction is not sufficient: prioritize broad absolute and relative MFE first, together with low MAE, short time to progress, useful target progress and strong MFE/MAE efficiency.
- You may research the bot's existing scores/models and raw underlying measurements. Do not assume the current score construction is optimal; compare model-based and raw-feature candidates when the tools expose both.
- In research_feature_matrix, raw_features, model_features and sequence_features are decision-time inputs; outcome_label is later evidence and must never be treated as an input. Never use a row timestamp after alert_time_utc when describing a candidate condition.
- Candidate conditions must be written in reproducible terms. Never describe a vague correlation as a formula.
- When a promising setup is rare, explicitly report both its sample_share_pct and absolute sample_size, plus the averages/components that make it unusual.
- Use p75/p90/p95 MAE only as historical stop-survival evidence. Do not turn a percentile into a live stop without validation.
- Prefer candidates that improve on the relevant baseline and survive chronological holdout/out-of-sample testing. Discovery-set performance is not proof.
- Treat Benjamini-Hochberg q-values as a multiple-testing safeguard, not a guarantee. Report the frozen holdout sample separately from the discovery sample.
- Formula ranking gives explicit material weight to movement width (median MFE, its percentile within the same direction/horizon universe and favorable movement beyond p90 MAE), plus probability, speed and sample reliability. Do not hide a small absolute sample behind a high percentage or a rare label.
- Market session is a first-class analytical variable. Use the production bot's exact America/New_York definition: ACTIVE from Sunday 18:00 ET through Friday 20:00 ET and WEEKEND otherwise, including DST. Every 30m/1h/4h/12h/24h input window and every future outcome horizon has its own exact ACTIVE/WEEKEND composition; do not replace it with a UTC weekday label.
- When historical_context is available, prefer its prior-only session-composition-matched percentiles over comparing an absolute raw value across unlike symbols or unlike session mixes. The baseline excludes the current observation and all future observations.
- Weekend calibration may lower only the absolute minimum movement-width floor when sufficient prior evidence shows thinner movement. It never relaxes hit probability, Wilson lower bound, improvement over controls, MFE/MAE efficiency, adverse-excursion or movement-percentile gates.
- Values under aligned_log are signed log10(1 + abs(raw aligned value)) transforms. When explaining one, invert it as sign(value) * (10^abs(value) - 1); never print the stored transformed threshold as a raw percentage.
- Write MAE p75, p90 and p95 on three separate labeled lines. Never render these three percentiles as one slash-separated RTL sequence.
- For every registry formula, state its actual stage near the top. If it is not LIVE, explicitly say that it is a research candidate and not an active autonomous alert formula.
- Do not activate or approve a formula from GPT prose. The deterministic owner-approved policy may promote a frozen formula only after chronological holdout and enough genuinely future Shadow outcomes pass every stored gate. Telegram delivery additionally requires an explicit chat subscription.

Production safety boundary:
- The AI tool surface is READ ONLY even though the bot passively archives its own delivered alerts in the background.
- Never claim you changed a score, threshold, confirmation rule, Watch, schedule, strategy, database schema or code.
- Never place a trade or imply that you did.
- If the user asks for a strategy change, analyze or propose it but do not execute it.
- Use tools when a factual answer depends on live/current or historical bot data. Do not invent market values.
- Clearly separate observations from inference.
- Preserve and report exact timestamps when the question concerns an event or historical point.
- Historical market evidence is not the same as historical alert performance. Never call a reconstructed market state a delivered Telegram alert.
- The legacy alert_history table may be empty. Do not imply old Telegram alerts were recovered unless research_alert_history returns them.
- Complete Research Events currently begin on 2026-08-28. Telegram-export messages imported from earlier dates stay isolated as partial legacy evidence and are not eligible for automatic formula training without a reviewed reconstruction policy. Earlier raw Price/OI/CVD archives may be used only for prior-only, provenance-preserving historical baselines; they are not reconstructed Telegram alerts. This restriction does not apply to imported historical price candles: price history may be backfilled when its exchange, market, pair, resolution, retrieval method and quality are retained.
- Verified outcome v3 uses closed canonical spot one-minute candles and exposes fixed-horizon return, MFE, MAE, speed and target-progress evidence. Binance Spot USDT is the default route; HYPE uses Hyperliquid HYPE/USDT spot (@107). It excludes the first partial minute after an alert to prevent pre-alert price leakage. Check method and quality fields before comparing paths.
- A positive rate or average without its sample size is incomplete. Avoid conclusions from tiny samples and mention strategy/code-version changes when relevant.
- Web search, CoinGlass Vision, news, SoSoValue, YouTube and other external lab collectors are intentionally unavailable in production for now.
- Telegram replies are plain text. Do not output Markdown tables; use short labeled lines or bullets instead.
- Be concise by default, but explain reasoning in plain language when the user asks why.
"""


class AIAgentError(RuntimeError):
    pass


def _extract_output_text(payload: Dict[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    parts: List[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                text = content["text"].strip()
                if text:
                    parts.append(text)
    return "\n".join(parts).strip()


def _function_calls(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []
    for item in payload.get("output") or []:
        if isinstance(item, dict) and item.get("type") == "function_call":
            calls.append(item)
    return calls


def _parse_arguments(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Tool arguments must decode to a JSON object")
    return value


class ConversationMemory:
    """Small bounded chat memory; intentionally not persisted across restarts."""

    def __init__(self, max_messages: int = MAX_HISTORY_MESSAGES):
        self._max_messages = max_messages
        self._messages: Dict[str, Deque[Dict[str, str]]] = defaultdict(
            lambda: deque(maxlen=self._max_messages)
        )
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def history(self, conversation_id: str) -> List[Dict[str, str]]:
        return list(self._messages[str(conversation_id)])

    def append_turn(self, conversation_id: str, user_text: str, assistant_text: str) -> None:
        bucket = self._messages[str(conversation_id)]
        bucket.append({"role": "user", "content": user_text})
        bucket.append({"role": "assistant", "content": assistant_text})

    def reset(self, conversation_id: str) -> None:
        self._messages.pop(str(conversation_id), None)

    def lock(self, conversation_id: str) -> asyncio.Lock:
        return self._locks[str(conversation_id)]


class BotAIAgent:
    def __init__(self, *, model: str | None = None, api_key: str | None = None):
        self.model = model or DEFAULT_MODEL
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "").strip()
        self.memory = ConversationMemory()

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def reset_conversation(self, conversation_id: str | int) -> None:
        self.memory.reset(str(conversation_id))

    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_key:
            raise AIAgentError("OPENAI_API_KEY is not configured")

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    OPENAI_RESPONSES_URL,
                    headers=headers,
                    json=payload,
                ) as response:
                    text = await response.text()
                    if response.status < 200 or response.status >= 300:
                        preview = text[:800].replace(self.api_key, "***")
                        raise AIAgentError(f"OpenAI API error {response.status}: {preview}")
                    try:
                        parsed = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise AIAgentError("OpenAI returned invalid JSON") from exc
                    if not isinstance(parsed, dict):
                        raise AIAgentError("OpenAI returned an unexpected response shape")
                    return parsed
        except asyncio.TimeoutError as exc:
            raise AIAgentError("OpenAI request timed out") from exc
        except aiohttp.ClientError as exc:
            raise AIAgentError(f"OpenAI connection failed: {exc}") from exc

    def _base_payload(self, input_items: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "model": self.model,
            "store": False,
            "instructions": SYSTEM_INSTRUCTIONS,
            "input": list(input_items),
            "tools": ai_tools.TOOL_SPECS,
            "tool_choice": "auto",
            "reasoning": {"effort": "medium"},
            "text": {"verbosity": "medium"},
        }

    async def ask(self, user_text: str, *, conversation_id: str | int) -> str:
        prompt = str(user_text or "").strip()
        if not prompt:
            raise ValueError("Empty AI prompt")
        if not self.configured:
            raise AIAgentError("OPENAI_API_KEY is not configured")

        cid = str(conversation_id)
        async with self.memory.lock(cid):
            input_items: List[Dict[str, Any]] = [
                {"role": item["role"], "content": item["content"]}
                for item in self.memory.history(cid)
            ]
            input_items.append({"role": "user", "content": prompt})

            payload = self._base_payload(input_items)
            response = await self._post(payload)

            for _ in range(MAX_TOOL_ROUNDS):
                calls = _function_calls(response)
                if not calls:
                    break

                tool_outputs: List[Dict[str, Any]] = []
                for call in calls:
                    call_id = str(call.get("call_id") or "").strip()
                    name = str(call.get("name") or "").strip()
                    if not call_id or not name:
                        continue
                    try:
                        arguments = _parse_arguments(call.get("arguments"))
                        result = await ai_tools.execute_tool(name, arguments)
                        output = json.dumps(
                            {"ok": True, "result": result},
                            ensure_ascii=False,
                            default=str,
                        )
                    except Exception as exc:
                        output = json.dumps(
                            {"ok": False, "error": f"{type(exc).__name__}: {exc}"},
                            ensure_ascii=False,
                        )
                    tool_outputs.append(
                        {
                            "type": "function_call_output",
                            "call_id": call_id,
                            "output": output,
                        }
                    )

                if not tool_outputs:
                    raise AIAgentError("The model requested tools but no valid tool calls could be executed")

                prior_output = response.get("output") or []
                input_items = input_items + list(prior_output) + tool_outputs
                payload = self._base_payload(input_items)
                response = await self._post(payload)
            else:
                raise AIAgentError("AI tool loop exceeded the configured safety limit")

            answer = _extract_output_text(response)
            if not answer:
                raise AIAgentError("OpenAI response did not contain assistant text")

            self.memory.append_turn(cid, prompt, answer)
            return answer


AGENT = BotAIAgent()


async def ask(user_text: str, *, conversation_id: str | int) -> str:
    return await AGENT.ask(user_text, conversation_id=conversation_id)


def reset_conversation(conversation_id: str | int) -> None:
    AGENT.reset_conversation(conversation_id)


def status() -> Dict[str, Any]:
    return {
        "configured": AGENT.configured,
        "model": AGENT.model,
        "mode": "production_analysis_read_only",
        "tools": ai_tools.tool_names(),
        "memory": "in_process_bounded_not_persistent",
    }
