"""Candidate GPT agent for the crypto bot.

This module provides a conversational OpenAI Responses API client plus a bounded
read-only tool loop.  It is intentionally isolated from the production trading
logic.  The candidate can inspect current and historical bot state, but cannot
mutate strategy, alerts, watches, schedules, code, or trading settings.
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
3. Research assistant: use the approved historical tools to analyze stored Price/OI, Futures CVD, Spot CVD, OI regime and technical-signal history. You may inspect evidence nearest to an exact historical timestamp.
4. Future alert-performance researcher: timestamped Research Events and external exchange/news context are planned but are not yet being written. Do not pretend alert-outcome or news-context history exists before those tools are connected.

Candidate safety boundary:
- This version is READ ONLY.
- Never claim you changed a score, threshold, confirmation rule, Watch, schedule, strategy, database schema or code.
- Never place a trade or imply that you did.
- If the user asks for a change that requires a write-capable tool, explain that the current candidate can analyze or propose the change but cannot execute it yet.
- Use tools when a factual answer depends on live/current or historical bot data. Do not invent market values.
- Clearly separate observations from inference.
- Preserve and report exact timestamps when the question concerns an event or historical point.
- Historical market evidence is not the same as historical alert performance. Do not infer alert accuracy from market-history tools alone.
- External exchange/index data and global news are not available until dedicated time-stamped context sources are connected.
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
    """Small local chat memory; intentionally not persisted in the candidate stage."""

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
        "mode": "candidate_read_only",
        "tools": ai_tools.tool_names(),
        "memory": "in_process_bounded_not_persistent",
    }
