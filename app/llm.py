"""LLM access with token accounting and a hard daily budget guard.

Every call routes through here so spend is measurable and enforceable. The
budget ledger is process-local, which is only correct for a single-instance
PoC; see SUBMISSION.md for the production shape.
"""
import json
import threading
from dataclasses import dataclass, field
from datetime import date

import httpx

from app.config import settings


class BudgetExceeded(RuntimeError):
    """Raised when a call would push spend past the daily ceiling."""


@dataclass
class Usage:
    llm_calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0

    def add(self, other: "Usage") -> None:
        self.llm_calls += other.llm_calls
        self.prompt_tokens += other.prompt_tokens
        self.completion_tokens += other.completion_tokens
        self.cost_usd += other.cost_usd

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


def price(prompt_tokens: int, completion_tokens: int) -> float:
    return (
        prompt_tokens * settings.price_per_1m_input
        + completion_tokens * settings.price_per_1m_output
    ) / 1_000_000


def usage_from_body(body: dict) -> Usage:
    """Build a Usage from a provider response.

    Reasoning models bill for thinking tokens but report them in neither
    `completion_tokens` nor `prompt_tokens` -- gemini-3.x returns
    completion_tokens=0 alongside total_tokens=100. Trusting the reported
    completion count would understate spend by an order of magnitude and
    silently defeat the budget ceiling, so reconcile against total_tokens and
    treat the unexplained remainder as billable output.
    """
    raw = body.get("usage") or {}
    prompt_tokens = int(raw.get("prompt_tokens", 0))
    reported = int(raw.get("completion_tokens", 0))
    total = int(raw.get("total_tokens", 0))

    completion_tokens = max(reported, total - prompt_tokens if total else 0)

    return Usage(
        llm_calls=1,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_usd=price(prompt_tokens, completion_tokens),
    )


@dataclass
class _Ledger:
    day: date = field(default_factory=date.today)
    spent_usd: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _roll(self) -> None:
        today = date.today()
        if today != self.day:
            self.day = today
            self.spent_usd = 0.0

    def check(self) -> None:
        with self._lock:
            self._roll()
            if self.spent_usd >= settings.daily_budget_usd:
                raise BudgetExceeded(
                    f"Daily LLM budget of ${settings.daily_budget_usd:.2f} exhausted "
                    f"(${self.spent_usd:.4f} spent)."
                )

    def record(self, cost: float) -> None:
        with self._lock:
            self._roll()
            self.spent_usd += cost

    def snapshot(self) -> dict:
        with self._lock:
            self._roll()
            return {
                "day": self.day.isoformat(),
                "spent_usd": round(self.spent_usd, 6),
                "budget_usd": settings.daily_budget_usd,
                "remaining_usd": round(max(0.0, settings.daily_budget_usd - self.spent_usd), 6),
            }


ledger = _Ledger()


def chat(
    messages: list[dict],
    *,
    tools: list[dict] | None = None,
    max_tokens: int = 512,
    temperature: float = 0.0,
    force_json: bool = False,
) -> tuple[dict, Usage]:
    """One chat completion. Returns the assistant message and its usage."""
    ledger.check()

    payload: dict = {
        "model": settings.llm_model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    if force_json:
        payload["response_format"] = {"type": "json_object"}

    response = httpx.post(
        f"{settings.llm_base_url.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {settings.llm_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=settings.llm_timeout_seconds,
    )
    response.raise_for_status()
    body = response.json()

    usage = usage_from_body(body)
    ledger.record(usage.cost_usd)

    return body["choices"][0]["message"], usage


def extract_json(raw: str) -> dict | None:
    """Parse a JSON object from a model response, tolerating markdown fences."""
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines[1:]).strip()

    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end <= start:
            return None
        try:
            parsed = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None

    return parsed if isinstance(parsed, dict) else None
