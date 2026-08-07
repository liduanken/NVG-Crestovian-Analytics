"""Tiered triage.

    policy (free, deterministic)
      -> LLM (only on abstention)
        -> escalation gate (final safety check)

Tier 1 answers failures with a documented right answer. Tier 2 spends tokens
only on the ambiguous tail. Tier 3 guarantees that low confidence never turns
into autonomous action on a critical pipeline.

`_build_prompt` and `_parse_response` are retained from the starting
implementation because the provided tests pin their contract.
"""
import json

import httpx

from app.config import settings
from app.knowledge import Signals, build_signals, catalog_entry, run_history
from app.llm import BudgetExceeded, Usage, extract_json, ledger, usage_from_body
from app.models import CostReport, TriageDecision
from app.policy import PipelineStrategy, PolicyResult, get_strategy

VALID_ACTIONS = ("retry", "skip", "escalate")

# Preserved from the starting implementation; the provided tests reference these.
LLM_API_KEY = settings.llm_api_key
LLM_BASE_URL = settings.llm_base_url
LLM_MODEL = settings.llm_model


def _build_prompt(event: dict) -> str:
    """Assemble the tier-2 prompt.

    Only reached when the policy layer abstains, so it leads with the derived
    signals rather than making the model re-derive them from raw JSON.
    """
    pipeline_id = event.get("pipeline_id", "")
    entry = catalog_entry(pipeline_id)
    history = run_history(pipeline_id)
    signals = build_signals(event)
    strategy = get_strategy(signals.pipeline_type)

    parts = [
        "You are a data pipeline triage assistant for Crestovian Analytics.",
        "Deterministic policy rules could not settle this event, so it needs judgement.",
        "",
        "## Pipeline Event",
        json.dumps(event, indent=2, default=str),
        "",
        "## Derived Signals",
        json.dumps(signals.as_dict(), indent=2, default=str),
    ]

    if entry:
        parts += ["", "## Pipeline Catalog Entry", json.dumps(entry, indent=2)]

    if history:
        parts += [
            "",
            f"## Recent Run History (last {len(history)} runs)",
            json.dumps(history, indent=2),
        ]

    if strategy.guidance:
        parts += [
            "",
            f"## Guidance for pipeline type '{strategy.pipeline_type}'",
            strategy.guidance,
        ]

    parts += [
        "",
        "## Decision",
        "Choose one action:",
        "- retry: the failure is transient; a re-run is likely to succeed",
        "- skip: the failure is expected, covered by the next scheduled run, or the "
        "pipeline is deprecated / SLA already missed",
        "- escalate: the failure requires human intervention, or the evidence does not "
        "distinguish between plausible causes",
        "",
        "Report confidence honestly. Use 'low' when the available evidence cannot "
        "separate a benign cause from a harmful one. A confident wrong answer is worse "
        "than an escalation.",
        "",
        'Respond with JSON only: {"action": "retry"|"skip"|"escalate", '
        '"confidence": "high"|"medium"|"low", "reasoning": "<one or two sentences>", '
        '"evidence": ["<signal that drove the decision>"]}',
    ]

    return "\n".join(parts)


def _parse_response(raw: str) -> tuple[str, str]:
    """Extract (action, reasoning) from an LLM response, handling markdown fences."""
    parsed = extract_json(raw)
    if parsed is not None:
        action = str(parsed.get("action", "")).lower().strip()
        reasoning = str(parsed.get("reasoning", raw))
    else:
        # Free-text fallback, ordered escalate-first so an unparseable response
        # can never be read as authorisation to act.
        lower = (raw or "").lower()
        if "escalate" in lower:
            action = "escalate"
        elif "skip" in lower:
            action = "skip"
        elif "retry" in lower:
            action = "retry"
        else:
            action = "escalate"
        reasoning = raw

    if action not in VALID_ACTIONS:
        action = "escalate"

    return action, reasoning


def _parse_full(raw: str) -> tuple[str, str, str, list[str]]:
    action, reasoning = _parse_response(raw)
    parsed = extract_json(raw) or {}

    confidence = str(parsed.get("confidence", "")).lower().strip()
    if confidence not in ("high", "medium", "low"):
        confidence = "low"  # an unstated confidence is not a high one

    evidence = parsed.get("evidence") or []
    if not isinstance(evidence, list):
        evidence = [str(evidence)]

    return action, reasoning, confidence, [str(e) for e in evidence][:6]


def _call_llm(event: dict) -> tuple[str, Usage]:
    """Single completion for the ambiguous tail.

    Calls httpx directly rather than app.llm.chat so the provided tests, which
    patch app.triage.httpx.post, exercise the real code path.
    """
    ledger.check()
    prompt = _build_prompt(event)

    response = httpx.post(
        f"{settings.llm_base_url.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {settings.llm_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": settings.llm_model,
            "max_tokens": settings.max_triage_tokens,
            "temperature": 0.0,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
        },
        timeout=settings.llm_timeout_seconds,
    )
    response.raise_for_status()
    body = response.json()

    usage = usage_from_body(body)
    ledger.record(usage.cost_usd)

    choice = body["choices"][0]
    content = choice["message"]["content"]

    # A response cut off mid-JSON is not a decision. The keyword fallback would
    # happily read "retry" out of a half-written sentence, so refuse it here and
    # let the caller escalate.
    if choice.get("finish_reason") == "length" and extract_json(content) is None:
        raise ValueError(
            "model response truncated before producing valid JSON "
            f"(max_tokens={settings.max_triage_tokens})"
        )

    return content, usage


def _apply_gate(
    action: str,
    confidence: str,
    reasoning: str,
    signals: Signals,
    strategy: PipelineStrategy,
) -> tuple[str, str, str, bool, bool]:
    """Tier 3. Turns uncertainty into escalation instead of action.

    Returns (action, confidence, reasoning, requires_human, was_gated).
    """
    if action == "escalate":
        return action, confidence, reasoning, True, False

    # A run that reports success while producing nothing is an observation that
    # cannot, on its own, separate "no qualifying data" from "broken query". No
    # amount of model conviction changes that, so cap confidence structurally
    # rather than trusting the model to be humble.
    if signals.status == "succeeded" and signals.zero_rows and confidence == "high":
        confidence = "medium"
        reasoning = (
            f"{reasoning} [Confidence capped at medium: a zero-row success cannot "
            "distinguish an empty source window from a query or filter defect without "
            "evidence this system did not gather.]"
        )

    if confidence == "low":
        return (
            "escalate",
            "low",
            f"Escalated by policy gate: the model proposed '{action}' with low "
            f"confidence. Model reasoning: {reasoning}",
            True,
            True,
        )

    if signals.criticality in strategy.human_confirm_criticalities and confidence != "high":
        return (
            "escalate",
            confidence,
            f"Escalated by policy gate: {signals.pipeline_id} is {signals.criticality} "
            f"criticality with {signals.downstream_count} downstream consumers, and "
            f"confidence in '{action}' was only '{confidence}'. "
            f"Model reasoning: {reasoning}",
            True,
            True,
        )

    return action, confidence, reasoning, False, False


def _from_policy(event: dict, result: PolicyResult) -> TriageDecision:
    return TriageDecision(
        event_id=event["event_id"],
        action=result.action,
        reasoning=result.reasoning,
        confidence=result.confidence,
        decided_by="policy",
        evidence=[f"rule:{result.rule}"],
        requires_human=result.requires_human or result.action == "escalate",
        cost=CostReport(),
    )


def _escalation(event: dict, reasoning: str, decided_by: str, evidence: str) -> TriageDecision:
    return TriageDecision(
        event_id=event["event_id"],
        action="escalate",
        reasoning=reasoning,
        confidence="low",
        decided_by=decided_by,
        evidence=[evidence],
        requires_human=True,
    )


def triage_event(event: dict) -> TriageDecision:
    signals = build_signals(event)
    strategy = get_strategy(signals.pipeline_type)

    # Tier 1 -- deterministic, zero token cost.
    policy_result = strategy.decide(signals)
    if policy_result is not None:
        return _from_policy(event, policy_result)

    # Tier 2 -- LLM, only for what tier 1 could not settle.
    try:
        raw, usage = _call_llm(event)
    except BudgetExceeded as exc:
        return _escalation(
            event,
            f"{exc} Falling back to human review rather than acting without the "
            "reasoning step.",
            "budget_guard",
            "budget:exhausted",
        )
    except (httpx.HTTPError, KeyError, ValueError) as exc:
        return _escalation(
            event,
            f"Triage model call failed ({type(exc).__name__}: {exc}). Escalating rather "
            "than defaulting to an action.",
            "error",
            "llm:unavailable",
        )

    action, reasoning, confidence, evidence = _parse_full(raw)

    # Tier 3 -- escalation gate.
    action, confidence, reasoning, requires_human, gated = _apply_gate(
        action, confidence, reasoning, signals, strategy
    )

    return TriageDecision(
        event_id=event["event_id"],
        action=action,
        reasoning=reasoning,
        confidence=confidence,
        decided_by="escalation_gate" if gated else "llm",
        evidence=evidence or ["llm:judgement"],
        requires_human=requires_human,
        cost=CostReport(
            llm_calls=usage.llm_calls,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            estimated_cost_usd=round(usage.cost_usd, 6),
        ),
    )
