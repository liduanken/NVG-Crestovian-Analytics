import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

from app.models import TriageDecision

load_dotenv()

LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

_DATA_DIR = Path(__file__).parent.parent / "mock_data"


def _load_json(name: str) -> object:
    path = _DATA_DIR / name
    return json.loads(path.read_text()) if path.exists() else None


_CATALOG: dict[str, dict] = {
    p["pipeline_id"]: p
    for p in (_load_json("pipeline_catalog.json") or [])
}
_RUN_HISTORY: dict[str, list] = _load_json("run_history.json") or {}


def _build_prompt(event: dict) -> str:
    pipeline_id = event.get("pipeline_id", "")
    catalog_entry = _CATALOG.get(pipeline_id)
    run_history = _RUN_HISTORY.get(pipeline_id, [])

    parts = [
        "You are a data pipeline triage assistant for Crestovian Analytics.",
        "",
        "## Pipeline Event",
        json.dumps(event, indent=2, default=str),
    ]

    if catalog_entry:
        parts += [
            "",
            "## Pipeline Catalog Entry",
            json.dumps(catalog_entry, indent=2),
        ]

    if run_history:
        recent = run_history[:7]
        parts += [
            "",
            f"## Recent Run History (last {len(recent)} runs)",
            json.dumps(recent, indent=2),
        ]

    parts += [
        "",
        "## Decision",
        "Based on the event, catalog entry, and run history, choose one action:",
        "- retry: the failure is transient; a re-run is likely to succeed",
        "- skip: the failure is expected, covered by the next scheduled run, or the pipeline is deprecated / SLA already missed",
        "- escalate: the failure requires immediate human intervention",
        "",
        'Respond with JSON only: {"action": "retry"|"skip"|"escalate", "reasoning": "<one or two sentences>"}',
    ]

    return "\n".join(parts)


def _parse_response(raw: str) -> tuple[str, str]:
    """Extract (action, reasoning) from LLM response, handling markdown fences."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()

    try:
        parsed = json.loads(text)
        action = str(parsed.get("action", "")).lower()
        reasoning = str(parsed.get("reasoning", raw))
    except (json.JSONDecodeError, AttributeError):
        # Fall back to keyword scan — prefer escalate when ambiguous
        lower = text.lower()
        if "escalate" in lower:
            action = "escalate"
        elif "skip" in lower:
            action = "skip"
        elif "retry" in lower:
            action = "retry"
        else:
            action = "escalate"
        reasoning = raw

    if action not in ("retry", "skip", "escalate"):
        action = "escalate"

    return action, reasoning


def triage_event(event: dict) -> TriageDecision:
    prompt = _build_prompt(event)

    response = httpx.post(
        f"{LLM_BASE_URL.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {LLM_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": LLM_MODEL,
            "max_tokens": 256,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
        },
        timeout=30.0,
    )
    response.raise_for_status()
    raw = response.json()["choices"][0]["message"]["content"]
    action, reasoning = _parse_response(raw)

    return TriageDecision(
        event_id=event["event_id"],
        action=action,
        reasoning=reasoning,
    )
