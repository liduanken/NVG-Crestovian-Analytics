from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import TriageDecision
from app.triage import _build_prompt, _parse_response

client = TestClient(app)

SAMPLE_EVENT = {
    "event_id": "evt_001",
    "pipeline_id": "pl_crm_sync",
    "pipeline_type": "batch_etl",
    "timestamp": "2026-06-22T02:02:14Z",
    "status": "failed",
    "error_code": "SALESFORCE_TIMEOUT",
    "error_message": "Upstream API call timed out",
    "rows_processed": 0,
    "rows_expected": 52000,
    "duration_seconds": 32,
    "expected_duration_seconds": 120,
    "retry_count": 0,
}

UNKNOWN_PIPELINE_EVENT = {**SAMPLE_EVENT, "pipeline_id": "pl_unknown_pipeline"}

# SAMPLE_EVENT is a documented transient fault, which the deterministic policy
# tier now settles without an LLM call. These tests exercise the tier-2 path, so
# they need an event that policy genuinely abstains on: an unrecognised error
# class with retry budget still remaining.
AMBIGUOUS_EVENT = {
    **SAMPLE_EVENT,
    "event_id": "evt_002",
    "error_code": "PARTIAL_LOAD",
    "error_message": "Load completed with fewer records than expected",
    "rows_processed": 31200,
    "retry_count": 1,
}


# -- Health

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# -- Endpoint response shape

@pytest.mark.parametrize("action", ["retry", "skip", "escalate"])
def test_triage_returns_structured_decision(action):
    mock_decision = TriageDecision(event_id="evt_001", action=action, reasoning="test")
    # main.py imports triage_event by name, so patch the reference there
    with patch("app.main.triage_event", return_value=mock_decision):
        resp = client.post("/triage", json=SAMPLE_EVENT)
    assert resp.status_code == 200
    data = resp.json()
    assert data["action"] == action
    assert "reasoning" in data
    assert data["event_id"] == "evt_001"


def test_triage_invalid_payload_returns_422():
    resp = client.post("/triage", json={"event_id": "bad"})
    assert resp.status_code == 422


def test_triage_invalid_action_in_event_rejected():
    bad = {**SAMPLE_EVENT, "status": "not_a_valid_status"}
    resp = client.post("/triage", json=bad)
    assert resp.status_code == 422


# -- Prompt construction

def test_prompt_includes_event_data():
    prompt = _build_prompt(SAMPLE_EVENT)
    assert "pl_crm_sync" in prompt
    assert "SALESFORCE_TIMEOUT" in prompt


def test_prompt_includes_catalog_for_known_pipeline():
    prompt = _build_prompt(SAMPLE_EVENT)
    # Catalog fields that must appear in context
    assert "max_retries" in prompt
    assert "criticality" in prompt
    assert "sla" in prompt.lower()


def test_prompt_includes_run_history_for_known_pipeline():
    prompt = _build_prompt(SAMPLE_EVENT)
    assert "Run History" in prompt


def test_prompt_gracefully_handles_unknown_pipeline():
    prompt = _build_prompt(UNKNOWN_PIPELINE_EVENT)
    # Should still build a prompt — just without catalog / history sections
    assert "pl_unknown_pipeline" in prompt
    assert "Pipeline Event" in prompt


# -- Response parsing

@pytest.mark.parametrize("action", ["retry", "skip", "escalate"])
def test_parse_clean_json(action):
    raw = f'{{"action": "{action}", "reasoning": "some reason"}}'
    parsed_action, reasoning = _parse_response(raw)
    assert parsed_action == action
    assert reasoning == "some reason"


def test_parse_json_with_markdown_fence():
    raw = '```json\n{"action": "retry", "reasoning": "transient"}\n```'
    action, reasoning = _parse_response(raw)
    assert action == "retry"


def test_parse_freetext_fallback_escalate():
    action, _ = _parse_response("I think we should escalate this immediately")
    assert action == "escalate"


def test_parse_freetext_fallback_skip():
    action, _ = _parse_response("My recommendation is to skip this run")
    assert action == "skip"


def test_parse_unknown_action_defaults_to_escalate():
    raw = '{"action": "investigate", "reasoning": "unsure"}'
    action, _ = _parse_response(raw)
    assert action == "escalate"


# -- Context injection end-to-end (mocking httpx)

def _make_httpx_mock(action: str = "retry", reasoning: str = "transient timeout", confidence: str = "high"):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": (
                        f'{{"action": "{action}", "confidence": "{confidence}", '
                        f'"reasoning": "{reasoning}"}}'
                    )
                }
            }
        ],
        "usage": {"prompt_tokens": 900, "completion_tokens": 60},
    }
    return mock_resp


def test_known_transient_decided_without_llm():
    """The deterministic tier must settle documented failures at zero token cost."""
    with patch("app.triage.httpx.post") as mock_post:
        resp = client.post("/triage", json=SAMPLE_EVENT)

    mock_post.assert_not_called()
    body = resp.json()
    assert body["action"] == "retry"
    assert body["decided_by"] == "policy"
    assert body["cost"]["llm_calls"] == 0


def test_llm_receives_catalog_context():
    with patch("app.triage.httpx.post", return_value=_make_httpx_mock()) as mock_post:
        resp = client.post("/triage", json=AMBIGUOUS_EVENT)

    assert resp.status_code == 200
    prompt = mock_post.call_args[1]["json"]["messages"][0]["content"]
    assert "max_retries" in prompt
    assert "salesforce_api" in prompt.lower()


def test_llm_receives_run_history():
    with patch("app.triage.httpx.post", return_value=_make_httpx_mock()) as mock_post:
        client.post("/triage", json=AMBIGUOUS_EVENT)

    prompt = mock_post.call_args[1]["json"]["messages"][0]["content"]
    assert "Run History" in prompt


def test_endpoint_returns_action_from_llm():
    with patch("app.triage.httpx.post", return_value=_make_httpx_mock("escalate", "SLA at risk")):
        resp = client.post("/triage", json=AMBIGUOUS_EVENT)

    assert resp.json()["action"] == "escalate"
    assert "SLA" in resp.json()["reasoning"]


def test_truncated_response_escalates_instead_of_guessing():
    """A reply cut off mid-JSON must not be keyword-matched into a decision."""
    truncated = MagicMock()
    truncated.raise_for_status = MagicMock()
    truncated.json.return_value = {
        "choices": [
            {
                "finish_reason": "length",
                "message": {"content": '{"action": "retry", "confid'},
            }
        ],
        "usage": {"prompt_tokens": 1400, "completion_tokens": 0, "total_tokens": 2900},
    }

    with patch("app.triage.httpx.post", return_value=truncated):
        resp = client.post("/triage", json=AMBIGUOUS_EVENT)

    body = resp.json()
    assert body["action"] == "escalate"
    assert body["decided_by"] == "error"
    assert body["requires_human"] is True
