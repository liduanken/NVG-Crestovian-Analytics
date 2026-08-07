from unittest.mock import MagicMock, patch

from app.llm import Usage
from app.models import ResolveRequest
from app.resolve import resolve_task
from app.tools import Toolbox, condense_logs


def _toolbox(**kwargs) -> Toolbox:
    return Toolbox(client=MagicMock(), dag_id="web_analytics", task_id="aggregate_metrics", **kwargs)


# -- Log condensation keeps cost bounded


def test_condense_logs_keeps_error_lines_and_drops_noise():
    raw = "\n".join(
        ["INFO - Dependencies all met for task"] * 200
        + ['ERROR - column "user_uuid" does not exist']
        + ["INFO - Exporting env vars"] * 50
    )
    out = condense_logs(raw, 4000)
    assert "user_uuid" in out
    assert len(out) < len(raw)


def test_condense_logs_respects_limit():
    assert len(condense_logs("ERROR x\n" * 5000, 500)) <= 500


def test_condense_logs_handles_empty():
    assert condense_logs("", 100) == "(no log content returned)"


# -- Write guard: no state change before investigation


def test_write_tools_blocked_before_reading_logs():
    box = _toolbox()
    for tool, args in (
        ("rerun_task", {"justification": "x"}),
        ("set_airflow_variable", {"key": "k", "value": "v", "justification": "x"}),
        ("patch_dag_source", {"find": "a", "replace": "b", "justification": "x"}),
    ):
        assert box.call(tool, args).startswith("BLOCKED")
    assert box.actions_taken == []


def test_rerun_allowed_after_logs_read():
    box = _toolbox(has_read_logs=True, run_id="manual__1")
    assert box.call("rerun_task", {"justification": "transient"}).startswith("OK")
    assert len(box.actions_taken) == 1


def test_patch_rejects_text_that_is_not_present():
    box = _toolbox(has_read_logs=True)
    result = box.call(
        "patch_dag_source",
        {"find": "this string is not in the dag", "replace": "x", "justification": "y"},
    )
    assert result.startswith("ERROR")
    assert box.actions_taken == []


def test_patch_rejects_ambiguous_match():
    box = _toolbox(has_read_logs=True)
    result = box.call("patch_dag_source", {"find": "hook", "replace": "x", "justification": "y"})
    assert "matched" in result and result.startswith("ERROR")


# -- Agent loop bounds


def _tool_call(name: str, args: str = "{}"):
    return {"id": name, "function": {"name": name, "arguments": args}}


def test_step_limit_forces_escalation_rather_than_a_guess():
    endless = (
        {"tool_calls": [_tool_call("read_dag_source")]},
        Usage(llm_calls=1, prompt_tokens=10),
    )
    with patch("app.resolve.chat", return_value=endless), patch("app.resolve.AirflowClient"):
        decision = resolve_task(ResolveRequest(dag_id="crm_sync", task_id="extract_from_salesforce"))

    assert decision.action == "escalate"
    assert decision.confidence == "low"
    assert decision.stopped_because in ("fallback", "step_limit")
    assert decision.llm_calls > 0


def test_submit_decision_is_reported_with_cost_accounting():
    submit = (
        {
            "tool_calls": [
                _tool_call(
                    "submit_decision",
                    '{"action":"retry","confidence":"high","reasoning":"transient 504"}',
                )
            ]
        },
        Usage(llm_calls=1, prompt_tokens=500, completion_tokens=50, cost_usd=0.0001),
    )
    with patch("app.resolve.chat", return_value=submit), patch("app.resolve.AirflowClient"):
        decision = resolve_task(ResolveRequest(dag_id="crm_sync", task_id="extract_from_salesforce"))

    assert decision.action == "retry"
    assert decision.llm_calls == 1
    assert decision.tokens_used == 550
    assert "submit_decision" in (decision.tool_calls or [])


def test_low_confidence_from_agent_is_gated_to_escalate():
    submit = (
        {
            "tool_calls": [
                _tool_call(
                    "submit_decision",
                    '{"action":"skip","confidence":"low","reasoning":"unclear"}',
                )
            ]
        },
        Usage(llm_calls=1),
    )
    with patch("app.resolve.chat", return_value=submit), patch("app.resolve.AirflowClient"):
        decision = resolve_task(ResolveRequest(dag_id="report_export", task_id="query_client_data"))

    assert decision.action == "escalate"
    assert decision.stopped_because == "escalation_gate"


def test_invalid_action_from_agent_defaults_to_escalate():
    submit = (
        {
            "tool_calls": [
                _tool_call(
                    "submit_decision",
                    '{"action":"delete_everything","confidence":"high","reasoning":"no"}',
                )
            ]
        },
        Usage(llm_calls=1),
    )
    with patch("app.resolve.chat", return_value=submit), patch("app.resolve.AirflowClient"):
        decision = resolve_task(ResolveRequest(dag_id="crm_sync", task_id="extract_from_salesforce"))

    assert decision.action == "escalate"
