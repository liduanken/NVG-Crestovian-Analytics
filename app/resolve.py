"""The /resolve agent: a bounded tool loop over a live Airflow instance.

Control flow is an explicit while loop rather than a framework graph. For a
problem this size the loop *is* the architecture, and owning it directly is
what makes the step cap, the budget check, and the pre-action policy check
enforceable in code instead of requested in a prompt.

Order of operations per turn:
    budget check -> step cap -> LLM -> tool dispatch -> repeat
and on exit, the same escalation gate that /triage uses.
"""
import json

from app.airflow_client import AirflowClient, AirflowError
from app.config import settings
from app.knowledge import build_signals, entry_for_dag
from app.llm import BudgetExceeded, Usage, chat, extract_json
from app.models import ResolveDecision, ResolveRequest
from app.policy import get_strategy
from app.tools import Toolbox

SYSTEM_PROMPT = """\
You are the on-call pipeline triage agent for Crestovian Analytics, operating on a \
live Airflow deployment. You are investigating one failing task.

Work in this order:
1. get_pipeline_context -- learn ownership, criticality, SLA, and enforced policy.
2. get_run_state -- see what actually failed and how many attempts remain.
3. read_task_logs -- get the real error. This is mandatory before any action.
4. Only if the logs indicate a defect: read_dag_source and/or get_airflow_variable.

Then act, then call submit_decision. Your terminal action is always one of:
- retry: transient fault, or a defect you have just corrected. Use rerun_task.
- skip: the run does not need to succeed (deprecated pipeline, covered by the next \
scheduled run, or an owning team already handles it post-SLA).
- escalate: a human is needed, or the evidence does not distinguish between a benign \
and a harmful cause.

Rules you must follow:
- Prefer configuration fixes over source edits. If a value is driven by an Airflow \
Variable, correct the Variable; do not rewrite the DAG.
- Never retry into data corruption or a failed integrity check.
- A run that reports success but produced no output is a silent failure. Do not \
assume it is benign, and do not assume it is broken; say which evidence would settle it.
- Do not call tools you do not need. Every call costs budget.
- State low confidence when you have it. Escalating is a correct outcome, not a failure.

Finish by calling submit_decision exactly once.
"""


def _seed_context(dag_id: str, task_id: str) -> str:
    entry = entry_for_dag(dag_id) or {}
    return (
        f"Investigate task '{task_id}' in DAG '{dag_id}'.\n"
        f"Catalog pipeline_id: {entry.get('pipeline_id', 'unknown')} | "
        f"criticality: {entry.get('criticality', 'unknown')} | "
        f"type: {entry.get('type', 'unknown')}"
    )


def _fallback(dag_id: str, task_id: str, reason: str, box: Toolbox | None, usage: Usage) -> ResolveDecision:
    return ResolveDecision(
        dag_id=dag_id,
        task_id=task_id,
        action="escalate",
        reasoning=reason,
        confidence="low",
        llm_calls=usage.llm_calls,
        tokens_used=usage.total_tokens,
        tool_calls=box.calls if box else [],
        actions_taken=box.actions_taken if box else [],
        estimated_cost_usd=round(usage.cost_usd, 6),
        stopped_because="fallback",
    )


def resolve_task(request: ResolveRequest) -> ResolveDecision:
    usage = Usage()
    dag_id, task_id = request.dag_id, request.task_id

    try:
        client = AirflowClient(request.airflow_url, request.airflow_user, request.airflow_password)
    except AirflowError as exc:
        return _fallback(dag_id, task_id, f"Could not reach Airflow: {exc}", None, usage)

    box = Toolbox(client=client, dag_id=dag_id, task_id=task_id)
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": _seed_context(dag_id, task_id)},
    ]

    decision: dict | None = None
    stopped_because = "submitted_decision"

    try:
        for step in range(settings.max_resolve_steps):
            remaining = settings.max_resolve_steps - step
            if remaining <= 2 and decision is None:
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Only {remaining} steps remain. Stop investigating and call "
                            "submit_decision now with the confidence your current evidence "
                            "supports."
                        ),
                    }
                )

            try:
                message, call_usage = chat(
                    messages, tools=box.spec(), max_tokens=settings.max_resolve_tokens
                )
            except BudgetExceeded as exc:
                stopped_because = "budget_exceeded"
                return _fallback(
                    dag_id,
                    task_id,
                    f"{exc} Escalating rather than acting on partial investigation.",
                    box,
                    usage,
                )
            usage.add(call_usage)

            tool_calls = message.get("tool_calls") or []
            messages.append(
                {
                    "role": "assistant",
                    "content": message.get("content") or "",
                    **({"tool_calls": tool_calls} if tool_calls else {}),
                }
            )

            if not tool_calls:
                # Model answered in prose. Accept a well-formed decision, else nudge once.
                parsed = extract_json(message.get("content") or "")
                if parsed and parsed.get("action"):
                    decision = parsed
                    break
                messages.append(
                    {
                        "role": "user",
                        "content": "Use the provided tools. Finish with submit_decision.",
                    }
                )
                continue

            for call in tool_calls:
                fn = call.get("function", {})
                name = fn.get("name", "")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}

                result = box.call(name, args)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id", name),
                        "content": result,
                    }
                )
                if name == "submit_decision":
                    decision = args

            if decision is not None:
                break
        else:
            stopped_because = "step_limit"
    finally:
        client.close()

    if decision is None:
        return _fallback(
            dag_id,
            task_id,
            "Agent hit the investigation step limit without reaching a conclusion. "
            "Escalating rather than guessing.",
            box,
            usage,
        )

    action = str(decision.get("action", "")).lower().strip()
    if action not in ("retry", "skip", "escalate"):
        action = "escalate"
    confidence = str(decision.get("confidence", "")).lower().strip()
    if confidence not in ("high", "medium", "low"):
        confidence = "low"
    reasoning = str(decision.get("reasoning") or "No reasoning supplied.")

    # Same gate as /triage: uncertainty on a high-stakes pipeline is not a licence to act.
    entry = entry_for_dag(dag_id) or {}
    signals = build_signals(
        {
            "pipeline_id": entry.get("pipeline_id", ""),
            "pipeline_type": entry.get("type", "unknown"),
            "status": "failed",
        }
    )
    strategy = get_strategy(signals.pipeline_type)
    if action != "escalate" and confidence == "low":
        action = "escalate"
        reasoning = f"Escalated by policy gate on low confidence. Agent reasoning: {reasoning}"
        stopped_because = "escalation_gate"
    elif (
        action != "escalate"
        and signals.criticality in strategy.human_confirm_criticalities
        and confidence != "high"
    ):
        action = "escalate"
        reasoning = (
            f"Escalated by policy gate: {signals.criticality} criticality pipeline with "
            f"only '{confidence}' confidence. Agent reasoning: {reasoning}"
        )
        stopped_because = "escalation_gate"

    return ResolveDecision(
        dag_id=dag_id,
        task_id=task_id,
        action=action,
        reasoning=reasoning,
        confidence=confidence,
        llm_calls=usage.llm_calls,
        tokens_used=usage.total_tokens,
        tool_calls=box.calls,
        actions_taken=box.actions_taken,
        estimated_cost_usd=round(usage.cost_usd, 6),
        stopped_because=stopped_because,
    )
