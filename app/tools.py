"""Tools available to the /resolve agent.

Two design rules:

1. Read tools are cheap and unrestricted. Write tools change live state and are
   gated -- the agent must have actually read the logs before it can act, so a
   confident-but-uninvestigated retry on a critical pipeline is impossible.
2. Log payloads are condensed before they reach the model. Airflow task logs run
   to tens of thousands of characters and are mostly scheduler noise; sending
   them raw is the single largest cost risk in this system.
"""
import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from app.airflow_client import AirflowClient, AirflowError
from app.config import DAGS_DIR, settings
from app.knowledge import entry_for_dag
from app.policy import POLICY_OVERLAY

_NOISE = re.compile(
    r"(Dependencies all met|Starting attempt|Exporting env vars|Filling up the "
    r"DagBag|Running <TaskInstance|--------------------------------------------)"
)
_SIGNAL = re.compile(r"(error|exception|traceback|failed|fatal|warn|raise|\bat \b)", re.I)


def condense_logs(raw: str, limit: int) -> str:
    """Keep the lines that carry diagnostic signal, then the tail for context.

    Budget is spent on signal first. When the result must be truncated the tail
    is dropped before the error lines, and if the error lines alone overflow we
    keep the last of them, since a traceback ends with the actual exception.
    """
    lines = [ln.rstrip() for ln in (raw or "").splitlines() if ln.strip()]
    if not lines:
        return "(no log content returned)"

    signal = [ln for ln in lines if _SIGNAL.search(ln) and not _NOISE.search(ln)]

    seen = set(signal)
    tail = [ln for ln in lines[-25:] if ln not in seen]

    kept: list[str] = []
    used = 0
    for line in reversed(signal):
        if used + len(line) + 1 > limit:
            break
        kept.append(line)
        used += len(line) + 1
    kept.reverse()

    for line in tail:
        if used + len(line) + 1 > limit:
            break
        kept.append(line)
        used += len(line) + 1

    if not kept:
        return lines[-1][:limit]
    return "\n".join(kept)


@dataclass
class Toolbox:
    client: AirflowClient
    dag_id: str
    task_id: str
    run_id: Optional[str] = None
    calls: list[str] = field(default_factory=list)
    actions_taken: list[str] = field(default_factory=list)
    has_read_logs: bool = False

    # ------------------------------------------------------------------ specs

    def spec(self) -> list[dict]:
        return [
            _tool(
                "get_pipeline_context",
                "Catalog metadata, operational policy, and Airflow docs for this DAG. "
                "Free and local -- call this first.",
                {},
            ),
            _tool(
                "get_run_state",
                "Current and recent run states for this DAG, including per-task status "
                "and try numbers.",
                {},
            ),
            _tool(
                "read_task_logs",
                "Condensed error output from the failing task. Required before any "
                "state-changing action.",
                {
                    "try_number": {
                        "type": "integer",
                        "description": "Attempt to read; defaults to the latest.",
                    }
                },
            ),
            _tool(
                "read_dag_source",
                "Source of the DAG file. Use when logs point to a code or SQL defect.",
                {},
            ),
            _tool(
                "get_airflow_variable",
                "Read an Airflow Variable. Config-driven DAG behaviour usually lives here.",
                {"key": {"type": "string", "description": "Variable key."}},
                required=["key"],
            ),
            _tool(
                "set_airflow_variable",
                "Update an Airflow Variable. Prefer this over patching DAG source when "
                "the defect is a configuration value.",
                {
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                    "justification": {"type": "string"},
                },
                required=["key", "value", "justification"],
            ),
            _tool(
                "patch_dag_source",
                "Replace an exact snippet in the DAG file. Last resort -- only when the "
                "defect is genuinely in code and not in configuration.",
                {
                    "find": {"type": "string", "description": "Exact text to replace."},
                    "replace": {"type": "string"},
                    "justification": {"type": "string"},
                },
                required=["find", "replace", "justification"],
            ),
            _tool(
                "rerun_task",
                "Clear the task so Airflow re-runs it. This is the 'retry' action.",
                {"justification": {"type": "string"}},
                required=["justification"],
            ),
            _tool(
                "submit_decision",
                "Report the terminal triage decision and stop. Always finish with this.",
                {
                    "action": {"type": "string", "enum": ["retry", "skip", "escalate"]},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "reasoning": {"type": "string"},
                },
                required=["action", "confidence", "reasoning"],
            ),
        ]

    # ------------------------------------------------------------- dispatch

    def call(self, name: str, args: dict) -> str:
        self.calls.append(name)
        handler = getattr(self, f"_t_{name}", None)
        if handler is None:
            return f"ERROR: unknown tool '{name}'."
        try:
            return handler(args)
        except AirflowError as exc:
            return f"ERROR: {exc}"
        except Exception as exc:
            # Arguments come from the model, so a malformed call must surface as a
            # tool error the agent can recover from, not a 500.
            return f"ERROR: {type(exc).__name__}: {exc}"

    def _resolve_run_id(self) -> Optional[str]:
        if self.run_id:
            return self.run_id
        run = self.client.latest_run(self.dag_id)
        self.run_id = run.get("dag_run_id") if run else None
        return self.run_id

    def _guard_write(self) -> Optional[str]:
        if not self.has_read_logs:
            return (
                "BLOCKED: read_task_logs must be called before any state-changing action. "
                "Investigate before acting."
            )
        return None

    # ------------------------------------------------------------------ tools

    def _t_get_pipeline_context(self, _: dict) -> str:
        entry = entry_for_dag(self.dag_id) or {}
        payload: dict[str, Any] = {
            "catalog": entry,
            "enforced_policy": POLICY_OVERLAY.get(entry.get("pipeline_id", ""), {}),
        }
        try:
            payload["airflow_doc_md"] = (self.client.dag_details(self.dag_id) or {}).get("doc_md")
        except AirflowError as exc:
            payload["airflow_doc_md"] = f"(unavailable: {exc})"
        return json.dumps(payload, indent=2)[: settings.max_log_chars]

    def _t_get_run_state(self, _: dict) -> str:
        run_id = self._resolve_run_id()
        if not run_id:
            return "ERROR: no runs found for this DAG."
        instances = [
            {
                "task_id": ti.get("task_id"),
                "state": ti.get("state"),
                "try_number": ti.get("try_number"),
                "max_tries": ti.get("max_tries"),
                "duration": ti.get("duration"),
            }
            for ti in self.client.task_instances(self.dag_id, run_id)
        ]
        recent = [
            {"run_id": r.get("dag_run_id"), "state": r.get("state"), "date": r.get("execution_date")}
            for r in self.client.recent_runs(self.dag_id, limit=5)
        ]
        return json.dumps(
            {"current_run": run_id, "tasks": instances, "recent_runs": recent}, indent=2
        )

    def _t_read_task_logs(self, args: dict) -> str:
        run_id = self._resolve_run_id()
        if not run_id:
            return "ERROR: no runs found for this DAG."

        try_number = args.get("try_number")
        if not try_number:
            ti = self.client.task_instance(self.dag_id, run_id, self.task_id)
            try_number = max(1, int(ti.get("try_number") or 1))

        raw = self.client.task_logs(self.dag_id, run_id, self.task_id, int(try_number))
        self.has_read_logs = True
        return condense_logs(raw, settings.max_log_chars)

    def _t_read_dag_source(self, _: dict) -> str:
        path = DAGS_DIR / f"{self.dag_id}.py"
        if not path.exists():
            return f"ERROR: no DAG source found at {path.name}."
        return path.read_text()[: settings.max_log_chars * 2]

    def _t_get_airflow_variable(self, args: dict) -> str:
        key = args.get("key", "")
        value = self.client.get_variable(key)
        return json.dumps({"key": key, "value": value, "exists": value is not None})

    def _t_set_airflow_variable(self, args: dict) -> str:
        blocked = self._guard_write()
        if blocked:
            return blocked
        key, value = args.get("key", ""), args.get("value", "")
        self.client.set_variable(key, value)
        note = f"set_airflow_variable {key}={value} ({args.get('justification', '')})"
        self.actions_taken.append(note)
        return f"OK: {note}"

    def _t_patch_dag_source(self, args: dict) -> str:
        blocked = self._guard_write()
        if blocked:
            return blocked
        if not settings.allow_dag_patching:
            return "BLOCKED: DAG source patching is disabled by configuration."

        path = (DAGS_DIR / f"{self.dag_id}.py").resolve()
        if DAGS_DIR.resolve() not in path.parents:
            return "BLOCKED: refusing to write outside the dags directory."
        if not path.exists():
            return f"ERROR: no DAG source found at {path.name}."

        find, replace = args.get("find", ""), args.get("replace", "")
        source = path.read_text()
        occurrences = source.count(find)
        if not find or occurrences == 0:
            return "ERROR: 'find' text not present in the DAG source; no change made."
        if occurrences > 1:
            return f"ERROR: 'find' matched {occurrences} times; be more specific."

        path.write_text(source.replace(find, replace, 1))
        note = f"patch_dag_source {path.name}: {find!r} -> {replace!r} ({args.get('justification', '')})"
        self.actions_taken.append(note)
        return f"OK: {note}"

    def _t_rerun_task(self, args: dict) -> str:
        blocked = self._guard_write()
        if blocked:
            return blocked
        run_id = self._resolve_run_id()
        if not run_id:
            return "ERROR: no runs found for this DAG."
        self.client.clear_task(self.dag_id, run_id, self.task_id)
        note = f"rerun_task {self.dag_id}.{self.task_id} ({args.get('justification', '')})"
        self.actions_taken.append(note)
        return f"OK: {note}"

    def _t_submit_decision(self, args: dict) -> str:
        return json.dumps(args)


def _tool(name: str, description: str, properties: dict, required: list[str] | None = None) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required or [],
            },
        },
    }
