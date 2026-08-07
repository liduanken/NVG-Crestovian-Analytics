"""Thin Airflow REST client.

Scoped deliberately to the operations the triage agent is authorised to
perform. Anything not exposed here (deleting runs, pausing DAGs, running
arbitrary SQL) is out of reach of the agent by construction rather than by
prompt instruction.
"""
from typing import Any, Optional
from urllib.parse import quote

import httpx

from app.config import settings


class AirflowError(RuntimeError):
    pass


class AirflowClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 20.0,
    ):
        self.base = f"{(base_url or settings.airflow_url).rstrip('/')}/api/v1"
        self._client = httpx.Client(
            auth=(user or settings.airflow_user, password or settings.airflow_password),
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "AirflowClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> Any:
        try:
            response = self._client.request(method, f"{self.base}{path}", **kwargs)
        except httpx.HTTPError as exc:
            raise AirflowError(f"Airflow unreachable: {exc}") from exc
        if response.status_code >= 400:
            raise AirflowError(
                f"Airflow {method} {path} returned {response.status_code}: {response.text[:300]}"
            )
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        return response.text

    # ------------------------------------------------------------- read paths

    def dag_details(self, dag_id: str) -> dict:
        return self._request("GET", f"/dags/{dag_id}/details")

    def recent_runs(self, dag_id: str, limit: int = 7) -> list[dict]:
        body = self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns",
            params={"order_by": "-execution_date", "limit": limit},
        )
        return body.get("dag_runs", [])

    def latest_run(self, dag_id: str) -> Optional[dict]:
        runs = self.recent_runs(dag_id, limit=1)
        return runs[0] if runs else None

    def task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        body = self._request(
            "GET", f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances"
        )
        return body.get("task_instances", [])

    def task_instance(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}",
        )

    def task_logs(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        raw = self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            params={"full_content": "true"},
            headers={"Accept": "text/plain"},
        )
        return raw if isinstance(raw, str) else str(raw)

    def get_variable(self, key: str) -> Optional[str]:
        try:
            return self._request("GET", f"/variables/{key}").get("value")
        except AirflowError:
            return None

    # ------------------------------------------------------------ write paths

    def set_variable(self, key: str, value: str) -> dict:
        return self._request("PATCH", f"/variables/{key}", json={"key": key, "value": value})

    def clear_task(self, dag_id: str, run_id: str, task_id: str) -> dict:
        """Clear a task instance, which is how Airflow re-runs it."""
        return self._request(
            "POST",
            f"/dags/{dag_id}/clearTaskInstances",
            json={
                "dry_run": False,
                "dag_run_id": run_id,
                "task_ids": [task_id],
                "include_downstream": True,
                "include_upstream": False,
                "only_failed": False,
                "reset_dag_runs": True,
            },
        )

    def set_task_state(self, dag_id: str, run_id: str, task_id: str, state: str) -> dict:
        return self._request(
            "PATCH",
            f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}",
            json={"new_state": state, "dry_run": False},
        )
