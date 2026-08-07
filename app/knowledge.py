"""Access to the pipeline catalog and run history, plus the derived signals
the policy layer reasons over.

Loaded lazily with a TTL so a catalog edit does not require a restart, and so
tests can force a reload.
"""
import json
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from typing import Any, Optional

from app.config import MOCK_DATA_DIR, settings

_CACHE_TTL_SECONDS = 60.0
_cache: dict[str, Any] = {"loaded_at": 0.0, "catalog": {}, "history": {}}


def _read(name: str, fallback):
    path = MOCK_DATA_DIR / name
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return fallback


def _refresh(force: bool = False) -> None:
    if not force and (time.monotonic() - _cache["loaded_at"]) < _CACHE_TTL_SECONDS:
        return
    catalog_rows = _read("pipeline_catalog.json", [])
    _cache["catalog"] = {row["pipeline_id"]: row for row in catalog_rows}
    _cache["history"] = _read("run_history.json", {})
    _cache["loaded_at"] = time.monotonic()


def reload() -> None:
    _refresh(force=True)


def catalog_entry(pipeline_id: str) -> Optional[dict]:
    _refresh()
    return _cache["catalog"].get(pipeline_id)


def run_history(pipeline_id: str, limit: int | None = None) -> list[dict]:
    _refresh()
    rows = _cache["history"].get(pipeline_id, [])
    return rows[: limit if limit is not None else settings.history_window]


def all_pipelines() -> list[dict]:
    _refresh()
    return list(_cache["catalog"].values())


def entry_for_dag(dag_id: str) -> Optional[dict]:
    """Resolve a catalog entry from an Airflow dag_id.

    The `pl_<dag_id>` convention holds for every pipeline except
    pl_market_feed, so match on the explicit dag_id field instead.
    """
    _refresh()
    for row in _cache["catalog"].values():
        if row.get("dag_id") == dag_id:
            return row
    return _cache["catalog"].get(f"pl_{dag_id}")


def _parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass
class Signals:
    """Facts derived from event + catalog + history, computed without an LLM.

    Every field here is something a rule can branch on and an eval can assert.
    """
    pipeline_id: str
    pipeline_type: str
    known_pipeline: bool
    criticality: str
    deprecated: bool
    past_end_of_life: bool
    max_retries: int
    retry_count: int
    retries_remaining: int
    retries_exhausted: bool
    sla_time: Optional[str]
    sla_passed: Optional[bool]
    status: str
    error_code: Optional[str]
    error_message: Optional[str]
    error_kinds: set[str]
    rows_processed: Optional[int]
    expected_row_min: Optional[int]
    expected_row_max: Optional[int]
    zero_rows: bool
    rows_below_expected: bool
    consecutive_failures: int
    error_seen_before: bool
    historically_self_resolves: bool
    downstream_count: int
    notes: str

    def as_dict(self) -> dict:
        data = self.__dict__.copy()
        data["error_kinds"] = sorted(self.error_kinds)
        return data


_TRANSIENT_HINTS = ("timeout", "throttl", "rate_limit", "503", "504", "connection_reset")
_CORRUPTION_HINTS = ("corrupt", "checksum", "integrity", "malformed")
_DEPRECATION_HINTS = ("deprecat", "sunset", "gone", "410")
_CODE_BUG_HINTS = (
    "columnnotfound",
    "undefinedcolumn",
    "column not found",
    "does not exist",
    "syntaxerror",
    "syntax error",
    "nameerror",
    "typeerror",
    "keyerror",
    "modulenotfound",
    "attributeerror",
)


def classify_error(error_code: Optional[str], error_message: Optional[str] = None) -> set[str]:
    blob = f"{error_code or ''} {error_message or ''}".lower()
    kinds = set()
    if any(h in blob for h in _TRANSIENT_HINTS):
        kinds.add("transient")
    if any(h in blob for h in _CORRUPTION_HINTS):
        kinds.add("corruption")
    if any(h in blob for h in _DEPRECATION_HINTS):
        kinds.add("deprecation")
    if any(h in blob for h in _CODE_BUG_HINTS):
        kinds.add("code_bug")
    if "unavailable" in blob or "unreachable" in blob:
        kinds.add("upstream_outage")
    return kinds


def build_signals(event: dict, now: Optional[datetime] = None) -> Signals:
    pipeline_id = event.get("pipeline_id", "")
    entry = catalog_entry(pipeline_id) or {}
    history = run_history(pipeline_id)
    now = now or _parse_dt(event.get("timestamp")) or datetime.now(timezone.utc)

    max_retries = int(entry.get("max_retries", 0))
    retry_count = int(event.get("retry_count", 0) or 0)

    eol = _parse_dt(entry.get("end_of_life_date"))
    row_range = entry.get("expected_row_range") or {}
    rows_processed = event.get("rows_processed")

    sla_time = entry.get("sla_completion_by")
    sla_passed: Optional[bool] = None
    if sla_time:
        try:
            hour, minute = (int(p) for p in str(sla_time).split()[0].split(":")[:2])
            deadline = now.astimezone(timezone.utc).replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )
            sla_passed = now.astimezone(timezone.utc) > deadline
        except (ValueError, IndexError):
            sla_passed = None

    consecutive = 0
    for run in history:
        if run.get("status") in ("failed", "degraded"):
            consecutive += 1
        else:
            break

    error_code = event.get("error_code")
    error_message = event.get("error_message")
    error_seen_before = bool(
        error_code
        and any(
            error_code.lower() in json.dumps(run).lower()
            for run in history
        )
    )
    self_resolves = any(
        run.get("status") == "succeeded" and int(run.get("retry_count", 0) or 0) > 0
        for run in history
    )

    return Signals(
        pipeline_id=pipeline_id,
        pipeline_type=event.get("pipeline_type") or entry.get("type") or "unknown",
        known_pipeline=bool(entry),
        criticality=entry.get("criticality", "unknown"),
        deprecated=bool(entry.get("deprecated")),
        past_end_of_life=bool(eol and now.astimezone(timezone.utc) >= eol),
        max_retries=max_retries,
        retry_count=retry_count,
        retries_remaining=max(0, max_retries - retry_count),
        retries_exhausted=retry_count >= max_retries if entry else False,
        sla_time=sla_time,
        sla_passed=sla_passed,
        status=event.get("status", "failed"),
        error_code=error_code,
        error_message=error_message,
        error_kinds=classify_error(error_code, error_message),
        rows_processed=rows_processed,
        expected_row_min=row_range.get("min"),
        expected_row_max=row_range.get("max"),
        zero_rows=rows_processed == 0,
        rows_below_expected=(
            rows_processed is not None
            and row_range.get("min") is not None
            and rows_processed < row_range["min"]
        ),
        consecutive_failures=consecutive,
        error_seen_before=error_seen_before,
        historically_self_resolves=self_resolves,
        downstream_count=len(entry.get("downstream_dependencies") or []),
        notes=entry.get("notes", ""),
    )
