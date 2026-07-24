from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel


class PipelineEvent(BaseModel):
    event_id: str
    pipeline_id: str
    pipeline_type: str
    timestamp: datetime
    status: Literal["failed", "succeeded", "degraded"]
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    rows_processed: Optional[int] = None
    rows_expected: Optional[int] = None
    duration_seconds: Optional[int] = None
    expected_duration_seconds: Optional[int] = None
    retry_count: int = 0
    metadata: Optional[dict[str, Any]] = None


class TriageDecision(BaseModel):
    event_id: str
    action: Literal["retry", "skip", "escalate"]
    reasoning: str


class ResolveRequest(BaseModel):
    dag_id: str
    task_id: str
    airflow_url: str = "http://localhost:8080"
    airflow_user: str = "admin"
    airflow_password: str = "admin"


class ResolveDecision(BaseModel):
    dag_id: str
    task_id: str
    action: Literal["retry", "skip", "escalate"]
    reasoning: str
    confidence: Literal["high", "medium", "low"] = "high"
    llm_calls: Optional[int] = None
    tokens_used: Optional[int] = None
    tool_calls: Optional[list[str]] = None
