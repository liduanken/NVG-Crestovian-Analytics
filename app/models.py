from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

Action = Literal["retry", "skip", "escalate"]
Confidence = Literal["high", "medium", "low"]
DecidedBy = Literal["policy", "llm", "escalation_gate", "budget_guard", "error"]


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


class CostReport(BaseModel):
    llm_calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    estimated_cost_usd: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class TriageDecision(BaseModel):
    event_id: str
    action: Action
    reasoning: str
    confidence: Confidence = "high"
    # Which tier produced the decision, so the cheap/expensive path split is
    # measurable rather than a claim in a README.
    decided_by: DecidedBy = "policy"
    # Named rules or signals behind the decision, for audit and eval scoring.
    evidence: list[str] = Field(default_factory=list)
    requires_human: bool = False
    cost: CostReport = Field(default_factory=CostReport)


class ResolveRequest(BaseModel):
    dag_id: str
    task_id: str
    airflow_url: str = "http://localhost:8080"
    airflow_user: str = "admin"
    airflow_password: str = "admin"


class ResolveDecision(BaseModel):
    dag_id: str
    task_id: str
    action: Action
    reasoning: str
    confidence: Confidence = "high"
    llm_calls: Optional[int] = None
    tokens_used: Optional[int] = None
    tool_calls: Optional[list[str]] = None
    # What the agent changed in live state. Kept separate from the terminal
    # action so "escalate" can still report remediation it staged.
    actions_taken: list[str] = Field(default_factory=list)
    estimated_cost_usd: float = 0.0
    stopped_because: Optional[str] = None
