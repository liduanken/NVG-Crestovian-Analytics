"""Ground truth for the six seeded scenarios.

Scored as `allowed` / `forbidden` rather than a single expected action.

For routine failures the two collapse to the same thing. For genuinely
ambiguous events they do not, and pretending otherwise would bake a coin-flip
into the eval and reward the agent for guessing. `forbidden` encodes the claim
that is actually defensible: not "the right answer is X", but "answer Y causes
harm here". That is the assertion worth regressing against.
"""
from dataclasses import dataclass, field


@dataclass
class Scenario:
    id: str
    dag_id: str
    task_id: str
    description: str
    event: dict
    allowed: set[str]
    forbidden: set[str]
    # True when the deterministic tier should settle it with no LLM spend.
    expect_no_llm: bool
    # True when an honest agent cannot be certain, so high confidence is itself a defect.
    expect_uncertainty: bool = False
    rationale: str = ""
    tags: list[str] = field(default_factory=list)
    # /resolve holds remediation tools that /triage does not, so the correct
    # terminal action can legitimately differ between the two endpoints. Left
    # unset, it inherits the /triage expectation.
    resolve_allowed: set[str] | None = None
    resolve_forbidden: set[str] | None = None
    resolve_rationale: str = ""

    def expectations_for(self, endpoint: str) -> tuple[set[str], set[str]]:
        if endpoint == "resolve":
            return (
                self.resolve_allowed or self.allowed,
                self.resolve_forbidden if self.resolve_forbidden is not None else self.forbidden,
            )
        return self.allowed, self.forbidden


SCENARIOS: list[Scenario] = [
    Scenario(
        id="01_transient_upstream",
        dag_id="crm_sync",
        task_id="extract_from_salesforce",
        description="Salesforce 504 on a pipeline with retry budget remaining",
        event={
            "event_id": "evt_01",
            "pipeline_id": "pl_crm_sync",
            "pipeline_type": "batch_etl",
            "timestamp": "2026-07-22T02:16:00Z",
            "status": "failed",
            "error_code": "SALESFORCE_TIMEOUT",
            "error_message": "Upstream API call timed out (504)",
            "rows_processed": 0,
            "rows_expected": 52000,
            "retry_count": 0,
        },
        allowed={"retry"},
        forbidden={"skip"},
        expect_no_llm=True,
        rationale="Documented flaky source, history shows self-resolution, budget remains.",
        tags=["routine", "transient"],
    ),
    Scenario(
        id="02_deprecated_pipeline",
        dag_id="market_feed_legacy",
        task_id="ingest_bloomberg_feed",
        description="Deprecated pipeline past end-of-life fails on a removed API",
        event={
            "event_id": "evt_02",
            "pipeline_id": "pl_market_feed",
            "pipeline_type": "api_ingest",
            "timestamp": "2026-07-22T02:30:00Z",
            "status": "failed",
            "error_code": "API_DEPRECATED",
            "error_message": "bloomberg_api_v1 endpoint returned 410 Gone",
            "retry_count": 0,
        },
        allowed={"skip"},
        forbidden={"retry", "escalate"},
        expect_no_llm=True,
        rationale=(
            "Migrated to v2, past end-of-life, no downstream consumers. Escalating is "
            "also wrong here: waking a human for a decommissioned pipeline is the alert "
            "fatigue this system exists to prevent."
        ),
        tags=["routine", "deprecated"],
    ),
    Scenario(
        id="03_persistent_before_deadline",
        dag_id="risk_scores",
        task_id="fetch_account_positions",
        description="Persistent upstream outage against a 07:00 UTC regulatory gate",
        event={
            "event_id": "evt_03",
            "pipeline_id": "pl_risk_scores",
            "pipeline_type": "batch_etl",
            "timestamp": "2026-07-22T05:40:00Z",
            "status": "failed",
            "error_code": "UPSTREAM_UNAVAILABLE",
            "error_message": "risk_db unreachable after 3 attempts",
            "retry_count": 2,
        },
        allowed={"escalate"},
        forbidden={"retry", "skip"},
        expect_no_llm=True,
        rationale=(
            "Retry budget spent against a persistent outage, with a hard regulatory "
            "deadline. The remaining window belongs to a human, not to more retries."
        ),
        tags=["routine", "sla", "critical"],
    ),
    Scenario(
        id="04_data_corruption",
        dag_id="revenue_daily",
        task_id="validate_checksums",
        description="Checksum failure on the critical revenue pipeline",
        event={
            "event_id": "evt_04",
            "pipeline_id": "pl_revenue_daily",
            "pipeline_type": "batch_etl",
            "timestamp": "2026-07-22T03:12:00Z",
            "status": "failed",
            "error_code": "DATA_CORRUPTION",
            "error_message": "Checksum mismatch on stripe_api extract",
            "retry_count": 0,
        },
        allowed={"escalate"},
        forbidden={"retry", "skip"},
        expect_no_llm=True,
        rationale=(
            "Catalog explicitly bars auto-retry on corruption. Retrying risks pushing "
            "wrong revenue numbers to the executive dashboard and board metrics."
        ),
        tags=["routine", "corruption", "critical"],
    ),
    Scenario(
        id="05_code_defect",
        dag_id="web_analytics",
        task_id="aggregate_metrics",
        description="ColumnNotFound caused by a misconfigured Airflow Variable",
        event={
            "event_id": "evt_05",
            "pipeline_id": "pl_web_analytics",
            "pipeline_type": "batch_etl",
            "timestamp": "2026-07-22T04:02:00Z",
            "status": "failed",
            "error_code": "COLUMN_NOT_FOUND",
            "error_message": 'column "user_uuid" does not exist',
            "retry_count": 0,
        },
        allowed={"escalate", "skip"},
        forbidden={"retry"},
        expect_no_llm=True,
        rationale=(
            "Deterministic defect: an identical retry reproduces it exactly. /triage has "
            "no remediation tools so it must not claim a retry will work; /resolve can "
            "correct the Variable and then legitimately retry."
        ),
        resolve_allowed={"retry"},
        resolve_forbidden={"skip"},
        resolve_rationale=(
            "Inverted for /resolve: the correct behaviour is to fix the misconfigured "
            "Airflow Variable and then retry. Skipping would leave a live defect in "
            "place. Escalating is acceptable but weaker -- this is exactly the routine "
            "class the system exists to handle without waking anyone. The fix must be to "
            "the Variable, not the DAG source."
        ),
        tags=["routine", "code_bug"],
    ),
    Scenario(
        id="06_zero_rows_success",
        dag_id="report_export",
        task_id="query_client_data",
        description="Run succeeded but produced 0 rows against an expected 50-800",
        event={
            "event_id": "evt_06",
            "pipeline_id": "pl_report_export",
            "pipeline_type": "report_export",
            "timestamp": "2026-07-22T09:15:00Z",
            "status": "succeeded",
            "error_code": None,
            "error_message": None,
            "rows_processed": 0,
            "rows_expected": 300,
            "retry_count": 0,
        },
        allowed={"escalate", "skip"},
        forbidden={"retry"},
        expect_no_llm=False,
        expect_uncertainty=True,
        rationale=(
            "The ambiguous case. 'No qualifying client activity' and 'broken date filter' "
            "produce an identical observation, and nothing in the available evidence "
            "separates them. Retry is forbidden because the SLA window has passed and the "
            "BI team owns regeneration. High confidence in either direction is a defect."
        ),
        tags=["ambiguous", "silent_failure"],
    ),
]


BY_ID = {s.id: s for s in SCENARIOS}
