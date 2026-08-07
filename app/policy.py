"""Tier 1: deterministic policy.

Rules here are cheap, auditable, and deterministic. They exist to answer the
failures that have a documented right answer, so the LLM is only spent on the
genuinely ambiguous tail.

A rule returns a PolicyResult to decide, or None to abstain. Abstention is a
first-class outcome: it is how a rule says "this needs judgement", which is
what routes an event to the LLM tier.

Extensibility: behaviour is looked up by pipeline_type through STRATEGIES.
Adding `stream_consumer` means registering a strategy, not editing the agent.
"""
from dataclasses import dataclass, field
from typing import Callable, Optional

from app.knowledge import Signals
from app.models import Action, Confidence

# Operational policy that the fixtures express as English prose in the catalog's
# `notes` field. Free-text notes cannot be enforced, so the binding constraints
# are lifted into structured predicates here. In production these belong as
# first-class columns in pipeline_catalog.json -- see SUBMISSION.md.
POLICY_OVERLAY: dict[str, dict] = {
    "pl_revenue_daily": {"never_auto_retry_on": ["corruption"]},
    "pl_market_feed": {"do_not_action": True},
    "pl_risk_scores": {"hard_deadline_utc": "07:00", "escalate_before_deadline_gate": True},
    "pl_report_export": {"no_retry_after_sla": True},
    "pl_web_analytics": {"covered_by_next_run": True},
}


@dataclass
class PolicyResult:
    action: Action
    reasoning: str
    rule: str
    confidence: Confidence = "high"
    requires_human: bool = False


Rule = Callable[[Signals], Optional[PolicyResult]]


def _overlay(signals: Signals) -> dict:
    return POLICY_OVERLAY.get(signals.pipeline_id, {})


# ---------------------------------------------------------------- shared rules


def rule_unknown_pipeline(s: Signals) -> Optional[PolicyResult]:
    """No catalog entry means no basis for an autonomous decision."""
    if s.known_pipeline:
        return None
    return PolicyResult(
        action="escalate",
        reasoning=(
            f"Pipeline {s.pipeline_id} is not in the catalog, so ownership, criticality "
            "and retry policy are unknown. Routing to a human rather than guessing."
        ),
        rule="unknown_pipeline",
        confidence="high",
        requires_human=True,
    )


def rule_decommissioned(s: Signals) -> Optional[PolicyResult]:
    """Deprecated pipelines past end-of-life should absorb no engineering time."""
    if not s.deprecated:
        return None
    if _overlay(s).get("do_not_action") or s.past_end_of_life:
        detail = "past its end-of-life date" if s.past_end_of_life else "in wind-down"
        return PolicyResult(
            action="skip",
            reasoning=(
                f"{s.pipeline_id} is deprecated and {detail}, with no downstream "
                "dependencies. Catalog policy is that failures are not actioned."
            ),
            rule="decommissioned_pipeline",
            confidence="high",
        )
    return None


def rule_never_auto_retry(s: Signals) -> Optional[PolicyResult]:
    """Some failure classes are explicitly barred from automatic retry."""
    barred = _overlay(s).get("never_auto_retry_on") or []
    hit = s.error_kinds.intersection(barred)
    if not hit:
        return None
    return PolicyResult(
        action="escalate",
        reasoning=(
            f"Failure class '{', '.join(sorted(hit))}' on {s.pipeline_id} is barred from "
            "auto-retry by catalog policy. Retrying on corrupted source data risks "
            "propagating bad numbers downstream."
        ),
        rule="never_auto_retry",
        confidence="high",
        requires_human=True,
    )


def rule_deadline_gate(s: Signals) -> Optional[PolicyResult]:
    """A hard external deadline beats the retry budget.

    Burning remaining retries against a persistent outage wastes the window a
    human would need to invoke a manual process.
    """
    if not _overlay(s).get("escalate_before_deadline_gate"):
        return None
    persistent = "upstream_outage" in s.error_kinds or s.consecutive_failures >= 2
    if not (persistent and (s.retries_exhausted or s.sla_passed)):
        return None
    return PolicyResult(
        action="escalate",
        reasoning=(
            f"{s.pipeline_id} has a hard {_overlay(s).get('hard_deadline_utc')} UTC "
            "regulatory gate and is failing against a persistent upstream outage. "
            "Catalog policy requires immediate escalation rather than exhausting retries."
        ),
        rule="deadline_gate",
        confidence="high",
        requires_human=True,
    )


def rule_code_bug_not_retryable(s: Signals) -> Optional[PolicyResult]:
    """Deterministic errors reproduce on every attempt; retrying only wastes time."""
    if "code_bug" not in s.error_kinds:
        return None
    return PolicyResult(
        action="escalate",
        reasoning=(
            f"Error '{s.error_code}' is deterministic (schema or code defect), so a retry "
            "reproduces it. Needs a fix, not a re-run. /resolve can investigate and "
            "remediate this class autonomously."
        ),
        rule="code_bug_not_retryable",
        confidence="high",
        requires_human=True,
    )


def rule_retries_exhausted(s: Signals) -> Optional[PolicyResult]:
    if not s.retries_exhausted or s.status == "succeeded":
        return None
    return PolicyResult(
        action="escalate",
        reasoning=(
            f"Retry budget is spent ({s.retry_count}/{s.max_retries}) and the failure "
            "persists. Further automatic retries are not authorised."
        ),
        rule="retries_exhausted",
        confidence="high",
        requires_human=True,
    )


def rule_known_transient(s: Signals) -> Optional[PolicyResult]:
    """The bread-and-butter case: a documented flaky source with budget left."""
    if "transient" not in s.error_kinds or s.retries_remaining <= 0:
        return None
    if not (s.error_seen_before or s.historically_self_resolves):
        return None
    return PolicyResult(
        action="retry",
        reasoning=(
            f"'{s.error_code}' is a known transient fault for {s.pipeline_id} that has "
            f"self-resolved on retry before. {s.retries_remaining} of {s.max_retries} "
            "retries remain, so a re-run is the cheapest correct move."
        ),
        rule="known_transient_within_budget",
        confidence="high",
    )


def rule_covered_by_next_run(s: Signals) -> Optional[PolicyResult]:
    """A frequent schedule with slack downstream absorbs an isolated failure."""
    if not _overlay(s).get("covered_by_next_run"):
        return None
    if s.consecutive_failures > 1 or s.retries_exhausted:
        return None
    if s.error_kinds & {"code_bug", "corruption"}:
        return None
    return PolicyResult(
        action="skip",
        reasoning=(
            f"Isolated failure on {s.pipeline_id}, which reruns on a short schedule and "
            "feeds a dashboard with staleness tolerance. The next scheduled run covers it."
        ),
        rule="covered_by_next_run",
        confidence="medium",
    )


def rule_no_retry_after_sla(s: Signals) -> Optional[PolicyResult]:
    """Past the SLA window a retry races a human who already owns the fallback."""
    if not (_overlay(s).get("no_retry_after_sla") and s.sla_passed):
        return None
    if s.status == "succeeded":
        # Succeeded-but-degraded past SLA is a data question, not a scheduling
        # one. Deliberately abstain and let the LLM tier weigh it.
        return None
    return PolicyResult(
        action="skip",
        reasoning=(
            f"{s.pipeline_id} missed its {s.sla_time} SLA window. Catalog policy hands "
            "post-SLA regeneration to the owning team, so an automatic retry would "
            "duplicate work already owned by a human."
        ),
        rule="no_retry_after_sla",
        confidence="medium",
    )


def rule_healthy_run(s: Signals) -> Optional[PolicyResult]:
    """Nothing to do for a clean success within the expected row range."""
    if s.status != "succeeded" or s.rows_below_expected or s.zero_rows:
        return None
    return PolicyResult(
        action="skip",
        reasoning="Run succeeded within the expected row range. No action required.",
        rule="healthy_run",
        confidence="high",
    )


# ------------------------------------------------------------------ strategies

_COMMON_PRELUDE: list[Rule] = [
    rule_unknown_pipeline,
    rule_decommissioned,
    rule_never_auto_retry,
    rule_deadline_gate,
]

_COMMON_TAIL: list[Rule] = [
    rule_code_bug_not_retryable,
    rule_retries_exhausted,
    rule_known_transient,
]


@dataclass
class PipelineStrategy:
    """Per-pipeline-type behaviour: which rules apply, and how to brief the LLM."""

    pipeline_type: str
    rules: list[Rule] = field(default_factory=list)
    guidance: str = ""
    # Criticalities where an LLM decision must be confirmed by a human before
    # any state-changing action.
    human_confirm_criticalities: tuple[str, ...] = ("critical",)

    def decide(self, signals: Signals) -> Optional[PolicyResult]:
        for rule in self.rules:
            result = rule(signals)
            if result is not None:
                return result
        return None


BATCH_ETL = PipelineStrategy(
    pipeline_type="batch_etl",
    rules=[*_COMMON_PRELUDE, rule_healthy_run, *_COMMON_TAIL, rule_covered_by_next_run],
    guidance=(
        "Batch ETL runs on a fixed schedule. Retry is appropriate for transient source "
        "faults with budget remaining. Never retry into corrupted source data. Weigh "
        "downstream dependency count and SLA proximity when judging urgency."
    ),
)

API_INGEST = PipelineStrategy(
    pipeline_type="api_ingest",
    rules=[*_COMMON_PRELUDE, rule_healthy_run, *_COMMON_TAIL],
    guidance=(
        "API ingest pipelines poll an external provider on a short schedule, so an "
        "isolated failure is usually self-healing. Provider deprecation or auth errors "
        "are structural and never resolve on retry."
    ),
)

REPORT_EXPORT = PipelineStrategy(
    pipeline_type="report_export",
    rules=[*_COMMON_PRELUDE, rule_no_retry_after_sla, rule_healthy_run, *_COMMON_TAIL],
    guidance=(
        "Report exports are user-visible artefacts. A run that succeeds but produces no "
        "output is a silent failure and is more dangerous than a loud one, because "
        "nothing alerts on it. Distinguish 'genuinely no qualifying data' from 'query or "
        "filter defect' before acting; if the evidence does not separate them, escalate."
    ),
)

# Forward-looking: the second pipeline type Crestovian onboards next quarter.
# Registered now to prove the seam is real -- the agent, endpoints, and
# escalation gate need no changes to pick this up.
STREAM_CONSUMER = PipelineStrategy(
    pipeline_type="stream_consumer",
    rules=[rule_unknown_pipeline, rule_decommissioned, rule_never_auto_retry, rule_code_bug_not_retryable],
    guidance=(
        "Stream consumers are long-running and measured by consumer lag, not run "
        "success. Restarting a consumer is closer to 'retry' but risks duplicate "
        "processing without idempotent sinks, and 'skip' means accepting permanent data "
        "loss for the lagged window rather than deferring to a next run. Prefer "
        "escalation until lag and offset-commit semantics are confirmed."
    ),
    human_confirm_criticalities=("critical", "high"),
)

DEFAULT_STRATEGY = PipelineStrategy(
    pipeline_type="unknown",
    rules=[rule_unknown_pipeline, rule_decommissioned, rule_never_auto_retry],
    guidance="Unrecognised pipeline type. Apply conservative defaults and prefer escalation.",
)

STRATEGIES: dict[str, PipelineStrategy] = {
    s.pipeline_type: s
    for s in (BATCH_ETL, API_INGEST, REPORT_EXPORT, STREAM_CONSUMER)
}


def register(strategy: PipelineStrategy) -> None:
    STRATEGIES[strategy.pipeline_type] = strategy


def get_strategy(pipeline_type: str) -> PipelineStrategy:
    return STRATEGIES.get(pipeline_type, DEFAULT_STRATEGY)
