from app.knowledge import build_signals, classify_error
from app.policy import (
    DEFAULT_STRATEGY,
    PipelineStrategy,
    PolicyResult,
    STRATEGIES,
    get_strategy,
    register,
)
from app.triage import _apply_gate
from evals.scenarios import BY_ID, SCENARIOS


def _decide(event: dict):
    signals = build_signals(event)
    return get_strategy(signals.pipeline_type).decide(signals), signals


# -- Deterministic tier resolves the routine fleet


def test_routine_scenarios_settled_without_llm():
    for scenario in SCENARIOS:
        if not scenario.expect_no_llm:
            continue
        result, _ = _decide(scenario.event)
        assert result is not None, f"{scenario.id} should be settled by policy"
        assert result.action in scenario.allowed, f"{scenario.id} -> {result.action}"
        assert result.action not in scenario.forbidden


def test_ambiguous_scenario_abstains_to_llm():
    result, _ = _decide(BY_ID["06_zero_rows_success"].event)
    assert result is None, "the ambiguous case must not be settled by a rule"


def test_corruption_never_retried_even_with_budget():
    event = {**BY_ID["04_data_corruption"].event, "retry_count": 0}
    result, _ = _decide(event)
    assert result.action == "escalate"
    assert result.rule == "never_auto_retry"


def test_unknown_pipeline_escalates():
    result, _ = _decide(
        {
            "event_id": "e",
            "pipeline_id": "pl_does_not_exist",
            "pipeline_type": "batch_etl",
            "status": "failed",
            "error_code": "SOMETHING",
            "retry_count": 0,
        }
    )
    assert result.action == "escalate"
    assert result.rule == "unknown_pipeline"


# -- Error classification


def test_classify_error_kinds():
    assert "transient" in classify_error("SALESFORCE_TIMEOUT")
    assert "corruption" in classify_error("DATA_CORRUPTION")
    assert "deprecation" in classify_error("API_DEPRECATED")
    assert "code_bug" in classify_error("COLUMN_NOT_FOUND", 'column "user_uuid" does not exist')
    assert "upstream_outage" in classify_error("UPSTREAM_UNAVAILABLE")


# -- Escalation gate


def _signals_for(pipeline_id: str, pipeline_type: str):
    return build_signals(
        {
            "event_id": "e",
            "pipeline_id": pipeline_id,
            "pipeline_type": pipeline_type,
            "status": "failed",
            "retry_count": 0,
        }
    )


def test_gate_converts_low_confidence_into_escalation():
    signals = _signals_for("pl_web_analytics", "batch_etl")
    action, _, _, requires_human, gated = _apply_gate(
        "retry", "low", "not sure", signals, get_strategy("batch_etl")
    )
    assert action == "escalate"
    assert requires_human and gated


def test_gate_blocks_medium_confidence_on_critical_pipeline():
    signals = _signals_for("pl_revenue_daily", "batch_etl")
    action, _, _, _, gated = _apply_gate(
        "retry", "medium", "probably fine", signals, get_strategy("batch_etl")
    )
    assert action == "escalate" and gated


def test_gate_allows_high_confidence_on_non_critical_pipeline():
    signals = _signals_for("pl_web_analytics", "batch_etl")
    action, _, _, requires_human, gated = _apply_gate(
        "skip", "high", "covered by next run", signals, get_strategy("batch_etl")
    )
    assert action == "skip"
    assert not requires_human and not gated


# -- Extensibility: the constraint that a new pipeline type needs no agent changes


def test_stream_consumer_type_is_registered():
    assert "stream_consumer" in STRATEGIES
    strategy = get_strategy("stream_consumer")
    assert strategy.guidance
    # Stricter than batch: 'high' criticality also demands human confirmation.
    assert "high" in strategy.human_confirm_criticalities


def test_new_pipeline_type_plugs_in_without_touching_the_agent():
    def always_skip(_signals):
        return PolicyResult(action="skip", reasoning="test rule", rule="test_rule")

    register(PipelineStrategy(pipeline_type="cdc_replicator", rules=[always_skip]))
    try:
        result, _ = _decide(
            {
                "event_id": "e",
                "pipeline_id": "pl_crm_sync",
                "pipeline_type": "cdc_replicator",
                "status": "failed",
                "error_code": "SALESFORCE_TIMEOUT",
                "retry_count": 0,
            }
        )
        assert result.rule == "test_rule"
    finally:
        STRATEGIES.pop("cdc_replicator", None)


def test_unrecognised_type_falls_back_to_conservative_default():
    assert get_strategy("not_a_real_type") is DEFAULT_STRATEGY
