"""Eval harness.

    uv run python -m evals.run_eval              # offline: policy tier + gate, no spend
    uv run python -m evals.run_eval --live       # real LLM for the ambiguous tail
    uv run python -m evals.run_eval --live --resolve   # also drive /resolve against Airflow

Scores four things, not just accuracy:

  correctness  -- action within `allowed`, and never within `forbidden`
  harm         -- a forbidden action, tracked separately because these are not
                  equivalent failures; a wrong retry on corrupt data is a
                  different category of bad than a needless escalation
  calibration  -- confidence honest on the ambiguous case
  cost         -- share of decisions settled with zero tokens, and $/decision
"""
import argparse
import json
import pathlib
import subprocess
import sys
from dataclasses import dataclass
from unittest.mock import patch

from app.config import settings
from app.models import ResolveRequest
from evals.scenarios import SCENARIOS, Scenario

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"

# Offline stand-in for the LLM tier. Deliberately returns low confidence on an
# ambiguous event so the escalation gate is exercised without network calls.
_STUB_CONTENT = json.dumps(
    {
        "action": "skip",
        "confidence": "low",
        "reasoning": "Cannot distinguish an empty source window from a filter defect.",
        "evidence": ["zero_rows", "no error_code"],
    }
)


def _stub_response():
    class _R:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{"message": {"content": _STUB_CONTENT}}],
                "usage": {"prompt_tokens": 1200, "completion_tokens": 80},
            }

    return _R()


@dataclass
class Result:
    scenario: Scenario
    action: str
    confidence: str
    decided_by: str
    reasoning: str
    llm_calls: int
    tokens: int
    cost: float

    @property
    def harmful(self) -> bool:
        return self.action in self.scenario.forbidden

    @property
    def correct(self) -> bool:
        return self.action in self.scenario.allowed and not self.harmful

    @property
    def cost_ok(self) -> bool:
        return not self.scenario.expect_no_llm or self.llm_calls == 0

    @property
    def calibrated(self) -> bool:
        if not self.scenario.expect_uncertainty:
            return True
        return self.confidence != "high" or self.action == "escalate"


def run_triage(live: bool) -> list[Result]:
    from app.triage import triage_event

    results = []
    for scenario in SCENARIOS:
        if live:
            decision = triage_event(scenario.event)
        else:
            with patch("app.triage.httpx.post", return_value=_stub_response()):
                decision = triage_event(scenario.event)
        results.append(
            Result(
                scenario=scenario,
                action=decision.action,
                confidence=decision.confidence,
                decided_by=decision.decided_by,
                reasoning=decision.reasoning,
                llm_calls=decision.cost.llm_calls,
                tokens=decision.cost.prompt_tokens + decision.cost.completion_tokens,
                cost=decision.cost.estimated_cost_usd,
            )
        )
    return results


def run_resolve() -> int:
    from app.resolve import resolve_task

    print(f"\n{DIM}── /resolve against live Airflow ──{RESET}")
    failures = 0
    spend = 0.0
    for scenario in SCENARIOS:
        allowed, forbidden = scenario.expectations_for("resolve")
        decision = resolve_task(
            ResolveRequest(dag_id=scenario.dag_id, task_id=scenario.task_id)
        )
        spend += decision.estimated_cost_usd
        ok = decision.action in allowed and decision.action not in forbidden
        failures += not ok
        mark = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        print(
            f"  {mark}  {scenario.dag_id:<20} {decision.action:<9} "
            f"conf={decision.confidence:<7} calls={decision.llm_calls} "
            f"tokens={decision.tokens_used} ${decision.estimated_cost_usd:.5f}"
        )
        print(f"        expect: {'|'.join(sorted(allowed))}  {DIM}never: {'|'.join(sorted(forbidden)) or '-'}{RESET}")
        print(f"        tools: {', '.join(decision.tool_calls or []) or 'none'}")
        if decision.actions_taken:
            print(f"        {YELLOW}acted:{RESET} {'; '.join(decision.actions_taken)}")
        print(f"        {DIM}{decision.reasoning[:190]}{RESET}")

    print(f"\n  /resolve spend: ${spend:.5f} across {len(SCENARIOS)} investigations")
    print(f"  {RED}{failures} FAILED{RESET}" if failures else f"  {GREEN}ALL PASSED{RESET}")
    return failures


def report(results: list[Result], live: bool) -> int:
    print(f"\n{DIM}── /triage decisions ({'live' if live else 'offline'}) ──{RESET}")
    for r in results:
        if r.harmful:
            mark = f"{RED}HARM{RESET}"
        elif r.correct:
            mark = f"{GREEN}PASS{RESET}"
        else:
            mark = f"{YELLOW}MISS{RESET}"
        print(
            f"  {mark}  {r.scenario.id:<28} {r.action:<9} conf={r.confidence:<7} "
            f"via={r.decided_by:<16} calls={r.llm_calls}"
        )
        if not r.cost_ok:
            print(f"        {YELLOW}cost: expected zero LLM calls, made {r.llm_calls}{RESET}")
        if not r.calibrated:
            print(f"        {YELLOW}calibration: high confidence on an ambiguous event{RESET}")
        print(f"        {DIM}{r.reasoning[:190]}{RESET}")

    total = len(results)
    correct = sum(r.correct for r in results)
    harmful = sum(r.harmful for r in results)
    miscost = sum(not r.cost_ok for r in results)
    miscal = sum(not r.calibrated for r in results)
    free = sum(r.llm_calls == 0 for r in results)
    spend = sum(r.cost for r in results)

    print(f"\n{DIM}── summary ──{RESET}")
    print(f"  correct            {correct}/{total}")
    print(f"  harmful actions    {harmful}      {DIM}(must be 0){RESET}")
    print(f"  cost violations    {miscost}      {DIM}(routine cases must cost nothing){RESET}")
    print(f"  miscalibrated      {miscal}      {DIM}(ambiguous cases must not be confident){RESET}")
    print(f"  settled w/o LLM    {free}/{total}  {DIM}({free / total:.0%} of fleet traffic){RESET}")
    print(f"  spend this run     ${spend:.5f}")

    per_day = (spend / total) * 15 * 3 if total else 0
    print(
        f"  projected          ${per_day:.4f}/day at 15 pipelines x 3 runs "
        f"{DIM}(ceiling ${settings.daily_budget_usd:.2f}){RESET}"
    )

    failed = harmful or miscost or miscal or correct < total
    print(f"\n  {RED}FAILED{RESET}" if failed else f"\n  {GREEN}ALL CHECKS PASSED{RESET}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline triage agent eval")
    parser.add_argument("--live", action="store_true", help="use the real LLM provider")
    parser.add_argument("--resolve", action="store_true", help="also exercise /resolve")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="restore the seeded failure state first (required for a repeatable --resolve run)",
    )
    args = parser.parse_args()

    if args.live and not settings.llm_api_key:
        print(f"{RED}LLM_API_KEY is not set; cannot run --live.{RESET}")
        return 2

    if args.reset:
        script = pathlib.Path(__file__).parent / "reset_fleet.sh"
        if subprocess.run(["bash", str(script)]).returncode != 0:
            print(f"{RED}Fleet reset failed; results would not be reproducible.{RESET}")
            return 2

    code = report(run_triage(args.live), args.live)
    if args.resolve:
        code += run_resolve()
    return 1 if code else 0


if __name__ == "__main__":
    sys.exit(main())
