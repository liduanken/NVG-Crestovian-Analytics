# Submission: Pipeline Triage Agent

> **DRAFT OUTLINE — REWRITE IN YOUR OWN WORDS BEFORE SUBMITTING.**
> Everything below is factual material from the build, with measured numbers.
> These are raw notes, not prose. Cut anything you would not want to be
> questioned on in a live conversation.

---

## 1. Architecture Decisions

**How did you structure the agent(s)?**

NOTES:
- Three tiers, in `app/triage.py`:
  `policy (deterministic, free) → LLM (only on abstention) → escalation gate`
- Tier 1 (`app/policy.py`): ordered rules over derived signals. A rule returns a
  decision or `None`. Abstention is a first-class outcome — it is how a rule says
  "this needs judgement", and it is the only thing that triggers spend.
- Tier 2: one LLM call, prompted with pre-computed signals rather than raw JSON.
- Tier 3 (`_apply_gate`): converts uncertainty into escalation. Low confidence,
  or non-high confidence on a critical pipeline, becomes `escalate` regardless of
  what the model asked for.
- `app/knowledge.py` computes `Signals` — every fact a rule branches on and an
  eval asserts, derived without a model.
- `/resolve` (`app/resolve.py`) is an explicit `while` loop, not a graph:
  `budget check → step cap → LLM → tool dispatch → repeat`, then the same gate.
- Measured: 5 of 6 fleet scenarios settled at tier 1, zero tokens.

**What did you consider and reject?**

NOTES:
- *Pure LLM triage* (what the starting code does) — fails all three constraints
  at once: unbounded cost, no way to express uncertainty, and `pipeline_type` is
  accepted but never used, so a new type means a rewrite.
- *Pure rules* — rejected because scenario 06 has no defensible rule.
- *Multi-agent (investigator + decider)* — doubles token cost for a single-turn
  decision, and the split gives the escalation gate two places to live.
- *Semantic cache over past decisions* — deferred, not rejected. Right lever at
  fleet scale; premature at 6 pipelines, and it would have masked the tiering win.

**Why this framework/library?**

NOTES:
- Raw OpenAI-compatible calls over `httpx`. No agent framework.
- At this size the loop *is* the architecture. Owning it directly is what makes
  the step cap, budget check, and pre-action guard enforceable in code rather
  than requested in a prompt.
- Concretely: `app/tools.py` blocks every write tool until `read_task_logs` has
  actually been called. That is a property of the dispatcher, not the prompt.
- Cost: `usage` is read per call, so the ledger is exact. Frameworks usually
  abstract this away, and I would not have caught the reasoning-token bug (§4).
- With LangGraph I would get retries, checkpointing and streaming for free, and
  trade away token-level accounting. At more than one pipeline type and a real
  human-in-the-loop step, that trade flips and I would move.
- <!-- TODO: frame this as a scale judgement, not a claim that frameworks are bad. -->

---

## 2. Handling Ambiguity & Escalation

**How does your system decide when to escalate?**

NOTES — hybrid, three distinct mechanisms, in order:
1. **Rules that escalate outright**: unknown pipeline (no catalog basis),
   corruption barred from auto-retry, retries exhausted, deterministic code
   defect, hard-deadline gate.
2. **Model self-report**: the prompt asks for honest confidence; an unstated
   confidence is treated as `low`, never `high`.
3. **Structural gate**: `low` → escalate always. Non-`high` on a `critical`
   pipeline → escalate. Zero-row success → confidence capped at `medium` in code.
- Failure modes also escalate: budget exhausted (`decided_by=budget_guard`), LLM
  unreachable or truncated (`decided_by=error`), step limit hit.
- Principle worth stating: **escalation is the default on every unknown path.**
  The system needs a positive reason to act, not a reason to escalate.

**Walk through the ambiguous scenario.**

NOTES on scenario 06 (`report_export`, succeeded, 0 rows vs expected 50–800):
- First: it is not a failure. DAG state is `success`. A triage system subscribed
  only to failed runs never sees it. That is the actual danger — it is a *silent*
  failure and nothing alerts on it.
- Every rule deliberately abstains. `rule_no_retry_after_sla` explicitly returns
  `None` when status is `succeeded`, because past-SLA-with-zero-rows is a data
  question, not a scheduling one.
- Two causes produce an identical observation: genuinely no qualifying client
  activity, or a broken date filter / upstream gap. Nothing in the event, catalog
  or run history separates them.
- `retry` is forbidden: the SLA window has passed and the catalog hands
  regeneration to the BI team, so retrying duplicates work a human already owns.
- Live behaviour observed: the model returned `skip`/`high` on one run and
  `escalate`/`low` on another — **non-deterministic at `temperature=0`**. That
  variance is itself the finding.
- Response: stopped asking the model to be humble and enforced it. A zero-row
  success now has confidence structurally capped at `medium`, because the
  observation genuinely cannot support more.
- <!-- TODO: give your own view. I lean escalate-with-context over skip, because
  the cost of a missed broken report exceeds one notification. Say what you would
  want as the on-call engineer. -->

---

## 3. Live Resolution Agent (`/resolve`)

**What tools did you give your agent?**

NOTES:
- Read (free): `get_pipeline_context`, `get_run_state`, `read_task_logs`,
  `read_dag_source`, `get_airflow_variable`.
- Write (gated): `set_airflow_variable`, `patch_dag_source`, `rerun_task`.
- Terminal: `submit_decision`.
- Deliberately excluded: arbitrary SQL, deleting DAG runs, pausing DAGs,
  `mark_success`. Out of reach by construction, not by instruction.
- `patch_dag_source` is constrained: exact-match single occurrence, refuses
  multi-match, path-confined to `airflow/dags/`, kill-switch via
  `ALLOW_DAG_PATCHING`.
- Log condensation matters: raw Airflow logs are ~14k chars of mostly scheduler
  noise. `condense_logs` cuts to ~3k while preserving the error — verified the
  `user_uuid` line survives even a 300-char budget. Largest cost lever in
  `/resolve`.

**How does the agent decide when it has enough information to act?**

NOTES:
- *Under-investigation* is prevented mechanically: no write tool executes until
  `read_task_logs` has been called. Returns `BLOCKED` — not a refusal the model
  can talk its way past.
- *Over-querying* is bounded by `MAX_RESOLVE_STEPS=8`, with a nudge injected at
  2 remaining telling it to submit with the confidence it can currently support.
- Hitting the cap is not a decision — it returns `escalate`/`low`.
- Budget is re-checked before every call, not once per request.
- Observed tool counts: 4 (clear-cut) to 8 (defect requiring a fix), so the cap
  binds rarely and cheap cases genuinely stay cheap.

**How does your agent handle a code bug vs. a transient failure?**

NOTES — both observed live, from a reset fleet:

(a) *Column-not-found, `web_analytics`* — 8 calls, 31,441 tokens, $0.00352:
`get_pipeline_context → get_run_state → read_task_logs → read_dag_source →
get_airflow_variable → set_airflow_variable → rerun_task → submit_decision`
- Read the DAG source, saw the column comes from an Airflow **Variable**, and
  corrected the Variable rather than editing the DAG. `git diff airflow/dags/`
  stayed empty. Pipeline reached `success`.
- This was the trap: the brief offers DAG patching as a tool, but the DAG's own
  `doc_md` says to check the Variable. Prompt rule: prefer configuration fixes
  over source edits.

(b) *Salesforce 504, `crm_sync`* — 6 calls, 20,937 tokens, $0.00247:
`get_pipeline_context → get_run_state → read_task_logs → read_dag_source →
rerun_task → submit_decision`
- No diagnosis phase. Confirmed transient against catalog history, checked retry
  budget, cleared the task.

- Key difference: the code-bug path needs a **remediate-then-verify** step, so
  `retry` there means "I fixed something and am re-running". The transient path
  means "nothing is wrong, run it again". Same terminal word, different warrant.
- This is why `/triage` and `/resolve` carry different ground truth for the same
  event (see §5).

---

## 4. Production Gaps

**What corners did you cut under time pressure?**

NOTES — be direct, do not soften:
- **The `/resolve` gate is weaker than the `/triage` gate.** It builds `Signals`
  from a stub dict, so the zero-rows confidence cap never applies there. Real fix
  is threading live run data into the gate. Known, unfixed.
- **Budget ledger is process-local.** Correct for one instance; wrong the moment
  you scale horizontally. Needs Redis or equivalent.
- **Policy overlay lives in code, not the catalog.** `POLICY_OVERLAY` encodes
  constraints the fixtures express as English prose in `notes`. Free text cannot
  be enforced. These belong as structured catalog fields; I kept them in code to
  avoid mutating provided fixture data.
- **No persistence.** Decisions are not stored — no audit trail, no feedback loop.
- **No retry/backoff on the LLM call itself.** A transient 503 escalates. Safe,
  but noisy.
- **`escalate` does not actually notify anyone.** No PagerDuty, no Slack.
- **Prices are unverified.** `$0.10`/`$0.40` per 1M are gemini-2.0-flash rates.
  Every cost number here is a floor. <!-- TODO: verify and correct. -->
- **Single-run eval**, despite having observed non-determinism at
  `temperature=0`.

**What would you add before shipping this to a real client?**

NOTES, prioritised:
1. **Persist every decision** (event, signals, tier, action, confidence,
   evidence, cost). Without it there is no audit trail, no way to build ground
   truth for ambiguous cases, and no way to detect drift. Blocks everything else.
2. **A real escalation channel** with evidence attached. An `escalate` that
   reaches nobody is a silent failure — the exact bug class in scenario 06.
3. **Shared budget ledger + per-pipeline rate limits**, so one hot-looping DAG
   cannot consume the fleet's daily budget.
4. **Shadow mode** — run two weeks proposing actions without executing, compared
   against what on-call actually did. That is how you earn the right to act
   autonomously on a critical pipeline.

**How did you think about the $3/day cost constraint?**

NOTES:
- Reframed it: the constraint is not "use a cheap model", it is **"do not send
  routine work to a model at all"**.
- Measured: 5/6 scenarios settled at zero tokens. `/triage` spend $0.00080 for a
  full fleet sweep; projected ~$0.006/day at 15 pipelines × 3 runs.
- `/resolve` averages $0.0022 per investigation (~20× a triage call) — acceptable
  because it only runs on demand.
- **The bug that mattered**: gemini-3.x reports `completion_tokens: 0` alongside
  `total_tokens: 100`. Reasoning tokens are billed but hidden. The ledger was
  recording ~1 token where 1,315 were charged — the guard was decorative. Fixed
  by reconciling `max(reported, total − prompt)` in `app/llm.py`.
  State plainly: **a budget guard you have not verified against real provider
  accounting is not a budget guard.**
- Levers if the budget tightened, in order: push more cases into tier 1 (each new
  rule is permanently free); shrink prompts (signals instead of raw JSON); drop
  to `flash-lite`; cache by failure signature; batch non-urgent events.
- If it loosened, I would not spend it on triage. I would spend it on `/resolve`
  verifying more deeply before high-stakes actions.

---

## 5. Evaluation

**What does your eval test?**

NOTES — `evals/run_eval.py`, `evals/scenarios.py`. Four dimensions, not just
accuracy:
- **correctness** — action within `allowed`
- **harm** — action within `forbidden`, tracked separately because a wrong retry
  on corrupt data is not the same category of bad as a needless escalation
- **cost** — routine cases must make zero LLM calls; asserted, not hoped
- **calibration** — ambiguous cases must not return `high` confidence
- Runs offline by default (stubbed LLM, no spend), or `--live`, plus `--resolve`
  against real Airflow and `--reset` to restore fixtures.
- 44 unit tests alongside: policy rules, gate behaviour, write guards, log
  condensation, step limits, truncation handling, extensibility.
- Results from a reset fleet: `/triage` 6/6, 0 harmful, 0 miscalibrated, 5/6
  free. `/resolve` 6/6, $0.01342 total.

**What does your eval miss?**

NOTES:
- **Single sample per scenario.** The same input yielded `skip`/`high` and
  `escalate`/`low` on different runs at `temperature=0`. A one-shot eval cannot
  see that. Should be n≥10 with a variance bound.
- **Six scenarios, authored by the same person who wrote the rules.** Real
  coverage needs replayed production incidents.
- **The eval mutated what it measured.** `/resolve` repairs live state, so run
  two graded a fleet run one had already fixed. Fixed with `reset_fleet.sh`, but
  it is a reminder that any eval touching live systems is stateful by default.
- No adversarial cases: prompt injection via log content, catalog/reality
  mismatch, malformed events.
- No latency or cost regression gates in CI.
- Production shape: replay corpus from persisted decisions, n-sample variance,
  per-tier accuracy, drift alerts on tier-1 abstention rate, cost per decision
  as a tracked metric.

**How would you define "correct" for the ambiguous events?**

NOTES — the section to get right:
- I did not define a single correct action. Ground truth is `allowed` /
  `forbidden`. For routine cases these collapse to one answer; for scenario 06
  they do not, and pretending otherwise bakes a coin-flip into the eval and
  rewards guessing.
- What *is* defensible is the negative claim: not "the right answer is X" but
  "answer Y causes harm here". That is the assertion worth regressing against.
- Second axis: **calibration is scored separately from correctness.** On an
  ambiguous event, `skip`/`high` and `skip`/`low` are different results even
  though the action matches.
- Note the deprecated-pipeline case treats `escalate` as *forbidden*, not merely
  suboptimal — waking a human for a decommissioned pipeline is exactly the alert
  fatigue this system exists to remove. Escalation is not a free safe default.
- Building real ground truth over time: log every decision with its evidence;
  capture what the on-call engineer did and, more usefully, what they wished had
  happened; treat disagreements as the labelling queue. Ambiguous cases resolve
  into rules once enough labels exist — and each promotion into tier 1 makes that
  class permanently free.
- <!-- TODO: note that "correct" here should eventually be defined by the owning
  team, not the agent's author. The catalog `notes` field is where that knowledge
  already lives, unenforceably. -->

---

## 6. Deployment Sketch

**How would you deploy this to GCP (or Azure, AWS)?**

NOTES (GCP):
- FastAPI on **Cloud Run** — request-driven, scales to zero, fits bursty failure
  traffic.
- Airflow as **Cloud Composer**; the agent talks to it over the same REST API, so
  nothing in `app/airflow_client.py` changes.
- Events in via **Pub/Sub** from Airflow failure callbacks, not polling.
- **Firestore or Cloud SQL** for the decision log; **Memorystore/Redis** for the
  shared budget ledger and dedupe.
- Catalog moves out of `mock_data/*.json` into Cloud SQL, with the policy overlay
  as real columns.
- **Secret Manager** for the API key; Workload Identity, no static creds.
- Escalation via Pub/Sub → PagerDuty/Slack with evidence attached.
- Human-approval path for high-stakes actions: agent writes a proposed action, a
  human approves, a worker executes.

**What observability would you wire up?**

NOTES:
- The response model already carries the primitives: `decided_by`, `confidence`,
  `evidence`, `cost`, `tool_calls`, `actions_taken`, `stopped_because`. Emit
  these as structured logs and they become the dashboard.
- Metrics: decisions by tier and action; **tier-1 abstention rate** (rising =
  drift or a new failure class); escalation rate; cost per decision and daily
  burn vs ceiling; `/resolve` tool-call distribution and step-limit hit rate;
  time-to-decision.
- Alerts: daily spend >80% of ceiling; abstention-rate step change; any
  `forbidden` action in shadow mode; LLM error rate; step-limit hits trending up
  (agent is thrashing).
- **The metric I would actually watch**: rate of escalations the on-call engineer
  closed as no-action. That measures whether the system is earning its keep or
  just relocating the work.
- Trace every decision end to end, retaining prompt, tool results and raw model
  response — without that, an incident review cannot answer "why did it retry?".

---

## Appendix: notes on the starting repo

Findings worth keeping or cutting, your call:
- `.env.example` shipped `LLM_MODEL=gemma-4-31b-it`, which is not a real model id
  on any provider. The README's suggested `gemini-2.0-flash` and `gemini-2.5-flash`
  both now return 404 "no longer available" on new keys, while still appearing in
  the `/models` listing. Corrected to `gemini-3.5-flash`, pinned rather than
  `-latest` so eval baselines do not drift.
- `airflow/bootstrap.sh` uses `declare -A` (bash 4+) and fails on macOS's stock
  bash 3.2. Seeding completes; only the run-triggering loop dies. `reset_fleet.sh`
  is written bash-3.2-safe to avoid repeating this.
- Three provided tests asserted that every event reaches the LLM, which is the
  design being replaced. Repointed at an event the policy tier genuinely abstains
  on, and added `test_known_transient_decided_without_llm` to pin the zero-cost
  path.
- A local, gitignored `airflow/docker-compose.override.yml` remaps Postgres off
  the default host port. Note that Compose *appends* `ports` on merge — the
  `!override` tag is required to replace.
