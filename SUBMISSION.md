# Submission: Pipeline Triage Agent

## 1. Architecture Decisions

**How did you structure the agent(s)?**

I structured triage as a three-stage decision process. It starts with deterministic policy rules for known operational patterns, uses an LLM only when those rules deliberately abstain, and finishes with a safety gate. The gate turns low confidence into escalation and prevents an uncertain autonomous action on a critical pipeline.

The policy layer works from derived operational signals such as failure type, retry budget, criticality, SLA status, and recent history. This keeps routine decisions fast, auditable, and free of LLM cost. In the fleet evaluation, five of the six scenarios were resolved at this stage without using tokens.

The live resolution path follows the same principle, but it is a bounded investigation loop. It checks the remaining budget, asks the model for the next investigative step, executes only an allowed action, and repeats until there is enough evidence or the step limit is reached. Every exit path applies the same escalation posture: uncertainty should result in a human handoff, not a confident guess.

I also separated pipeline-specific behavior through a strategy registry. Adding a stream consumer does not require restructuring the endpoints or the control flow. It requires adding that pipeline type's failure semantics and escalation rules to the registry.

**What did you consider and reject?**

I did not use a pure LLM approach because it would make routine work unnecessarily expensive and would leave uncertainty mostly as a prompt-writing problem. The starting implementation also did not use pipeline type to drive behavior, so a new type would have created a maintenance problem.

I also rejected a pure rules solution. Some events, especially a successful run with zero output, do not contain enough evidence for a defensible fixed rule. A multi-agent design would add cost and complexity without improving a single investigation loop, and it would make the safety boundary harder to reason about.

I considered a semantic cache for repeated incidents, but deferred it. It would be useful at higher volume, after a stable decision history exists. At this scale, I wanted to measure the benefit of resolving known cases without an LLM before adding another source of behavior.

**Why this framework/library?**

I used a small FastAPI service with direct OpenAI-compatible HTTP calls rather than an agent framework. For this scope, the loop is simple enough to own directly. That makes the budget checks, step cap, and safeguards before write actions enforceable in code rather than merely requested in a prompt.

The service accounts for usage after every model call. This exposed an important provider-accounting issue: reasoning tokens were reflected in total usage even when completion tokens were reported as zero. I corrected the accounting to reconcile the reported totals, because a budget guard is only meaningful when it reflects actual billable use.

I would revisit this choice for a larger system. A workflow framework would be attractive once the service needs durable checkpoints, retries, streaming, multiple pipeline types at scale, and human approvals. At that point, I would retain explicit cost and action guards around the framework integration.

## 2. Handling Ambiguity & Escalation

**How does your system decide when to escalate?**

The system escalates immediately when policy identifies a condition that should never be resolved automatically. Examples include an unknown pipeline, a corruption event that is barred from retry, an exhausted retry budget, a deterministic code defect, or a hard operational deadline.

For cases that reach the model, missing confidence is treated as low confidence. The final gate escalates any low-confidence recommendation. It also requires high confidence before allowing an autonomous action on a critical pipeline. A successful run that produces zero rows cannot receive high confidence based on the available observation alone.

The same conservative behavior applies when the system cannot complete its reasoning safely. A budget limit, unavailable or truncated model response, or investigation step limit all lead to escalation. I designed this as a positive-authorisation model: the agent needs sufficient evidence to act, rather than a reason to avoid escalation.

**Walk through the ambiguous scenario.**

The report export scenario is intentionally difficult because the run is marked successful but returns zero rows when the expected range is 50 to 800. A failure-only monitoring system would miss it entirely, which makes it a silent failure rather than a routine failed task.

There are two plausible explanations: there may genuinely have been no qualifying client activity, or a filter, date window, or upstream dependency may be broken. The event, catalog, and run history do not distinguish between those explanations. Retrying is not appropriate because the SLA has passed and the owning team already owns post-SLA regeneration.

In live runs, the model returned different conclusions for the same facts despite a zero temperature setting. That confirmed the design concern rather than changing the business conclusion. I cap confidence at medium for a zero-row success and would escalate it with the supporting context. From an on-call perspective, one well-evidenced notification is preferable to silently accepting a potentially broken client-facing report.

## 3. Live Resolution Agent

**What tools did you give your agent?**

I gave the agent a limited set of capabilities to inspect pipeline context, run state, logs, source configuration, and relevant runtime settings. It can correct an approved configuration value, make a tightly constrained source change when explicitly enabled, rerun a task, and submit a final decision.

I deliberately excluded broad or destructive capabilities such as arbitrary database access, deleting runs, pausing pipelines, or marking a run successful. Those actions should not become available simply because a prompt asks for them.

Write actions are protected by the dispatcher. The agent must inspect task logs before it can change configuration, source, or execution state. Source changes are limited to an exact single match within the DAG directory and can be disabled entirely. I also condense noisy task logs before sending them to the model; this preserves the actionable error while reducing the largest source of investigation cost.

**How does the agent decide when it has enough information to act?**

The agent cannot act before it has examined the task logs, so it cannot skip the primary evidence. It also has a fixed investigation limit of eight model turns. When only two turns remain, it is instructed to stop gathering marginal evidence and make the best-supported decision available.

The budget is checked before every model call rather than only once at the beginning. If the agent reaches the step limit, exhausts its budget, or cannot reach a valid conclusion, it escalates with low confidence. This keeps both under-investigation and open-ended investigation bounded.

In the live fleet runs, clear cases required four tool calls and the configuration-defect case required eight. The cap therefore provides a practical guardrail without penalising the normal path.

**How does your agent handle a code bug vs. a transient failure?**

For the analytics pipeline with a missing-column error, the agent investigated the logs and source, identified that the value was controlled through configuration, corrected the configuration, and then reran the task to verify the result. It reached success without changing the DAG source. The important decision was to prefer the least invasive fix and verify it before reporting resolution.

For the Salesforce timeout, the evidence matched a known transient issue with retry capacity remaining. The agent verified the failure context and reran the task. No remediation was needed because the expected behavior was recovery on retry.

Both outcomes can be described as retrying, but the rationale differs. A transient retry assumes the system is healthy and the failure is temporary. A retry after a code or configuration fix is a verification step. I keep those distinctions visible in the evidence and actions recorded by the resolution response.

## 4. Production Gaps

**What corners did you cut under time pressure?**

The resolution path currently builds its final safety signals from a minimal context object. As a result, the zero-row confidence cap that protects triage does not yet apply to live resolution. The proper fix is to carry the live run output into that final gate.

The budget ledger is local to one process, which is correct for this proof of concept but not for horizontally scaled deployment. The policy overlay also lives in code because the supplied catalog expresses important constraints as free-text notes. In production, those constraints should be structured catalog fields rather than code constants.

The service does not yet persist decisions, retry transient model failures, or notify a real escalation channel. The model pricing assumptions have not been independently verified, and the evaluation runs each scenario once despite observed model variability. These are known limitations, not behavior I would present as production-ready.

**What would you add before shipping this to a real client?**

My first priority would be a durable decision record containing the event, derived signals, decision tier, action, confidence, evidence, and cost. Without that record, there is no audit trail, no reliable feedback loop, and no corpus for improving ambiguous decisions.

Next, I would connect escalation to an actual operating process such as PagerDuty or Slack, with the relevant evidence attached. An escalation that reaches no one is simply another silent failure.

I would then replace the local budget ledger with a shared store, add per-pipeline rate limits and deduplication, and run in shadow mode before allowing autonomous changes on high-stakes pipelines. In shadow mode, the service would propose actions while the on-call team retains control, allowing us to compare recommendations with real operational outcomes.

**How did you think about the $3/day cost constraint?**

I treated the budget as a routing problem rather than a model-selection problem. The biggest cost reduction comes from keeping routine, well-understood events out of the model entirely. In the fleet evaluation, five of six triage scenarios required no model call. The live triage sweep cost approximately $0.0008, which projects to roughly $0.006 per day at 15 pipelines running three times each.

Live resolution costs more because it is a multi-step investigation, averaging about $0.0022 per case. That is acceptable because it should be invoked only for an incident that warrants deeper investigation.

The key implementation lesson was validating provider usage rather than trusting an individual usage field. I reconcile completion usage with total reported usage so reasoning tokens are not missed. If the budget became tighter, I would first expand deterministic coverage, then reduce prompt size, use a lower-cost model, cache repeated failure signatures, and batch non-urgent events. With more budget, I would invest it in better investigation and verification, not in sending routine triage work to a larger model.

## 5. Evaluation

**Where it lives and how to run it**

    make eval        # offline, stubbed model, no spend
    make eval-live   # resets the fleet, then runs live LLM triage and /resolve
    make test        # unit suite

| File | Purpose |
| --- | --- |
| `evals/scenarios.py` | The six scenarios and their permitted/forbidden outcomes |
| `evals/run_eval.py` | Harness and scoring |
| `evals/reset_fleet.sh` | Restores seeded failure state between live runs |

`make eval-live` requires the Airflow stack to be running and `LLM_API_KEY` to be
set. The offline mode needs neither.

**What does your eval test?**

The evaluation measures more than whether the final action looks reasonable. It checks whether the action is permitted for the scenario, whether it falls into an explicitly harmful category, whether routine cases made unnecessary model calls, and whether an ambiguous case was reported with inappropriate confidence.

The default evaluation uses a stubbed model so it can run offline without spend. A live mode exercises the ambiguous triage path, and a separate live-resolution mode runs against Airflow after resetting the fleet state. The unit suite also covers the policy rules, safety gate, write protection, log condensation, step limits, response truncation, and pipeline-type extensibility.

On a reset fleet, triage resolved all six scenarios with no harmful or miscalibrated decisions, and five needed no model call. Live resolution also resolved all six scenarios within the allowed outcomes at a total cost of $0.01342.

**What does your eval miss?**

The evaluation is still small and should not be mistaken for production validation. Each scenario is sampled once, even though the ambiguous case produced different model answers across live runs. A stronger evaluation would run multiple samples per scenario and track variance as a failure condition.

The six scenarios were authored alongside the policy rules, which creates a natural risk of overfitting. The next meaningful dataset would be replayed production incidents with independently reviewed outcomes. The live resolution evaluation also changes state, so the fleet must be reset between runs to avoid grading a repair made by an earlier test.

I would add adversarial inputs, malformed events, catalog-versus-reality mismatches, latency and cost regression checks, and an ongoing replay corpus built from persisted decisions. I would also monitor whether the deterministic layer starts abstaining more often, as that would indicate drift or a new failure class.

**How would you define "correct" for the ambiguous events?**

For an ambiguous event, I would not force a single correct action when the available evidence does not support one. The evaluation uses permitted and forbidden outcomes. For routine cases, those often collapse to one answer. For an ambiguous case, the stronger claim is that certain actions would be harmful, while more than one cautious response may be acceptable.

Calibration is a separate requirement. The same action with high confidence and low confidence is not equivalent when the facts are ambiguous. The evaluation therefore rejects unwarranted high confidence even when the action itself is allowed.

Escalation is not automatically harmless either. Escalating a deprecated pipeline creates alert fatigue without operational value, so it is treated as forbidden in that scenario. Over time, the owning team should define the ground truth. I would use persisted evidence, on-call actions, and feedback on what should have happened to turn recurring ambiguity into clearer policy rules.

## 6. Deployment Sketch

**How would you deploy this to GCP (or Azure, AWS)?**

On GCP, I would deploy the FastAPI service to Cloud Run and operate Airflow through Cloud Composer. Airflow failure callbacks would publish events to Pub/Sub, allowing the service to respond to incidents rather than polling for them.

I would move the pipeline catalog and decision history to Cloud SQL or Firestore, with Redis used for the shared budget ledger and deduplication. Secrets would live in Secret Manager and workloads would use managed identities rather than static credentials.

For high-stakes actions, the agent would create a proposed action and evidence record first. A human approval step would then authorize execution. Escalations would be routed through Pub/Sub to the team's normal notification channels, with enough context for an on-call engineer to act without repeating the investigation.

**What observability would you wire up?**

I would emit structured events for the decision tier, action, confidence, evidence, cost, tool activity, actions taken, and stopping reason. Those fields provide the basis for both operational dashboards and incident review.

The core metrics would be decision volume by tier and action, deterministic-policy abstention rate, escalation rate, cost per decision, daily burn against budget, investigation length, step-limit rate, and time to decision. A sudden increase in policy abstention is especially useful because it can reveal a changed upstream system or a failure class that is not yet represented in the policy.

I would alert on budget consumption approaching the daily ceiling, model errors, rising step-limit hits, and any harmful action observed during shadow mode. The metric I would watch most closely is the share of escalations that on-call engineers close with no action. It directly shows whether the system is reducing operational work or merely transferring it to another queue.
