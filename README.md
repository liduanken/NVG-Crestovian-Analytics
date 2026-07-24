# Pipeline Triage Agent — Take-Home Assignment

**Role**: Senior AI Engineer  
**Suggested time**: 4–6 hours  
**Submission**: Return this repo (forked GitHub link) with your code changes and a completed `SUBMISSION.md`

---

## Background

Crestovian Analytics is a data consultancy running 15+ daily ETL and batch pipelines that feed client-facing dashboards and internal reporting systems. When a pipeline fails or produces degraded output, an on-call engineer manually investigates: checking error logs, querying run history, and deciding whether to retry, skip, or escalate. This takes between 30 and 90 minutes per incident.

You've been brought in to build a proof-of-concept agentic triage system. The goal is for the agent to handle routine failures autonomously and escalate to a human only when genuinely uncertain. Crestovian also wants to onboard a second pipeline type (`stream_consumer`) next quarter and they've made it clear they don't want a full rewrite to do it.

---

## What You're Working With

### `airflow/`

A local Airflow environment pre-loaded with 6 real DAGs representing the Crestovian pipeline fleet. This is the live system your agent will connect to for the `/resolve` endpoint (see Deliverables).

| File | Contents |
|---|---|
| `docker-compose.yml` | Airflow 2.9 + LocalExecutor + Postgres. One command to start. |
| `dags/` | 6 pipeline DAGs: `crm_sync`, `market_feed_legacy`, `risk_scores`, `revenue_daily`, `web_analytics`, `report_export` |
| `scripts/init_db.sql` | Creates evaluation state tables (`eval_scenario_state`, `web_events`) at DB init |
| `scripts/seed_history.sql` | Seeds 7 days of historical `dag_run` rows; run by `bootstrap.sh` after Airflow migrates |

Each DAG has a `doc_md` block describing the pipeline's catalog entry — owner, criticality, SLA, downstream dependencies, and operational notes. The Airflow UI (localhost:8080) and REST API are both available once the stack is up.

### `mock_data/`

| File | Contents |
|---|---|
| `pipeline_catalog.json` | Metadata for the 6 pipelines currently managed: owners, SLAs, downstream dependencies, criticality, retry policies, notes, and the `dag_id` each maps to |
| `run_history.json` | Recent run outcomes per pipeline, including retry counts and any resolution notes |

> **Note on IDs**: `/triage` events use the catalog `pipeline_id` (e.g. `pl_crm_sync`); the live Airflow DAGs use a `dag_id` (e.g. `crm_sync`). Each catalog entry carries an explicit `dag_id` field so you can correlate the two — the mapping is `pl_<dag_id>` for every pipeline **except** `pl_market_feed`, whose DAG is `market_feed_legacy`.

### `app/`

A minimal FastAPI application with a single `POST /triage` endpoint. `triage.py` contains a starting implementation that works for simple cases. You're welcome to refactor or replace it entirely.

### `tests/`

A shallow test stub. It passes.

---

## Constraints

These are real constraints for the project. Your design decisions should fit within these contraints.

**1. Cost ceiling: $3.00/day LLM spend**  
This is a PoC budget. At typical API pricing, with 15+ pipelines running multiple times a day, understand and plan for how this would scale into production.

**2. Ambiguous cases exist**  
Not every pipeline failure has a clear-cut answer. The agent must be clear in its decisions and when it doesn't know. An agent that confidently produces a wrong answer is worse than one that escalates appropriately.

**3. Extensibility: a second pipeline type is coming**  
`stream_consumer` pipelines will have different failure modes, different retry semantics, and different escalation rules. Your architecture should accommodate a new pipeline type without restructuring the agent.

---

## Deliverables

**1. Working Python system**  
Must run locally with `uvicorn app.main:app`. The system must expose two endpoints:

- `POST /triage` — processes a pipeline event and returns a structured triage decision. A minimal implementation is provided; improve or replace it.
- `POST /resolve` — connects to the live Airflow instance, investigates a failing DAG task, takes action (`retry`, `skip`, or `escalate`), and returns what it did. Patching a DAG file or its config is one of the tools available to the agent when the failure is a code bug — the terminal decision it reports is still one of retry / skip / escalate. This endpoint is a stub that raises `NotImplementedError` — you must implement it.

Framework, agent architecture, and LLM provider are your choice.

**2. Completed `SUBMISSION.md`**  
The template asks you to explain your architecture decisions, how you handled ambiguous events, how your `/resolve` agent decides when it has enough information to act, and how you thought about cost and evaluation. Specificity matters more than length.

**3. A minimal eval**  
A script or test that validates AI behaviour and that the agent makes sensible decisions. What you test and how you test it is part of the solution. Include it in the repo and reference it in your `SUBMISSION.md`.

---

## Getting Started

```bash
cp .env.example .env
# Add your API key to .env

uv sync

# Start the Airflow environment (Postgres on :5432, Airflow UI on :8080)
cd airflow
docker compose up -d

# Bootstrap the fleet: seeds 7 days of run history, seeds today's failures,
# and triggers one failing run per pipeline. Takes ~2 minutes.
./bootstrap.sh
cd ..

# Run the API
uv run uvicorn app.main:app --reload

# Run the existing tests
uv run pytest
```

After bootstrap, the Airflow UI at http://localhost:8080 will show all 6 pipelines with run history and a current failure in each one — a realistic fleet for your agent to investigate.

To send a triage event manually:
```bash
curl -X POST http://localhost:8000/triage \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt_0001","pipeline_id":"pl_crm_sync","pipeline_type":"batch_etl","timestamp":"2026-07-22T02:16:00Z","status":"failed","error_code":"SALESFORCE_TIMEOUT","retry_count":0}'
```

To call the resolve endpoint once you've implemented it:
```bash
curl -X POST http://localhost:8000/resolve \
  -H "Content-Type: application/json" \
  -d '{"dag_id":"crm_sync","task_id":"extract_from_salesforce"}'
```

There is also a Makefile with these commands for convenience

The Airflow UI is at http://localhost:8080 (admin / admin). The REST API base is http://localhost:8080/api/v1.

---

## What We're Looking For

We care more about how you think than what you produce. A well-reasoned system with honest caveats is more interesting to us than a polished implementation that doesn't acknowledge its gaps.

Specifically:

- **Do you identify the limitations of the starting implementation?** A senior engineer should see immediately what's missing and make deliberate choices about what to address.
- **Are your architecture decisions justified?** The `SUBMISSION.md` matters as much as the code and they should match.
- **How do you handle uncertainty?** The ambiguous events are the most important part of this assignment. There is no single correct answer and we want to see your reasoning.
- **Do you think like an engineer building for production?** Cost, observability, and failure modes should be present in your thinking, even if not fully implemented.

---

## Submission

Return your completed repo as a GitHub link. Make sure `SUBMISSION.md` is filled in and your eval script is present and runnable.

If anything in this brief is unclear, make a reasonable assumption and note it in your `SUBMISSION.md`.
