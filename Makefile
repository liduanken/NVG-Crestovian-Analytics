.DEFAULT_GOAL := help

.PHONY: help setup airflow-up airflow-down airflow-bootstrap run test curl-triage curl-resolve

help:
	@echo ""
	@echo "  setup             Install dependencies"
	@echo "  airflow-up        Start the local Airflow stack (Postgres + UI)"
	@echo "  airflow-bootstrap Seed run history and trigger sample failures"
	@echo "  airflow-down      Stop the Airflow stack"
	@echo "  run               Start the FastAPI server with live reload"
	@echo "  test              Run the test suite"
	@echo "  curl-triage       Send a sample event to POST /triage"
	@echo "  curl-resolve      Send a sample task to POST /resolve"
	@echo ""

# ── Environment ──────────────────────────────────────────────────────────────

setup:
	cp -n .env.example .env 2>/dev/null && echo "Created .env — add your API key" || echo ".env already exists"
	uv sync

# ── Airflow ───────────────────────────────────────────────────────────────────

airflow-up:
	cd airflow && docker compose up -d

airflow-bootstrap:
	cd airflow && ./bootstrap.sh

airflow-down:
	cd airflow && docker compose down

# ── API ───────────────────────────────────────────────────────────────────────

run:
	uv run uvicorn app.main:app --reload

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	uv run pytest -v

# ── Manual probes ─────────────────────────────────────────────────────────────

curl-triage:
	curl -s -X POST http://localhost:8000/triage \
	  -H "Content-Type: application/json" \
	  -d '{"event_id":"evt_0001","pipeline_id":"pl_crm_sync","pipeline_type":"batch_etl","timestamp":"2026-07-22T02:16:00Z","status":"failed","error_code":"SALESFORCE_TIMEOUT","retry_count":0}' \
	  | python3 -m json.tool

curl-resolve:
	curl -s -X POST http://localhost:8000/resolve \
	  -H "Content-Type: application/json" \
	  -d '{"dag_id":"crm_sync","task_id":"extract_from_salesforce"}' \
	  | python3 -m json.tool
