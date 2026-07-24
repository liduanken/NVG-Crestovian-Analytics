#!/usr/bin/env bash
# bootstrap.sh — seed the Airflow environment with scenario state and run history.
#
# Run this once after `docker compose up -d`. It waits for Airflow to be ready,
# injects 7 days of historical run data, seeds the current failure states for
# all 6 pipelines, and triggers one failing run per DAG.
#
# Usage:
#   cd pipeline-triage/airflow
#   docker compose up -d
#   ./bootstrap.sh

set -euo pipefail

AIRFLOW_URL="http://localhost:8080"
AUTH="admin:admin"

# ── Colours ──────────────────────────────────────────────────────────────────
bold()   { printf '\033[1m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
dim()    { printf '\033[2m%s\033[0m\n' "$*"; }

airflow_get() {
    curl -sf --max-time 10 -u "$AUTH" "${AIRFLOW_URL}/api/v1${1}" 2>/dev/null || echo '{}'
}

airflow_post() {
    curl -sf --max-time 10 -u "$AUTH" \
        -X POST "${AIRFLOW_URL}/api/v1${1}" \
        -H "Content-Type: application/json" \
        -d "${2:-{\}}" 2>/dev/null || echo '{}'
}

# ── Step 1: Wait for Airflow ─────────────────────────────────────────────────
bold "==> Waiting for Airflow to be healthy..."
attempts=0
until curl -sf --max-time 10 "${AIRFLOW_URL}/health" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if d.get('metadatabase',{}).get('status')=='healthy' else 1)" \
    2>/dev/null; do
    attempts=$((attempts + 1))
    if [ $attempts -ge 36 ]; then
        red "ERROR: Airflow did not become healthy after 3 minutes."
        echo "Check: docker compose logs airflow-webserver"
        exit 1
    fi
    printf '.'
    sleep 5
done
echo ""
green "  Airflow is healthy."

# ── Step 2: Force DAG serialization ──────────────────────────────────────────
# The scheduler discovers DAGs on a 30s interval; reserialize ensures they're
# in the database immediately without waiting for that cycle.
bold "==> Serializing DAGs into the Airflow database..."
docker compose exec airflow-scheduler airflow dags reserialize > /dev/null 2>&1
green "  DAGs serialized."

# ── Step 3: Confirm DAGs are visible via the REST API ────────────────────────
bold "==> Waiting for DAGs to be parsed by the scheduler..."
expected_dags=("crm_sync" "market_feed_legacy" "risk_scores" "revenue_daily" "web_analytics" "report_export")
attempts=0
while true; do
    parsed=0
    for dag in "${expected_dags[@]}"; do
        if airflow_get "/dags/${dag}" | grep -q '"dag_id"'; then
            parsed=$((parsed + 1))
        fi
    done
    if [ "$parsed" -eq "${#expected_dags[@]}" ]; then
        break
    fi
    attempts=$((attempts + 1))
    if [ $attempts -ge 24 ]; then
        yellow "WARNING: Only ${parsed}/${#expected_dags[@]} DAGs parsed after 2 minutes."
        yellow "Check: docker compose logs airflow-scheduler"
        break
    fi
    dim "  ${parsed}/${#expected_dags[@]} DAGs parsed, waiting..."
    sleep 5
done
green "  All DAGs parsed."

# ── Step 3: Seed historical run data ─────────────────────────────────────────
bold "==> Seeding 7 days of historical run data..."
docker compose exec -T postgres psql -U airflow -q \
    -f /seed/seed_history.sql
green "  Historical run data seeded."

# ── Step 4: Seed the web_analytics_user_column Airflow Variable ──────────────
bold "==> Seeding web_analytics_user_column Variable..."
airflow_post "/variables" '{"key":"web_analytics_user_column","value":"user_uuid"}' > /dev/null
green "  Variable seeded: web_analytics_user_column=user_uuid"

# ── Step 5: Seed current failure states ──────────────────────────────────────
bold "==> Seeding current failure states..."
docker compose exec -T postgres psql -U airflow -q -c "
    INSERT INTO eval_scenario_state (dag_id, failure_mode, retry_count_remaining, error_code)
    VALUES
        ('crm_sync',      'transient',       2, 'SALESFORCE_TIMEOUT'),
        ('risk_scores',   'persistent',      0, 'UPSTREAM_UNAVAILABLE'),
        ('revenue_daily', 'data_corruption', 0, 'DATA_CORRUPTION'),
        ('report_export', 'zero_rows',       1,  NULL)
    ON CONFLICT (dag_id) DO UPDATE SET
        failure_mode          = EXCLUDED.failure_mode,
        retry_count_remaining = EXCLUDED.retry_count_remaining,
        error_code            = EXCLUDED.error_code,
        updated_at            = now();
"
green "  Failure states seeded."

# ── Step 5: Trigger one run per pipeline (with small gap to avoid overloading LocalExecutor)
bold "==> Triggering failing runs for all 6 pipelines..."
declare -A run_ids

for dag in "${expected_dags[@]}"; do
    resp=$(airflow_post "/dags/${dag}/dagRuns" '{"conf":{"source":"bootstrap"}}')
    run_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('dag_run_id',''))" 2>/dev/null || echo "")
    if [ -n "$run_id" ]; then
        run_ids[$dag]="$run_id"
        dim "  Triggered ${dag}: ${run_id}"
    else
        yellow "  WARNING: Could not trigger ${dag}"
    fi
    sleep 2  # small gap so the scheduler isn't flooded at once
done
green "  All runs triggered."

# ── Step 6: Brief wait then snapshot state ───────────────────────────────────
bold "==> Giving the scheduler 30 seconds to pick up the runs..."
for i in $(seq 1 6); do printf '.'; sleep 5; done
echo ""

declare -A final_states
for dag in "${expected_dags[@]}"; do
    run_id="${run_ids[$dag]:-}"
    if [ -z "$run_id" ]; then
        final_states[$dag]="not triggered"
        continue
    fi
    state=$(airflow_get "/dags/${dag}/dagRuns/${run_id}" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null \
        || echo "unknown")
    final_states[$dag]="$state"
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
bold "── Crestovian Analytics Pipeline Fleet ───────────────────────────────────"
echo ""
printf "  %-24s %-12s %s\n" "PIPELINE" "STATE" "OBSERVED FAULT"
printf "  %-24s %-12s %s\n" "────────" "─────" "──────────────"

declare -A scenarios=(
    ["crm_sync"]="extract_from_salesforce raising SALESFORCE_TIMEOUT"
    ["market_feed_legacy"]="ingest_bloomberg_feed raising API_DEPRECATED"
    ["risk_scores"]="fetch_account_positions raising UPSTREAM_UNAVAILABLE"
    ["revenue_daily"]="validate_checksums raising DATA_CORRUPTION"
    ["web_analytics"]="aggregate_metrics raising a ColumnNotFound error"
    ["report_export"]="run succeeded but query returned 0 rows"
)

for dag in "${expected_dags[@]}"; do
    state="${final_states[$dag]:-unknown}"
    scenario="${scenarios[$dag]}"
    printf "  %-24s %-12s %s\n" "$dag" "$state" "$scenario"
done

echo ""
bold "── Ready ────────────────────────────────────────────────────────────────"
echo ""
echo "  Airflow UI:  http://localhost:8080  (admin / admin)"
echo "  REST API:    http://localhost:8080/api/v1"
echo ""
echo "  Runs are in progress — some will still be running or queued."
echo "  Some runs are still retrying — check the UI for live task state."
echo ""
dim "  Each pipeline currently has a failing or degraded run. Point your"
dim "  /triage and /resolve endpoints at the fleet and see what your agent does."
echo ""
