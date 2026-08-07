#!/usr/bin/env bash
# Restore the seeded failure state so an eval run is repeatable.
#
# The /resolve agent changes live state by design: it corrects Variables, clears
# tasks, and can patch DAG source. That makes the eval non-idempotent -- a second
# run measures a fleet the first run already repaired. Reset between runs.
#
# Written for bash 3.2 (macOS default). bootstrap.sh uses `declare -A`, which is
# bash 4+ only and fails on stock macOS.

set -uo pipefail

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
AUTH="${AIRFLOW_USER:-admin}:${AIRFLOW_PASSWORD:-admin}"
COMPOSE_DIR="$(cd "$(dirname "$0")/../airflow" && pwd)"
DAGS="crm_sync market_feed_legacy risk_scores revenue_daily web_analytics report_export"

echo "==> Restoring the broken Airflow Variable"
curl -sf -u "$AUTH" -X PATCH "${AIRFLOW_URL}/api/v1/variables/web_analytics_user_column" \
    -H "Content-Type: application/json" \
    -d '{"key":"web_analytics_user_column","value":"user_uuid"}' > /dev/null \
    && echo "    web_analytics_user_column=user_uuid" \
    || echo "    WARNING: could not reset variable"

echo "==> Re-seeding scenario failure state"
(cd "$COMPOSE_DIR" && docker compose exec -T postgres psql -U airflow -q -c "
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
") > /dev/null && echo "    scenario state re-seeded"

echo "==> Reverting any agent edits to DAG source"
(cd "$COMPOSE_DIR/.." && git checkout -- airflow/dags/ 2>/dev/null) \
    && echo "    airflow/dags/ restored" || echo "    (no DAG changes to revert)"

# Rewriting the DAG files makes the scheduler mark them inactive, and it does not
# recover on its own within the eval's timeframe. Without this the fleet looks
# empty and every trigger 404s.
echo "==> Re-serializing DAGs"
(cd "$COMPOSE_DIR" && docker compose exec -T airflow-scheduler airflow dags reserialize) > /dev/null 2>&1 \
    && echo "    DAGs serialized" || { echo "    ERROR: reserialize failed"; exit 1; }
sleep 5

echo "==> Triggering a fresh failing run per pipeline"
failed=0
for dag in $DAGS; do
    run_id=$(curl -sf -u "$AUTH" -X POST "${AIRFLOW_URL}/api/v1/dags/${dag}/dagRuns" \
        -H "Content-Type: application/json" -d '{"conf":{"source":"eval_reset"}}' \
        | python3 -c "import sys,json;print(json.load(sys.stdin).get('dag_run_id',''))" 2>/dev/null)
    printf "    %-22s %s\n" "$dag" "${run_id:-FAILED}"
    [ -z "$run_id" ] && failed=1
    sleep 2
done

# A partial reset produces a green eval against a fleet that was never restored,
# which is worse than no reset at all. Fail loudly instead.
if [ "$failed" -eq 1 ]; then
    echo "ERROR: one or more DAGs could not be triggered. Results would be invalid."
    exit 1
fi

echo "==> Waiting for the scheduler to land the runs"
sleep 45
echo "    done. Fleet is back in its seeded failure state."
