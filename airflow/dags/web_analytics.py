"""
Web Analytics Aggregation — pl_web_analytics

Aggregates hourly web event metrics from the web_events table.
Three-stage pipeline: extract raw events → aggregate metrics → write summary.
Runs every 2 hours. Dashboard has 4-hour staleness tolerance.

The SQL column used for unique-user counting is driven by the Airflow Variable
'web_analytics_user_column'. extract_raw_events uses no Variable and always
succeeds; aggregate_metrics fails if the Variable value does not match the
web_events schema; write_summary runs only once aggregation succeeds.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_raw_events(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    result = hook.get_records(
        "SELECT COUNT(*) FROM web_events WHERE event_timestamp > NOW() - INTERVAL '2 hours'"
    )
    count = result[0][0] if result else 0
    print(f"Extracted {count} raw web events from the past 2 hours.")
    return {"raw_event_count": count}


def aggregate_metrics(**context):
    from airflow.models import Variable

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Column name is configured via an Airflow Variable so it can be corrected
    # without a DAG deploy. If the Variable is wrong you'll see a ColumnNotFound error.
    user_col = Variable.get("web_analytics_user_column", default_var="user_uuid")

    result = hook.get_records(f"""
        SELECT
            DATE_TRUNC('hour', event_timestamp) AS hour,
            COUNT(DISTINCT {user_col}) AS unique_users,
            COUNT(*) AS total_events
        FROM web_events
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 100
    """)

    bucket_count = len(result)
    print(f"Aggregated {bucket_count} hourly buckets using column '{user_col}'.")
    return {"buckets_produced": bucket_count, "user_column": user_col}


def write_summary(**context):
    ti = context["ti"]
    aggregated = ti.xcom_pull(task_ids="aggregate_metrics") or {}
    buckets = aggregated.get("buckets_produced", 0)
    print(f"Writing {buckets} hourly metric buckets to analytics_summary.")
    print("product_dashboard notified. Staleness window reset.")
    return {"buckets_written": buckets, "status": "success"}


with DAG(
    dag_id="web_analytics",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "owner": "product-analytics",
        "email": ["analytics@crestovian.example"],
    },
    tags=["analytics", "batch_etl", "medium"],
    doc_md="""
## Web Analytics Aggregation

- **Pipeline ID**: pl_web_analytics
- **Owner**: product-analytics (analytics@crestovian.example)
- **Criticality**: medium
- **Schedule**: every 2 hours
- **Source**: web_events table (postgres_default)
- **Downstream**: product_dashboard (4-hour staleness tolerance)

Row counts vary with traffic. No hard SLA. A single failed run is covered
by the next scheduled execution 2 hours later.

The unique-user column name is configured via the Airflow Variable
`web_analytics_user_column`. If you see a ColumnNotFound error, verify
the Variable value matches the actual schema.
""",
) as dag:
    extract = PythonOperator(
        task_id="extract_raw_events",
        python_callable=extract_raw_events,
    )
    aggregate = PythonOperator(
        task_id="aggregate_metrics",
        python_callable=aggregate_metrics,
    )
    write = PythonOperator(
        task_id="write_summary",
        python_callable=write_summary,
    )

    extract >> aggregate >> write
