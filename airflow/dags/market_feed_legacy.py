"""
Market Data Feed (Legacy) — pl_market_feed

DEPRECATED: Fully migrated to pl_market_feed_v2 on 2026-03-01.
End-of-life: 2026-07-01.

Three-stage pipeline: ingest → parse → store.

ingest_bloomberg_feed raises API_DEPRECATED; parse_feed_data and
store_market_prices run only once ingestion succeeds.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def ingest_bloomberg_feed(**context):
    raise Exception(
        "API_DEPRECATED: The bloomberg_api_v1 endpoint has been decommissioned. "
        "Migration to pl_market_feed_v2 was completed on 2026-03-01. "
        "This pipeline reached its end-of-life date on 2026-07-01 and is no longer maintained. "
        "Do not retry or escalate — decommission is expected and intentional."
    )


def parse_feed_data(**context):
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="ingest_bloomberg_feed") or {}
    tick_count = raw.get("tick_count", 0)
    print(f"Parsing {tick_count} Bloomberg tick records into normalised OHLCV format.")
    return {"records_parsed": tick_count}


def store_market_prices(**context):
    ti = context["ti"]
    parsed = ti.xcom_pull(task_ids="parse_feed_data") or {}
    records = parsed.get("records_parsed", 0)
    print(f"Storing {records} OHLCV records into market_prices table.")
    return {"records_stored": records, "status": "success"}


with DAG(
    dag_id="market_feed_legacy",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "market-data",
        "email": ["market-data@crestovian.example"],
    },
    tags=["market-data", "api_ingest", "low", "deprecated"],
    doc_md="""
## Market Data Feed (Legacy) — DEPRECATED

- **Pipeline ID**: pl_market_feed
- **Owner**: market-data (market-data@crestovian.example)
- **Criticality**: low
- **Deprecated**: 2026-03-01
- **End-of-life**: 2026-07-01
- **Replacement**: pl_market_feed_v2

Fully migrated to pl_market_feed_v2. This pipeline is in wind-down.
**Failures should NOT be actioned** — the pipeline will be decommissioned at end_of_life_date.
""",
) as dag:
    ingest = PythonOperator(
        task_id="ingest_bloomberg_feed",
        python_callable=ingest_bloomberg_feed,
    )
    parse = PythonOperator(
        task_id="parse_feed_data",
        python_callable=parse_feed_data,
    )
    store = PythonOperator(
        task_id="store_market_prices",
        python_callable=store_market_prices,
    )

    ingest >> parse >> store
