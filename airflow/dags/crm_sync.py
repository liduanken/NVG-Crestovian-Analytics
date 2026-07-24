"""
CRM Customer Sync — pl_crm_sync

Syncs customer records from Salesforce into the data warehouse.
Three-stage pipeline: extract → transform → load.

Runtime behaviour is driven by the eval_scenario_state row for 'crm_sync'.
extract_from_salesforce reads that row; transform_records and load_to_warehouse
run only once extraction succeeds.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_from_salesforce(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    rows = hook.get_records(
        "SELECT failure_mode, retry_count_remaining FROM eval_scenario_state WHERE dag_id = 'crm_sync'"
    )

    if rows and rows[0][0] == "transient" and rows[0][1] > 0:
        hook.run(
            "UPDATE eval_scenario_state SET retry_count_remaining = retry_count_remaining - 1 WHERE dag_id = 'crm_sync'"
        )
        raise Exception(
            "SALESFORCE_TIMEOUT: Upstream API call to salesforce_api timed out after 30s "
            f"(attempt {context['ti'].try_number} of 5). HTTP 504 received from Salesforce. "
            "This is consistent with intermittent high-load behaviour documented in the pipeline catalog."
        )

    print("Extracted 52,341 customer records from Salesforce (contacts + account updates).")
    return {"rows_extracted": 52341, "source": "salesforce_api"}


def transform_records(**context):
    ti = context["ti"]
    extracted = ti.xcom_pull(task_ids="extract_from_salesforce") or {}
    rows = extracted.get("rows_extracted", 0)
    print(f"Transforming {rows} records: deduplication, phone normalisation, email validation.")
    dupes_removed = int(rows * 0.003)
    print(f"  Removed {dupes_removed} duplicate records. {rows - dupes_removed} records ready for load.")
    return {"rows_transformed": rows - dupes_removed}


def load_to_warehouse(**context):
    ti = context["ti"]
    transformed = ti.xcom_pull(task_ids="transform_records") or {}
    rows = transformed.get("rows_transformed", 0)
    print(f"Loading {rows} customer records into warehouse.customers_staging.")
    print("Swapping staging → production table. Notifying downstream: customer_health_scores, sales_dashboard.")
    return {"rows_loaded": rows, "status": "success"}


with DAG(
    dag_id="crm_sync",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
        "owner": "crm-engineering",
        "email": ["crm-eng@crestovian.example"],
    },
    tags=["crm", "batch_etl", "high"],
    doc_md="""
## CRM Customer Sync

- **Pipeline ID**: pl_crm_sync
- **Owner**: crm-engineering (crm-eng@crestovian.example)
- **Criticality**: high
- **SLA**: 09:00 UTC
- **Max retries**: 5
- **Downstream**: customer_health_scores, sales_dashboard
- **Source**: salesforce_api

Salesforce API occasionally returns intermittent 504s during high-load periods.
Pipeline has historically self-resolved within 4 retries. CRM engineering team
is aware and working on an upstream fix.
""",
) as dag:
    extract = PythonOperator(
        task_id="extract_from_salesforce",
        python_callable=extract_from_salesforce,
    )
    transform = PythonOperator(
        task_id="transform_records",
        python_callable=transform_records,
    )
    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    extract >> transform >> load
