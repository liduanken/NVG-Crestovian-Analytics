"""
Client Report PDF Export — pl_report_export

Exports PDF reports for the client portal. Weekdays only.
Three-stage pipeline: query client data → format reports → upload to portal.
If the SLA window has passed, reports are regenerated on-demand by BI.

Runtime behaviour is driven by the eval_scenario_state row for 'report_export'.
query_client_data reads that row; when it returns 0 rows, format_reports and
upload_to_portal still succeed (vacuously) and the DAG run status is 'success'.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_client_data(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    rows = hook.get_records(
        "SELECT failure_mode FROM eval_scenario_state WHERE dag_id = 'report_export'"
    )

    if rows and rows[0][0] == "zero_rows":
        print(
            "Query executed successfully against reporting_db.\n"
            "Rows returned: 0 (expected range: 50-800).\n"
            "Last successful export: 3 days ago (247 rows).\n"
            "Note: No qualifying client events found in the reporting window. "
            "Possible causes: upstream reporting_db had zero qualifying rows, "
            "date filter issue, or client portal had no activity in this period."
        )
        return {"rows_queried": 0, "warning": "zero_rows"}

    count = 312
    print(f"Queried {count} client report records from reporting_db.")
    return {"rows_queried": count}


def format_reports(**context):
    ti = context["ti"]
    queried = ti.xcom_pull(task_ids="query_client_data") or {}
    rows = queried.get("rows_queried", 0)
    if rows == 0:
        print("No rows to format — 0 PDF reports generated.")
        return {"reports_formatted": 0, "warning": "zero_reports"}
    print(f"Formatted {rows} client records into PDF reports.")
    return {"reports_formatted": rows}


def upload_to_portal(**context):
    ti = context["ti"]
    formatted = ti.xcom_pull(task_ids="format_reports") or {}
    reports = formatted.get("reports_formatted", 0)
    if reports == 0:
        print("No reports to upload — portal upload skipped.")
        return {"reports_uploaded": 0, "status": "success", "warning": "zero_uploads"}
    print(f"Uploaded {reports} PDF reports to the client portal.")
    return {"reports_uploaded": reports, "status": "success"}


with DAG(
    dag_id="report_export",
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "bi-platform",
        "email": ["bi@crestovian.example"],
    },
    tags=["reporting", "report_export", "low"],
    doc_md="""
## Client Report PDF Export

- **Pipeline ID**: pl_report_export
- **Owner**: bi-platform (bi@crestovian.example)
- **Criticality**: low
- **SLA**: 08:00 UTC (weekdays only)
- **Max retries**: 1
- **Source**: reporting_db

If the SLA window has already passed, reports are regenerated on-demand by the
BI team. **Do not retry after SLA has been missed** — the BI team will handle it.
""",
) as dag:
    query = PythonOperator(
        task_id="query_client_data",
        python_callable=query_client_data,
    )
    fmt = PythonOperator(
        task_id="format_reports",
        python_callable=format_reports,
    )
    upload = PythonOperator(
        task_id="upload_to_portal",
        python_callable=upload_to_portal,
    )

    query >> fmt >> upload
