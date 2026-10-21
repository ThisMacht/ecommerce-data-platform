"""Schedule the existing receipt-gated local runner; no transformations live here."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="olist_core_full_snapshot",
    description="Local full snapshots through the existing receipt/dbt completion gate",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["olist", "local", "full-snapshot"],
) as dag:
    BashOperator(
        task_id="run_core_pipeline",
        bash_command="exec python3 scripts/make_dataset.py",
        cwd=str(Path(__file__).resolve().parents[2]),
        env={
            "DATASET_MODE": "full",
            "OLIST_AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
            "OLIST_AIRFLOW_RUN_ID": "{{ run_id }}",
        },
        append_env=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(hours=1),
        skip_on_exit_code=None,
        do_xcom_push=False,
    )
