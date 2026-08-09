from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

log = logging.getLogger(__name__)


def task_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    log.error(
        "ALERT: Task '%s' in DAG '%s' failed on run '%s'. Exception: %s",
        task_instance.task_id, task_instance.dag_id, dag_run.run_id,
        context.get("exception"),
    )


def dag_failure_alert(context):
    dag_run = context.get("dag_run")
    log.error("ALERT: DAG run '%s' failed entirely.", dag_run.run_id)


def validate_raw_ingestion():
    """Confirms raw source files exist before the pipeline proceeds."""
    required_files = [
        "/opt/airflow/seeds/customers.csv",
        "/opt/airflow/seeds/orders.csv",
    ]
    missing = [f for f in required_files if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(f"Missing raw source files: {missing}")
    log.info("Raw ingestion check passed — all source files present.")


default_args = {
    "owner": "simran",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="full_customer_alerts_pipeline",
    description="End-to-end: raw ingestion -> Spark transform -> Parquet -> dbt run -> dbt test",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["spark", "dbt", "pipeline", "full"],
    default_args=default_args,
    on_failure_callback=dag_failure_alert,
) as dag:

    validate_ingestion = PythonOperator(
        task_id="validate_raw_ingestion",
        python_callable=validate_raw_ingestion,
    )

    run_spark_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command="cd /opt/airflow/spark_pipeline && python sparksql_writeout.py",
    )

    run_dbt_run = BashOperator(
        task_id="run_dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "export DBT_PROFILES_DIR=/opt/airflow/dbt_project && "
            "dbt run --select int_customer_alerts_from_spark fct_customer_targets_from_spark"
        ),
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "export DBT_PROFILES_DIR=/opt/airflow/dbt_project && "
            "dbt test --select int_customer_alerts_from_spark fct_customer_targets_from_spark"
        ),
    )

    # Full chain: validate -> transform -> load (dbt run) -> test
    validate_ingestion >> run_spark_transform >> run_dbt_run >> run_dbt_test