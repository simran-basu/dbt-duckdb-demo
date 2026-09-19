from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)


def task_failure_alert(context):
    """Stub alert — in production this would hit Slack/email/PagerDuty."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    log.error(
        "ALERT: Task '%s' in DAG '%s' failed on run '%s'. "
        "Exception: %s",
        task_instance.task_id,
        task_instance.dag_id,
        dag_run.run_id,
        context.get("exception"),
    )
    # In a real setup: send_slack_message(...), send_email(...), etc.


def dag_failure_alert(context):
    """Stub alert fired if the whole DAG run fails (not just one task)."""
    dag_run = context.get("dag_run")
    log.error("ALERT: DAG run '%s' failed entirely.", dag_run.run_id)


default_args = {
    "owner": "simran",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="spark_transform_trigger",
    description="Triggers the Week 2 PySpark job, then runs dbt on top of its output",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["spark", "dbt", "pipeline"],
    default_args=default_args,
    on_failure_callback=dag_failure_alert,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_sparksql_writeout",
        bash_command="cd /opt/airflow/spark_pipeline && python sparksql_writeout.py",
    )

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "export DBT_PROFILES_DIR=/opt/airflow/dbt_project && "
            "dbt run --select int_customer_alerts_from_spark fct_customer_targets_from_spark && "
            "dbt test --select int_customer_alerts_from_spark fct_customer_targets_from_spark"
        ),
    )

    run_spark_job >> run_dbt_build