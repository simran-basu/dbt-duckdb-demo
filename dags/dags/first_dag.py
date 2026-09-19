from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def extract():
    print("Pretending to extract raw data...")

def transform():
    print("Pretending to transform data (this is where dbt/Spark would run)...")

with DAG(
    dag_id="first_dag_pipeline_demo",
    description="Minimal DAG demonstrating task dependencies",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["learning"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = BashOperator(
        task_id="load",
        bash_command='echo "Pretending to load data into the warehouse..."',
    )

    # Define dependencies: extract must run before transform, transform before load
    extract_task >> transform_task >> load_task