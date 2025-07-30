from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def sample_task():
    print("This is a manually triggered DAG run.")


# Define the DAG
with DAG(
    dag_id="manual_trigger_only_dag",
    description="A DAG that runs only when manually triggered",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # No automatic scheduling
    catchup=False,           # Don't backfill old runs
    tags=["manual", "example"],
) as dag:

    run_this = PythonOperator(
        task_id="print_message",
        python_callable=sample_task,
    )
