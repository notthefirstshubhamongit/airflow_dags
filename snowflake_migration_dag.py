from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='snowflake_migration_dag',   # <<==== This DAG ID should match the one in FastAPI
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['snowflake', 'migration']
) as dag:

    def connect_and_query_snowflake(**kwargs):
        # Read from dag_run.conf
        conf = kwargs['dag_run'].conf
        account = conf.get("snowflake_account")
        user = conf.get("snowflake_username")
        password = conf.get("snowflake_password")
        warehouse = conf.get("snowflake_warehouse")
        database = conf.get("snowflake_database")
        schema = conf.get("snowflake_schema")

        if not all([account, user, password, warehouse, database, schema]):
            raise ValueError("Missing one or more Snowflake connection parameters in DAG run configuration.")

        # Establish connection
        logging.info(f"Connecting to Snowflake account: {account}")
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        # Run a simple query
        cursor = conn.cursor()
        try:
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            logging.info("List of tables:")
            for row in tables:
                logging.info(row)
        finally:
            cursor.close()
            conn.close()

    # Define task
    run_query = PythonOperator(
        task_id='query_snowflake',
        python_callable=connect_and_query_snowflake,
        provide_context=True
    )

    run_query
