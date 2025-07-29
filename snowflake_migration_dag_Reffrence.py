from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

def create_migration_dag(migration_config):
    dag = DAG(
        f'snowflake_migration_{migration_config["id"]}',
        default_args={
            'owner': 'snowflake-migrator',
            'depends_on_past': False,
            'start_date': datetime.now(),
            'retries': 3,
            'retry_delay': timedelta(minutes=5)
        },
        schedule_interval=None,  # Triggered manually
        catchup=False
    )
    
    # Task 1: Validate Connections
    validate_connections = PythonOperator(
        task_id='validate_connections',
        python_callable=validate_database_connections,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task 2: Extract Schema
    extract_schema = PythonOperator(
        task_id='extract_schema',
        python_callable=extract_source_schema,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task 3: Convert DDL using SnowConvert
    convert_ddl = PythonOperator(
        task_id='convert_ddl',
        python_callable=convert_ddl_with_snowconvert,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task 4: Setup Snowflake Environment
    setup_snowflake = PythonOperator(
        task_id='setup_snowflake',
        python_callable=setup_snowflake_environment,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task 5: Deploy Schema
    deploy_schema = PythonOperator(
        task_id='deploy_schema',
        python_callable=deploy_schema_to_snowflake,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task 6: Extract and Load Data (Parallel)
    data_migration_tasks = []
    for table in migration_config['tables']:
        task = PythonOperator(
            task_id=f'migrate_table_{table}',
            python_callable=migrate_table_data,
            op_kwargs={'config': migration_config, 'table': table},
            dag=dag
        )
        data_migration_tasks.append(task)
    
    # Task 7: Validate Migration
    validate_migration = PythonOperator(
        task_id='validate_migration',
        python_callable=validate_migration_results,
        op_kwargs={'config': migration_config},
        dag=dag
    )
    
    # Task Dependencies
    validate_connections >> extract_schema >> convert_ddl >> setup_snowflake >> deploy_schema
    deploy_schema >> data_migration_tasks >> validate_migration
    
    return dag