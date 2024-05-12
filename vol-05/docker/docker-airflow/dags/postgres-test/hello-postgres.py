# Import Python dependencies needed for the workflow
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Define default and DAG-specific arguments
args = {
    'owner': 'airflow',    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
with DAG(
    dag_id='Postgres_Demo',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['read/write', 'postgres'],
) as dag:
    start_task = DummyOperator(task_id='start_task')

    # Create the Tasks
    create_table = PostgresOperator(
        sql = "./create_table.sql",
        task_id = "create_table_task",
        postgres_conn_id = "oasis_con_postgres",
        dag = dag
    )

    insert_data = PostgresOperator(
        sql = "./insert_data.sql",
        task_id = "insert_data_task",
        postgres_conn_id = "oasis_con_postgres",
        dag = dag
    )

    end_task = DummyOperator(task_id='end_task')                                  
    start_task >> create_table >> insert_data >> end_task