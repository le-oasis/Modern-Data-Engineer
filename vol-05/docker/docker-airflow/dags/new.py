# Import Python dependencies needed for the workflow
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3

# Parameters & Arguments
spark_app_name="Spark_ETL"

# Arguments
args = {
    'owner': 'airflow',    
    'retry_delay': timedelta(minutes=5),
}

# Function to list items in the bronze bucket
def list_items():
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_url = os.getenv('MINIO_URL')

    s3 = boto3.client('s3', aws_access_key_id=minio_access_key, aws_secret_access_key=minio_secret_key, endpoint_url=minio_url)
    for obj in s3.list_objects(Bucket='bronze')['Contents']:
        print(obj['Key'])

# DAG Definition
with DAG(
    dag_id='Spark_ETL',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['ETL', 'spark'],
) as dag:
    start_task = DummyOperator(task_id='start_task')

    # Sequence of Tasks
    Spark_ETL = SparkSubmitOperator(
        task_id='Spark_ETL',
        application='/usr/local/spark/app/spark-test/new.py',  # Replace with the path to your Spark application
        conn_id= 'spark_connect', 
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name=spark_app_name,
        packages='org.postgresql:postgresql:42.4.0',
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )                                             

    list_items_task = PythonOperator(
        task_id='list_items',
        python_callable=list_items,
        dag=dag,
    )

    end_task = DummyOperator(task_id='end_task')                                  
    start_task >> Spark_ETL >> list_items_task >> end_task