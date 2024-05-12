# Import Libraries
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import boto3

# Get environment variables
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_url = os.getenv('MINIO_URL')

# Function to list buckets
def list_buckets():
    s3 = boto3.client('s3', aws_access_key_id=minio_access_key, aws_secret_access_key=minio_secret_key, endpoint_url=minio_url)
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f'Bucket Name: {bucket["Name"]}')

# DAG Definition
with DAG(dag_id='Minio_Demo_List_Buckets',
        start_date=datetime(2022, 8, 9),  
        schedule_interval=None,
        catchup=False,
        tags=['minio-list-buckets'],
    ) as dag:

    # Create a start task
    start_task = DummyOperator(task_id='start_task')

    # Create a task to list the buckets
    list_buckets_task = PythonOperator(
        task_id='list_buckets',
        provide_context=True,
        python_callable=list_buckets
    )

    # Create an end task
    end_task = DummyOperator(task_id='end_task')

    # Define the order of the tasks
    start_task >> list_buckets_task >> end_task