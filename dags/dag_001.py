from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def upload_file_to_s3(bucket_name, file_name, key):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(file_name, key, bucket_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    's3_upload_dag',
    default_args=default_args,
    description='s3_upload_dag',
    schedule_interval=None,
)

upload_task = PythonOperator(
    task_id='upload_file_to_s3',
    python_callable=upload_file_to_s3,
    op_args=['sprint6', 'local/path/to/group_log.csv', 'data/group_log.csv'],
    dag=dag,
)

upload_task
