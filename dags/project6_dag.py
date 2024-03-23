from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator

import boto3
import pendulum

import vertica_python

conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2024021942',       
             'password': 'qXSH8TNCEDZ4ZtS',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def fetch_s3_file_group(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='sprint6',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
				Bucket=bucket, 
				Key=key, 
				Filename=f'/data/{key}'
)


bash_command_tmpl = """
head {{ params.files }}
"""


def load_grouplog(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("COPY STV2024021942__STAGING.group_log(message_id,message_ts,message_from,message_to,message,message_group) FROM LOCAL '/data/group_log.csv' DELIMITER',';")
        res = cur.fetchall()
    return res


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_project_dag_00():
    bucket_files = ('group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch01_{key}',
            python_callable=fetch_s3_file_group,
            op_kwargs={'bucket': 'sprint_6', 'key': key},
        ) for  key in bucket_files
    ]
        
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )
    
    load_group_log = [
        PythonOperator(
            task_id = 'load_group_log',
            python_callable= load_grouplog,
            # conn_id = conn_info,
            # file = f"/data/{task}",
            op_kwargs={'bucket': 'sprint6', 'key': task}
            # dag = dag
    ) for task in [f'/data/group_log.csv']
    ]
    
    
    fetch_tasks >> print_10_lines_of_each >> load_group_log

_ = sprint6_project_dag_00()

    
    


# def download_csv_from_s3(bucket_name, key, local_file_path):
#     s3 = boto3.client('s3')
    
#     try:
#         s3.download_file(bucket_name, key, local_file_path)
#         print(f"CSV file downloaded from s3://{bucket_name}/{key} to {local_file_path}")
        
#         # Чтение CSV файла с помощью pandas
#         df = pd.read_csv(local_file_path)
#         print(df.head())  # Вывод первых строк CSV файла
        
#     except Exception as e:
#         print(f"Error downloading CSV file: {e}")
        
# bucket_name = 'sprint6'
# key = 'https://storage.yandexcloud.net/group_log.csv'
# local_file_path = 'C:/Users/shishmar/OneDrive - Mars Inc/DE-Yandex-project/s6-lessons/data/group_log.csv' 

# download_csv_from_s3(bucket_name, key, local_file_path)
        
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2022, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# dag = DAG(
#     'download_from_s3',
#     default_args=default_args,
#     description='DAG to download file from S3',
#     schedule_interval='@daily',
# )

# download_task = PythonOperator(
#     task_id='download_file_from_s3',
#     python_callable=download_csv_from_s3,
#     dag=dag,
# )

# download_csv_from_s3