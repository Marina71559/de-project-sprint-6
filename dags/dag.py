# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.decorators import dag
# from airflow.hooks.base import BaseHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.task_group import TaskGroup
# from datetime import datetime, timedelta

# from airflow.operators.dummy_operator import DummyOperator

# import boto3
# import pendulum

# import vertica_python

# conn_info = {'host': 'vertica.tgcloudenv.ru', 
#              'port': '5433',
#              'user': 'stv2024021942',       
#              'password': 'qXSH8TNCEDZ4ZtS',
#              'database': 'dwh',
#              # Вначале он нам понадобится, а дальше — решите позже сами
#             'autocommit': True
# }

# AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
# AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

# def fetch_s3_file_group(bucket: str, key: str) -> str:
#     session = boto3.session.Session()
#     s3_client = session.client(
#         service_name='s3',
#         endpoint_url='https://storage.yandexcloud.net',
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#     )
#     s3_client.download_file(
# 				Bucket=bucket, 
# 				Key=key, 
# 				Filename=f'/data/{key}'
# )


# bash_command_tmpl = """
# head {{ params.files }}
# """
# def load_group_log(conn_info=conn_info):
#     # И рекомендуем использовать соединение вот так
#     with vertica_python.connect(**conn_info) as conn:
#         cur = conn.cursor()
#         cur.execute("COPY STV2024021942__STAGING.group_log(message_id,message_ts,message_from,message_to,message,message_group) FROM LOCAL '/data/group_log.csv' DELIMITER',';")
#         res = cur.fetchall()
#     return res


# @dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
# def sprint6_project_dag_01():
#     bucket_files = ('group_log.csv')
#     fetch_tasks = [
#         PythonOperator(
#             task_id='task_01',
#             python_callable=fetch_s3_file_group,
#             op_kwargs={'bucket': 'sprint_6', 'key': key},
#         ) for key in bucket_files
#     ]
        
#     # print_10_lines_of_each = BashOperator(
#     #     task_id='print_10_lines_of_each',
#     #     bash_command=bash_command_tmpl,
#     #     params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
#     # )
    
#     load_group_log = [
#         PythonOperator(
#             task_id = 'load_group_log',
#             python_callable= load_group_log,
#             # conn_id = conn_info,
#             # file = f"/data/{task}",
#             op_kwargs={'bucket': 'sprint6', 'key': task}
#             # dag = dag
#     ) for task in [f'/data/group_log.csv']
#     ]
    
    
#     fetch_tasks  >> load_group_log

# _ = sprint6_project_dag_01()

    
    



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

def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
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
def load_dialog(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("COPY STV2024021942__STAGING.dialogs(message_id,message_ts,message_from,message_to,message,message_group) FROM LOCAL '/data/dialogs.csv' DELIMITER',';")
        res = cur.fetchall()
    return res

def load_group(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("COPY STV2024021942__STAGING.groups(id, admin_id, group_name, registration_dt, is_private) FROM LOCAL '/data/groups.csv' DELIMITER',';")
        res = cur.fetchall()
    return res

def load_user(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("COPY STV2024021942__STAGING.users(id,chat_name, registration_dt, country, age) FROM LOCAL '/data/users.csv' DELIMITER',';")
        res = cur.fetchall()
    return res


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_project_dag_get_data_00():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]
        
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )
    
    load_dialogs = [
        PythonOperator(
            task_id = 'load_dialogs',
            python_callable= load_dialog,
            # conn_id = conn_info,
            # file = f"/data/{task}",
            op_kwargs={'bucket': 'sprint6', 'key': task}
            # dag = dag
    ) for task in [f'/data/dialogs.csv']
    ]
    
    load_groups = [
        PythonOperator(
            task_id = 'load_groups',
            python_callable= load_group,
            # conn_id = conn_info,
            # file = f"/data/{task}",
            op_kwargs={'bucket': 'sprint6', 'key': task}
            # dag = dag
    ) for task in [f'/data/groups.csv']
    ]
    
    load_users = [
        PythonOperator(
            task_id = 'load_users',
            python_callable= load_user,
            # conn_id = conn_info,
            # file = f"/data/{task}",
            op_kwargs={'bucket': 'sprint6', 'key': task}
            # dag = dag
    ) for task in [f'/data/users.csv']
    ]
    group_load_tasks = load_dialogs + load_groups + load_users
    fetch_tasks >> print_10_lines_of_each >> group_load_tasks

_ = sprint6_project_dag_get_data_00()

# default_args = {
#     'owner':'airflow',
#     'retries':1,
#     'retry_delay': timedelta (seconds = 60)
# }
# def upload_stg():
#     load_dialogs = [
#         PythonOperator(
#             task_id = 'load_dialogs',
#             python_callable= load_dialog,
#             # conn_id = conn_info,
#             # file = f"/data/{task}",
#             op_kwargs={'bucket': 'sprint6', 'key': task}
#             # dag = dag
#     ) for task in [f'/data/dialogs.csv']
#     ]
#     load_dialogs
# _=upload_stg()
 
# dag = DAG('dwh_project_dag_00',
#         start_date=datetime(2024, 2, 22),
#         catchup=True,
#         schedule_interval='@daily',
#         max_active_runs=1,
#         default_args=default_args)

# with TaskGroup(group_id = 'upload_ver_to_stg', dag=dag) as upload_stg:
    
#     load_dialogs = [
#         PythonOperator(
#             task_id = 'load_dialogs',
#             python_callable=load_dialogs,
#             # conn_id = conn_info,
#             # file = f"/data/{task}",
#             op_kwargs={'bucket': 'sprint6', 'key': task},
#             dag = dag
#     ) for task in [f'/data/dialogs.csv']
#     ]

#     upload_stg
    
    
    


