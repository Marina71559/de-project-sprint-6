import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime   
from api_loader import Get_APIDataLoader
from api_loader import Load_APIDataLoader

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id = 'api_data_loading_dag_00',
    default_args=default_args,
    description='DAG for loading data from API',
    schedule_interval='0/15 * * * *',
    catchup=False,
) as dag:

    get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=Get_APIDataLoader,
        dag=dag,
    )

    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=Load_APIDataLoader,
        dag=dag,
    )

    get_data_task >> load_data_task