import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from pg_connect import ConnectionBuilder
from config_const import ConfigConst
from api_loader import Get_APIDataLoader
from api_loader import Load_APIDataLoader
from final.stg_settings_repository import StgEtlSettingsRepository

log = logging.getLogger(__name__)

with DAG(
    dag_id='api_stg_case_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
) as dag:

    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    settings_repository = StgEtlSettingsRepository(dwh_pg_connect)

    @task(task_id="Get_APIDataLoader")
    def load_countries():
        rest_loader = Get_APIDataLoader(origin_pg_connect, dwh_pg_connect)
        rest_loader.load_countries()

    @task(task_id="Load_APIDataLoader")
    def load_deliveries():
        event_loader = Load_APIDataLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_deliveries()

    # @task(task_id="users_load")
    # def load_users():
    #     user_loader = UserLoader(origin_pg_connect, dwh_pg_connect)
    #     user_loader.load_users()

    ranks_dict = load_countries()
    events = load_deliveries()

    ranks_dict  # type: ignore
    events  # type: ignore
