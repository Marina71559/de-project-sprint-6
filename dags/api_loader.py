import requests
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key') #
base_url = http_conn_id.host
db_connection_string = http_conn_id


class Get_APIDataLoader:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers

    def load_data(self):
        response = requests.get(self.url, headers=self.headers)
        if response.status_code == 200:
            with open('restaurants.json', 'wb') as file:
                file.write(response.content)
                print('Файл успешно скачан')
        else:
            print('Ошибка при загрузке файла:', response.status_code)
            
            
class Load_APIDataLoader:
    def __init__(self, url, headers, db_connection_string):
        self.url = url
        self.headers = headers
        self.db_connection_string = db_connection_string

    def load_data(self):
        response = requests.get(self.url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()

            conn = psycopg2.connect(self.db_connection_string)
            cur = conn.cursor()

            for item in data:
                cur.execute("INSERT INTO restaurants (id, _id, name) VALUES (%s, %s, %s)", (item['id'], item['_id'], item['name']))

            conn.commit()
            conn.close()

            print('Данные успешно загружены в таблицу restaurants')
        else:
            print('Ошибка при загрузке данных:', response.status_code)

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field=id'
headers = {
    'X-Nickname': 'marina71559',
    'X-Cohort': '21',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

get_data_loader = Get_APIDataLoader(url, headers)
load_data_loader = Load_APIDataLoader(url, headers, db_connection_string)

# def load_data():
#     data_loader.load_data()