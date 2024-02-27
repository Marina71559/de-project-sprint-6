import psycopg
import psycopg2
from airflow.hooks.base import BaseHook

class PgConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    def client(self):
        return psycopg.connect(self.url())



# class PgConnect:
#     def __init__(self, host, port, dbname, user, password):
#         self.host = host
#         self.port = int(port) if port is not None else None
#         self.dbname = dbname
#         self.user = user
#         self.password = password

#     def pg_conn(self):
#         conn = psycopg2.connect(
#             host=self.host,
#             port=self.port,
#             dbname=self.dbname,
#             user=self.user,
#             password=self.password
#         )
#         return conn

class ConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg    

host = 'localhost'
port = '5432'  # Измените на реальный порт базы данных, если он не равен None
dbname = 'de'
user = 'jovyan'
password = 'jovyan'

pg = PgConnect(host, port, dbname, user, password)
conn = pg.pg_conn()
print("Соединение с базой данных успешно установлено")