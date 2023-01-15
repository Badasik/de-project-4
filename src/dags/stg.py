from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
import pymongo
from datetime import datetime, date, timedelta
from airflow import DAG
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom
import logging
import logging
from examples.stg.Check import check_and_create
from examples.stg import stg_load

log = logging.getLogger(__name__)




#Текущая дата для выгрузки
today = date.today()
current_date = str(date.today())
current_date_string = current_date + ' 00:00:00'

#Вчерашняя дата для выгрузки
yesterday_date = date.today() - timedelta(1)
yesterday_date_string = str(yesterday_date) + ' 00:00:00'



offset = 1
limit = 10000
DWH = "dbname='de' port=5432 user=jovyan host=localhost password=jovyan"

ALL_STG_TABLES = ['bonussystem_ranks', 'bonussystem_users', 'bonussystem_events', 'ordersystem_orders', 'ordersystem_restaurants', 'ordersystem_users', 'srv_etl_settings', 'deliveries', 'couriers']
host='https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
#Заголовки для API
headers={
    "X-Nickname": "badasik",
    "X-Cohort": "7",
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }

class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 host: List[str],  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)
    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]

MONGO_DB_CERTIFICATE_PATH = '/opt/airflow/certificates/PracticumSp5MongoDb.crt'
MONGO_DB_DATABASE_NAME = "db-mongo"
MONGO_DB_HOST = "rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018"
MONGO_DB_PASSWORD = "student1"
MONGO_DB_REPLICA_SET = "rs01"
MONGO_DB_USER = "student"

url = f'mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}/?replicaSet={MONGO_DB_REPLICA_SET}&authSource={MONGO_DB_DATABASE_NAME}'
mongo_client = pymongo.MongoClient(url, tlsCAFile=MONGO_DB_CERTIFICATE_PATH)[MONGO_DB_DATABASE_NAME]

connect_to_scr = psycopg2.connect("host=rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net port=6432 sslmode=require dbname=de-public user=student password=student1")    
connect_to_db = psycopg2.connect("host=localhost port=5432 dbname=de user=jovyan password=jovyan")

def check_database(connect_to_db=connect_to_db):
    check_and_create(connect_to_db, 'stg', ALL_STG_TABLES)

def download_from_postgresql(connect_to_scr, connect_to_db, out_schema, out_table, schema, table, id_column):
    stg_load.download_postgresdata_to_staging(connect_to_scr, connect_to_db, out_schema=out_schema, out_table=out_table, schema=schema, table=table, id_column=id_column)

def download_from_mongo(mongo_client, connect_to_db, collection_name, schema, table_name):
    stg_load.download_mongo_to_staging(mongo_client=mongo_client, connect_to_db=connect_to_db, collection_name=collection_name, schema=schema, table_name=table_name)


#Новые функции для заполнения couriers и deliveries
def download_couriers(file_api, offset, limit, DWH, current_date_string, yesterday_date_string, headers):
    stg_load.load_couriers(file_api='couriers', offset=offset, limit=limit, DWH=DWH, current_date_string=current_date_string, yesterday_date_string=yesterday_date_string, headers=headers)

def download_deliveries(file_api, offset, limit, DWH, current_date_string, yesterday_date_string, headers):
    stg_load.load_deliveries(file_api='deliveries', offset=offset, limit=limit, DWH=DWH, current_date_string=current_date_string, yesterday_date_string=yesterday_date_string, headers=headers)

dag = DAG(
    schedule_interval='* 1 * * *',
    dag_id='download_data_to_stage',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['stage', 'mongo', 'postgresql'],
    is_paused_upon_creation=False
)


check = PythonOperator(task_id='check_database',
                                 python_callable=check_database,
                                 dag=dag)
rank_load = PythonOperator(task_id='rank_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect_to_db':connect_to_db, 'connect_to_scr': connect_to_scr, 'out_schema': 'public', 'out_table': 'ranks', 'schema': 'stg', 'table': 'bonussystem_ranks', 'id_column': 'True'},
                                 dag=dag)
users_load = PythonOperator(task_id='users_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect_to_db':connect_to_db, 'connect_to_scr': connect_to_scr, 'out_schema': 'public', 'out_table': 'users', 'schema': 'stg', 'table': 'bonussystem_users', 'id_column': 'False'},
                                 dag=dag)
outbox_load = PythonOperator(task_id='outbox_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect_to_db':connect_to_db, 'connect_to_scr': connect_to_scr, 'out_schema': 'public', 'out_table': 'outbox', 'schema': 'stg', 'table': 'bonussystem_events', 'id_column': 'True'},
                                 dag=dag)
orders_load = PythonOperator(task_id='orders',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'mongo_client':'mongo_client','connect_to_db':connect_to_db, 'collection_name': 'orders', 'schema': 'stg', 'table_name': 'ordersystem_orders'},
                                 dag=dag)
restaurants_load = PythonOperator(task_id='restaurants',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'mongo_client':'mongo_client','connect_to_db':connect_to_db, 'collection_name': 'restaurants', 'schema': 'stg', 'table_name': 'ordersystem_restaurants'},
                                 dag=dag)
ordersystem_users = PythonOperator(task_id='users',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'mongo_client':'mongo_client','connect_to_db':connect_to_db, 'collection_name': 'users', 'schema': 'stg', 'table_name': 'ordersystem_users'},
                                 dag=dag)                             
couriers_load = PythonOperator(task_id='couriers_load',
                                 python_callable=download_couriers,
                                 op_kwargs={'file_api':'couriers', 'offset': offset, 'limit':limit, 'DWH':DWH, "current_date_string":current_date_string, 'yesterday_date_string':yesterday_date_string, 'headers':headers},
                                 dag=dag)
delivery_load = PythonOperator(task_id='deliveries_load',
                                 python_callable=download_deliveries,
                                 op_kwargs={'file_api':'deliveries', 'offset': offset, 'limit':limit, 'DWH':DWH, "current_date_string":current_date_string, 'yesterday_date_string':yesterday_date_string, 'headers':headers},
                                 dag=dag)

check >> [rank_load, users_load, outbox_load, orders_load, ordersystem_users, restaurants_load, couriers_load, delivery_load]