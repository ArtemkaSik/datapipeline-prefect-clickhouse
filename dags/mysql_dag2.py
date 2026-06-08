# Подключаем библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Подключаем вспомогательные библиотеки
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd
import os

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 6, 30),
    'end_date': datetime(2022, 7, 26),
    'max_active_runs': 1, # За раз отработает только один DAG
}

dag = DAG(
    'user_payments',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Допишите недостающие функции здесь
def download_object_from_mysql(**context):
    engine = create_engine('mysql://magento-svc:m%40gent0@192.168.0.24/ya_sample_store') # адрес mysql yoga shop
    data = pd.read_sql(f"SELECT * FROM ya_sample_store.user_payments WHERE date = '{context['ds']}'", engine)
    data.to_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv', index=False)

# Адрес MySQL yoga shop

def load_object_from_mysql_to_clickhouse(**context):
    df = pd.read_csv(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

    client  = clickhouse_connect.get_client(
        host="rc1a-4fr6c37coh09gmao.mdb.yandexcloud.net", 
        port=8443, 
        user="admin", 
        password="Artem123456!",
        verify=False
        )
    
    client.insert_df('tmp.user_payments', df)

def etl_inside_clickhouse():

    client  = clickhouse_connect.get_client(
        host="rc1a-4fr6c37coh09gmao.mdb.yandexcloud.net", 
        port=8443, 
        user="admin", 
        password="Artem123456!",
        verify=False
        )

    client.command("""
        INSERT INTO raw.user_payments
        SELECT 
        toDateTime(date) as date,
        toDateTime(timestamp),
        user_client_id,
        item,
        price,
        quantity,
        amount,
        discount,
        order_id,
        status, 
        now() as insert_time,
        cityHash64(*) as hash
        FROM tmp.user_payments
        """)
        
    client.command("TRUNCATE TABLE tmp.user_payments ") # Очистка временной таблицы

def remove_tmp_file(**context):
  os.remove(f'/tmp/{context["ds"]}-user_payments_downloaded_file.csv')

download_object_from_mysql = PythonOperator(
    task_id="download_object_from_mysql",
    python_callable=download_object_from_mysql,
    dag=dag,
)

load_object_from_mysql_to_clickhouse= PythonOperator(
    task_id="load_object_from_mysql_to_clickhouse",
    python_callable=load_object_from_mysql_to_clickhouse,
    dag=dag,
)

etl_inside_clickhouse= PythonOperator(
    task_id="etl_inside_clickhouse",
    python_callable=etl_inside_clickhouse,
    dag=dag,
)

remove_tmp_file= PythonOperator(
    task_id="remove_tmp_file",
    python_callable=remove_tmp_file,
    dag=dag,
)

download_object_from_mysql >> load_object_from_mysql_to_clickhouse >> etl_inside_clickhouse >> remove_tmp_file