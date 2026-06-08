
# Подключаем библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Подключаем вспомогательные библиотеки
import clickhouse_connect
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os

# Указываем аргументы
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 6, 30),
    'end_date': datetime(2022, 7, 26),
    'max_active_runs': 1, # За раз отработает только один DAG
}

# DAG будет запускаться раз в день
dag = DAG(
    'site_visits',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Скачиваем данные из Object Storage в файл
def download_object_from_s3(**context):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id="YCAJEEFIeGL1DbqJBU5CPWOVO",
        aws_secret_access_key='YCPuoeHPWqNNxKNktVen3ZVpmQXilHwh7Tu2_RDx'

    )

    s3.download_file(Bucket='yc-metrics-theme',
                         Key=f'{context["ds"]}-site-visits.csv', Filename=f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv')

# Загружаем данные из файла в ClickHouse в слой TMP
def load_object_from_s3_to_clickhouse(**context):
    df = pd.read_csv(f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv')

    client  = clickhouse_connect.get_client(
        host="rc1a-4fr6c37coh09gmao.mdb.yandexcloud.net", 
        port=8443, 
        user="admin", 
        password="Artem123456!",
        verify=False
        )

    client.insert_df('tmp.site_visits', df)

# Перекладываем данные в слой RAW
def etl_inside_clickhouse(**context):

    client  = clickhouse_connect.get_client(
        host="rc1a-4fr6c37coh09gmao.mdb.yandexcloud.net", 
        port=8443, 
        user="admin", 
        password="Artem123456!",
        verify=False
        )
        
    client.command(f"""
        INSERT INTO raw.site_visits 
        SELECT 
            toDateTime(date) as date,
            toDateTime(timestamp) as timestamp,
            user_client_id,
            action_type,
            placement_type,
            placement_id,
            user_visit_url, 
            now() as insert_time,
            cityHash64(*) as hash
        FROM tmp.site_visits 
        WHERE date = '{context["ds"]}'
        """)

def remove_tmp_file(**context):
  os.remove(f'/tmp/{context["ds"]}-site_visits_downloaded_file.csv')

# Указываем операторы для запуска функций
download_from_s3 = PythonOperator(
    task_id="download_from_s3",
    python_callable=download_object_from_s3,
    dag=dag,
)

load_object_from_s3_to_clickhouse = PythonOperator(
    task_id="load_object_from_s3_to_clickhouse",
    python_callable=load_object_from_s3_to_clickhouse,
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

# Настраиваем зависимости
download_from_s3 >> load_object_from_s3_to_clickhouse >> etl_inside_clickhouse >> remove_tmp_file