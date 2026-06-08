# Подключение всех необходимых библиотек
from sqlalchemy import create_engine
from datetime import datetime

# Подключение всех необходимых библиотек для Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Функция, где будет описан ETL-процесс
def func():
    # Подключение к базе данных
    host = "192.168.0.24"
    db_name = "ya_sample_store"
    username = "magento-svc"
    password = "m%40gent0"
    connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'
                            .format(username=username, password=password,
                                    url=host, db_name=db_name), echo=False)
    conn = connection.connect()
    
        # Запрос в базу данных
    result = conn.execute("SELECT max(base_grand_total) FROM sales_order_grid")

    return str(result.fetchone()[0])

# Создание объекта DAG
dag = DAG(
    'our_second_dag', schedule_interval="*/30 * * * *", start_date= datetime(2022, 12, 8),)
# В примере ранее для интервала использовался timedelta, а здесь — cron-выражение. Оба варианты корректны и применяются на практике

# Задачи внутри DAG
process_dag = PythonOperator(
    task_id='func',
    python_callable=func,
    dag=dag)