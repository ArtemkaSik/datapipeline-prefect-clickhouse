# Подключение всех необходимых библиотек
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Создание объекта DAG
# dag — переменная, в которой лежит объект DAG
# schedule_interval — интервал, через который должен запускаться DAG
# start_date — дата начала запуска DAG
dag = DAG('our_first_dag',schedule_interval=timedelta(days=1), start_date=days_ago(1))

# Указание задач внутри DAG
# t№ — переменные, в которых лежит объект Operator
# task_id — имя задачи, которое будет отображаться в интерфейсе
t1 = DummyOperator(task_id='task_1', dag=dag)
t2 = DummyOperator(task_id='task_2',dag=dag)
t3 = DummyOperator(task_id='task_3',dag=dag)

# Настройка зависимостей задач друг от друга
# >> определяет последовательную зависимость задач
# [] позволяет исполнить задачи параллельно
t1 >> [t2, t3]