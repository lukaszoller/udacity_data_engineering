from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello, World!'

default_args = {
    'owner': 'bort',
    'start_date': datetime(2024, 2, 10),
    'retries': 1
}

with DAG('hello_world_dag', default_args=default_args, schedule_interval=None) as dag:
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )
