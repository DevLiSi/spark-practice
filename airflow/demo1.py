from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 定义默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义DAG
dag = DAG(
    'example_dag_test',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# 定义任务的Python函数
def print_hello():
    print("Hello World")

def print_goodbye():
    print("Goodbye World")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

goodbye_task = PythonOperator(
    task_id='goodbye_task',
    python_callable=print_goodbye,
    dag=dag,
)

hello_task >> goodbye_task