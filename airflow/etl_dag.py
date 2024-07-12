import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    'etl_spark_dag',
    default_args=default_args,
    description='extract transformation load DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 1),
    catchup=False,
)


def extract():
    os.system('python3 /Users/sili/Bigdata/spark-practice/jobs_local/extract_data_from_web_to_local.py')


def load():
    os.system('python3 /Users/sili/Bigdata/spark-practice/jobs_local/load_data_from_local.py')


extract = PythonOperator(
    task_id='extract_from_web',
    python_callable=extract,
    dag=dag,
)

transform = SparkSubmitOperator(
    task_id='transform_from_local',
    application='/Users/sili/Bigdata/spark-practice/jobs_local/transform_data_from_local.py',
    conn_id='spark-local-test',
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_db',
    python_callable=load,
    dag=dag,
)

extract >> transform >> load
