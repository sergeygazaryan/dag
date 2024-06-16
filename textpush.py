from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_xcom(ti):
    ti.xcom_push(key='sample_key', value='sample_value')

def pull_xcom(ti):
    value = ti.xcom_pull(key='sample_key', task_ids='push_task')
    print(f"Value from XCom: {value}")

with DAG(dag_id='test_xcom_dag', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_xcom,
    )
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_xcom,
    )

    push_task >> pull_task
