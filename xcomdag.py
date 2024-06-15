from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_dag_output',
    default_args=default_args,
    description='Output notebook to GCS',
    schedule_interval=None,  # Disable automatic scheduling
    catchup=False,
)

def push_xcom(**context):
    num_elements = context['ti'].xcom_pull(task_ids='run_notebook_task', key='num_elements')
    word_counts = context['ti'].xcom_pull(task_ids='run_notebook_task', key='word_counts')
    # You can push these values to another task or external system if needed
    # For example, save to a database or push to an API
    # In this example, we will simply log them
    logging.info(f"Pushed to XCom: num_elements={num_elements}, word_counts={word_counts}")

run_notebook_task = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
    image_pull_policy='IfNotPresent',
    cmds=["/bin/bash", "-c"],
    arguments=[
        """
        set -e
        git clone https://github.com/sergeygazaryan/notebook.git /tmp/workspace
        papermill /tmp/workspace/xcom_output.ipynb /tmp/workspace/test-output.ipynb > /tmp/workspace/output.log 2>&1
        cat /tmp/workspace/output.log
        cat /tmp/workspace/test-output.ipynb
        """
    ],
    name="notebook-execution",
    task_id="execute-notebook",
    is_delete_operator_pod=False,
    in_cluster=True,
    get_logs=True,
    dag=dag,
    startup_timeout_seconds=300
)

xcom_push_task = PythonOperator(
    task_id='push_xcom_task',
    python_callable=push_xcom,
    provide_context=True,
    dag=dag,
)

run_notebook_task >> xcom_push_task
