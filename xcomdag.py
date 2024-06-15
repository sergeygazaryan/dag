from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

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
    num_elements = context['ti'].xcom_pull(task_ids='execute-notebook', key='num_elements')
    word_counts = context['ti'].xcom_pull(task_ids='execute-notebook', key='word_counts')
    logging.info(f"Pushed to XCom: num_elements={num_elements}, word_counts={word_counts}")

run_notebook_task = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
    image_pull_policy='IfNotPresent',
    cmds=["/bin/bash", "-c"],
    arguments=[
        """
        set -e
        echo "Cloning the repository..."
        timeout 300 git clone https://github.com/sergeygazaryan/notebook.git /tmp/workspace || { echo "Git clone failed!"; exit 1; }
        echo "Repository cloned. Listing contents..."
        ls -la /tmp/workspace
        echo "Running papermill..."
        papermill /tmp/workspace/xcom_output.ipynb /tmp/workspace/test-output.ipynb > /tmp/workspace/output.log 2>&1 || { echo "Papermill execution failed!"; cat /tmp/workspace/output.log; exit 1; }
        echo "Papermill executed successfully. Listing contents of /tmp/workspace..."
        ls -la /tmp/workspace
        echo "Contents of output.log:"
        cat /tmp/workspace/output.log
        echo "Contents of test-output.ipynb:"
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
