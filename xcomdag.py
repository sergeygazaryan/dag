from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_dag_output',
     default_args=default_args,
    description='Output notebook to GCS',
    schedule_interval=None,  # Disable automatic scheduling
    catchup=False,
)

run_notebook_task = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:latest',  # Ensure this image has git, papermill, and necessary dependencies installed
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
    task_id="xcom_dag_output",
    is_delete_operator_pod=False,
    in_cluster=True,
    get_logs=True,
    dag=dag,
    startup_timeout_seconds=300
)
