from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_notebook_execution',
    default_args=default_args,
    description='Simple DAG to run a Jupyter notebook from Git',
    schedule_interval='@once',
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
        papermill /tmp/workspace/test.ipynb /tmp/workspace/test-output.ipynb > /tmp/workspace/output.log 2>&1
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