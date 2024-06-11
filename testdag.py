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
    'simple_notebook_execution',
    default_args=default_args,
    description='Simple DAG to run a Jupyter notebook from Git',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

run_notebook_task = KubernetesPodOperator(
    namespace='airflow',
    image='test_worker_image:latest',  # Ensure this image has git, papermill, and necessary dependencies installed
    image_pull_policy='IfNotPresent',
    cmds=["/bin/bash", "-c"],
    arguments=[
        """
        git clone https://github.com/sergeygazaryan/notebook.git /tmp/workspace && \
        papermill /tmp/workspace/test.ipynb /tmp/workspace/test-output.ipynb
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
