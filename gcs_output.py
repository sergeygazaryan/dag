from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
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
    description='Output notebook to GCS',
    schedule_interval=None,  # Disable automatic scheduling
    catchup=False,
)

run_notebook_task = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:latest',
    image_pull_policy='IfNotPresent',
    cmds=["/bin/bash", "-c"],
    arguments=[
        """
        git clone https://github.com/sergeygazaryan/notebook.git /tmp/workspace && \
        papermill /tmp/workspace/pyspark_logging_example.ipynb /tmp/workspace/test-output.ipynb
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

upload_notebook_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_notebook_to_gcs',
    src='/tmp/workspace/test-output.ipynb',
    dst='notebooks/test-output.ipynb',
    bucket='intent_db_to_ch',
    gcp_conn_id='google_cloud_default',  # Reference to the configured GCS connection
    dag=dag,
)

run_notebook_task >> upload_notebook_to_gcs
