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
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',  # Use the new image tag
    image_pull_policy='IfNotPresent',  # Or 'Always' if you want to always pull the latest version
    cmds=["/bin/bash", "-c"],
    arguments=[
        """
        set -e
        echo "Starting the task..."
        git clone https://github.com/sergeygazaryan/notebook.git /tmp/workspace
        echo "Repository cloned. Running papermill..."
        if papermill /tmp/workspace/xcom_output.ipynb /tmp/workspace/test-output.ipynb > /tmp/workspace/output.log 2>&1; then
          echo "Papermill execution finished. Output log:"
          cat /tmp/workspace/output.log
          echo "Papermill notebook output:"
          cat /tmp/workspace/test-output.ipynb
        else
          echo "Papermill execution failed. Check output log:" >&2
          cat /tmp/workspace/output.log >&2
          exit 1
        fi
        """
    ],
    name="run-notebook-task",
    task_id="run_notebook_task",
    is_delete_operator_pod=False,
    in_cluster=True,
    get_logs=True,
    dag=dag,
    startup_timeout_seconds=300
)

run_notebook_task
