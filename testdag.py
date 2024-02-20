from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_spark_notebook',
    default_args=default_args,
    description='Run a Spark Jupyter Notebook',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

run_notebook = KubernetesPodOperator(
    namespace='default',
    image='your-custom-image:latest', # Make sure this image has Papermill, pyspark, etc.
    cmds=["papermill", "https://github.com/sergeygazaryan/notebook/test.ipynb", "https://github.com/sergeygazaryan/notebook/output.ipynb"],
    name="run-spark-notebook",
    task_id="notebook_task",
    get_logs=True,
    dag=dag,
)
