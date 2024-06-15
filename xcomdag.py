from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
import os
import json
import tempfile

def push_xcom_from_file(**kwargs):
    ti = kwargs['ti']
    xcom_file = os.path.join(tempfile.gettempdir(), 'return.json')
    if os.path.exists(xcom_file):
        with open(xcom_file, 'r') as f:
            xcom_data = json.load(f)
        ti.xcom_push(key='return_value', value=xcom_data)
    else:
        raise FileNotFoundError(f"XCom file not found: {xcom_file}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_dag_output',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

execute_notebook = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
    cmds=["/bin/bash", "-c"],
    arguments=[
        '''
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
        '''
    ],
    name="notebook-execution",
    task_id="execute-notebook",
    get_logs=True,
    dag=dag,
)

push_xcom = PythonOperator(
    task_id='push_xcom',
    python_callable=push_xcom_from_file,
    provide_context=True,
    dag=dag,
)

execute_notebook >> push_xcom
