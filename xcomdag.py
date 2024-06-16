from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
import json

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

# This will be the bash command to run the notebook and push the result to XCom
bash_command = """
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

# Extracting results to return.json
jq '.cells[-1].outputs[0].data["application/json"]' /tmp/workspace/test-output.ipynb > /tmp/workspace/return.json
echo "Results extracted to return.json:"
cat /tmp/workspace/return.json

# Reading return.json and pushing to XCom
xcom_data=$(cat /tmp/workspace/return.json)
echo "Pushing to XCom"
echo $xcom_data
"""

execute_notebook = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
    cmds=["/bin/bash", "-c"],
    arguments=[bash_command],
    name="notebook-execution",
    task_id="execute-notebook",
    get_logs=True,
    is_delete_operator_pod=False,  # Do not delete the pod so we can access logs if needed
    do_xcom_push=True,  # Enable XCom push
    dag=dag,
)

execute_notebook
