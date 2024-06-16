from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='xcom_dag_output',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    execute_notebook = KubernetesPodOperator(
        namespace='airflow',
        image="sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0",
        cmds=["/bin/bash", "-c"],
        arguments=["""
            set -e
            REPO_URL="https://github.com/sergeygazaryan/notebook.git"
            WORKSPACE="/tmp/workspace"
            OUTPUT_LOG="$WORKSPACE/output.log"
            OUTPUT_NOTEBOOK="$WORKSPACE/test-output.ipynb"
            XCOM_FILE="/tmp/workspace/return.json"

            echo "Cloning the repository..."
            timeout 300 git clone $REPO_URL $WORKSPACE || { echo "Git clone failed!"; exit 1; }
            echo "Repository cloned. Listing contents..."
            ls -la $WORKSPACE
            echo "Running papermill..."
            papermill $WORKSPACE/xcom_output.ipynb $OUTPUT_NOTEBOOK > $OUTPUT_LOG 2>&1 || { echo "Papermill execution failed!"; cat $OUTPUT_LOG; exit 1; }
            echo "Papermill executed successfully. Listing contents of workspace..."
            ls -la $WORKSPACE
            echo "Contents of output.log:"
            cat $OUTPUT_LOG
            echo "Contents of test-output.ipynb:"
            cat $OUTPUT_NOTEBOOK

            # Extracting results to return.json using Python
            echo "Extracting results to return.json using Python..."
            python3 << EOF
import json

with open('$OUTPUT_LOG', 'r') as f:
    lines = f.readlines()

output = None
for line in lines:
    if line.startswith('{') and line.endswith('}\n'):
        output = json.loads(line)
        break

if output:
    with open('$XCOM_FILE', 'w') as f:
        json.dump(output, f)
else:
    print("Error: No JSON output found in the log.")
    exit(1)
EOF

            echo "Results extracted to return.json:"
            cat $XCOM_FILE

            # Push XCom result
            if [ -f $XCOM_FILE ]; then
              xcom_data=$(cat $XCOM_FILE)
              echo "Pushing to XCom"
              echo $xcom_data > /airflow/xcom/return.json
            else
              echo "Error: XCom file $XCOM_FILE not found."
              exit 1
            fi
        """],
        name="execute-notebook",
        task_id="execute-notebook",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        config_file=None,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> execute_notebook >> end
