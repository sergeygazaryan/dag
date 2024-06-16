from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
import json
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def fix_dates_in_output(output):
    if isinstance(output, dict):
        for key, value in output.items():
            if isinstance(value, str) and len(value) > 18 and value[4] == '-' and value[10] == 'T':
                try:
                    output[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
                except ValueError:
                    try:
                        output[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        pass
            elif isinstance(value, dict):
                fix_dates_in_output(value)
            elif isinstance(value, list):
                for item in value:
                    fix_dates_in_output(item)

with DAG(
    'xcom_dag_output',
    default_args=default_args,
    description='DAG xcom',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    execute_notebook = KubernetesPodOperator(
        task_id='execute-notebook',
        name='execute-notebook',
        namespace='airflow',
        image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
        cmds=['/bin/bash', '-c'],
        arguments=[
            """
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
            echo "Extracting results from test-output.ipynb using Python..."
            python3 << EOF
import json
import nbformat
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.utils.session import create_session
from datetime import datetime

def fix_dates_in_output(output):
    if isinstance(output, dict):
        for key, value in output.items():
            if isinstance(value, str) and len(value) > 18 and value[4] == '-' and value[10] == 'T':
                try:
                    output[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
                except ValueError:
                    try:
                        output[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        pass
            elif isinstance(value, dict):
                fix_dates_in_output(value)
            elif isinstance(value, list):
                for item in value:
                    fix_dates_in_output(item)

# Helper function to push XCom
@provide_session
def push_xcom(task_id, key, value, execution_date, dag_id, session=None):
    XCom.set(
        key=key,
        value=value,
        execution_date=execution_date,
        task_id=task_id,
        dag_id=dag_id,
        session=session
    )

with open("$OUTPUT_NOTEBOOK", "r") as f:
    nb = nbformat.read(f, as_version=4)

output = None
for cell in nb.cells:
    if cell.cell_type == 'code':
        for cell_output in cell.outputs:
            if cell_output.output_type == 'stream' and cell_output.name == 'stdout':
                text = cell_output.text
                if text.startswith("{") and text.endswith("}\\n"):
                    output = json.loads(text)
                    fix_dates_in_output(output)
                    break
        if output:
            break

if output:
    with open("$XCOM_FILE", "w") as f:
        json.dump(output, f)
    print("Pushing results to XCom")
    push_xcom(task_id='execute-notebook', key='return_value', value=output, execution_date="{{ ts }}", dag_id='xcom_dag_output')
else:
    print("Error: No JSON output found in the notebook.")
    exit(1)
EOF
            echo "Results extracted to return.json:"
            cat $XCOM_FILE
            """
        ],
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=False,
    )

    execute_notebook
