from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def extract_results_from_xcom(ti):
    output = ti.xcom_pull(task_ids='execute_notebook_group.execute-notebook', key='return_value')
    parsed_output = json.loads(output)
    print(f"Parsed Output: {parsed_output}")

with DAG(
    'xcom_dag_output',
    default_args=default_args,
    description='DAG to execute notebook and push to XCom',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    with TaskGroup("execute_notebook_group") as execute_notebook_group:
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
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from datetime import datetime
import pytz

with open("/tmp/workspace/test-output.ipynb", "r") as f:
    nb = nbformat.read(f, as_version=4)

output = None
for cell in nb.cells:
    if cell.cell_type == 'code':
        for cell_output in cell.outputs:
            if cell_output.output_type == 'stream' and cell_output.name == 'stdout':
                text = cell_output.text
                if text.startswith("{") and text.endswith("}\\n"):
                    output = json.loads(text)
                    break
        if output:
            break

if output:
    print("Pushing results to XCom")
    current_time = datetime.now(pytz.utc)  # Use timezone-aware datetime
    task_instance = TaskInstance(
        dag_id='xcom_dag_output',
        task_id='execute-notebook',
        execution_date=current_time,
        state=State.SUCCESS
    )
    task_instance.xcom_push(key='return_value', value=json.dumps(output))
else:
    print("Error: No JSON output found in the notebook.")
    exit(1)
EOF
                echo "Results extracted."
                """
            ],
            get_logs=True,
            in_cluster=True,
            is_delete_operator_pod=False,
        )

        parse_results = PythonOperator(
            task_id='parse-results',
            python_callable=extract_results_from_xcom,
        )

        execute_notebook >> parse_results
