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

def process_xcom_output(**context):
    output = context['ti'].xcom_pull(task_ids='execute_notebook_group.execute_notebook', key='return_value')
    print(f"Pulled Output: {output}")

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
                echo "Extracting results from test-output.ipynb..."
                output=$(python3 -c "
import json
import nbformat
notebook_path = '$OUTPUT_NOTEBOOK'
with open(notebook_path, 'r') as f:
    nb = nbformat.read(f, as_version=4)

parsed_output = None
for cell in nb.cells:
    if cell.cell_type == 'code':
        for cell_output in cell.outputs:
            if cell_output.output_type == 'stream' and cell_output.name == 'stdout':
                text = cell_output.text
                if text.startswith('{') and text.endswith('}\\n'):
                    parsed_output = json.loads(text)
                    break
        if parsed_output:
            break

if parsed_output:
    print(json.dumps(parsed_output))
else:
    raise ValueError('No JSON output found in the notebook.')
")
                echo "Pushing results to XCom"
                echo $output > /airflow/xcom/return.json
                """
            ],
            get_logs=True,
            in_cluster=True,
            is_delete_operator_pod=False,
        )

    process_xcom_output_task = PythonOperator(
        task_id='process_xcom_output',
        python_callable=process_xcom_output,
        provide_context=True,
    )

    execute_notebook_group >> process_xcom_output_task
