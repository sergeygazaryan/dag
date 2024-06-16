from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

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

with open("$OUTPUT_NOTEBOOK", "r") as f:
    nb = nbformat.read(f, as_version=4)

output = None
for cell in nb.cells:
    if cell.cell_type == 'code':
        for output in cell.outputs:
            if output.output_type == 'stream' and output.name == 'stdout':
                text = output.text
                if text.startswith("{") and text.endswith("}\\n"):
                    output = json.loads(text)
                    break
        if output:
            break

if output:
    with open("$XCOM_FILE", "w") as f:
        json.dump(output, f)
else:
    print("Error: No JSON output found in the notebook.")
    exit(1)
EOF
            echo "Results extracted to return.json:"
            cat $XCOM_FILE
            if [ -f $XCOM_FILE ]; then
              xcom_data=$(cat $XCOM_FILE)
              echo "Pushing to XCom"
              echo $xcom_data > /airflow/xcom/return.json
            else
              echo "Error: XCom file $XCOM_FILE not found."
              exit 1
            fi
            """
        ],
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=False,
    )

    execute_notebook
