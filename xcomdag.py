from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_dag_output',
    default_args=default_args,
    description='A DAG to execute a Jupyter notebook and push output to XCom',
    schedule_interval=timedelta(days=1),
)

# Bash command to clone repo, run notebook, extract results, and push to XCom
bash_command = """
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

with open('$OUTPUT_NOTEBOOK', 'r') as f:
    notebook = json.load(f)

output = None
for cell in notebook.get('cells', []):
    if 'outputs' in cell and cell['outputs']:
        for out in cell['outputs']:
            if 'text' in out and 'application/json' in out['text']:
                output = json.loads(out['text'])
                break
    if output:
        break

if output:
    with open('$XCOM_FILE', 'w') as f:
        json.dump(output, f)
else:
    print("Error: No JSON output found in the notebook.")
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
"""

# Task to execute the notebook
execute_notebook = KubernetesPodOperator(
    namespace='airflow',
    image='sergeygazaryan13/airflow2.1.2-pyspark3.1.2:v1.0.0',
    cmds=["/bin/bash", "-c"],
    arguments=[bash_command],
    name="notebook-execution",
    task_id="execute-notebook",
    get_logs=True,
    is_delete_operator_pod=False,
    do_xcom_push=True,
    dag=dag,
)

execute_notebook
