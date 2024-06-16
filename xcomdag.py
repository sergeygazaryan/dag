from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
import json
import nbformat
import pytz

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
    description='DAG to execute notebook and push to XCom',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def parse_notebook_and_push_xcom(**kwargs):
        # Assuming the output notebook is available at a certain location
        output_notebook_path = "/tmp/workspace/test-output.ipynb"
        with open(output_notebook_path, "r") as f:
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
            ti = kwargs['ti']
            ti.xcom_push(key='parsed_output', value=output)
        else:
            raise ValueError("No JSON output found in the notebook.")

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
                """
            ],
            get_logs=True,
            in_cluster=True,
            is_delete_operator_pod=False,
        )

        parse_notebook = PythonOperator(
            task_id='parse-notebook',
            python_callable=parse_notebook_and_push_xcom,
            provide_context=True
        )

    def use_xcom_data(**kwargs):
        ti = kwargs['ti']
        parsed_output = ti.xcom_pull(key='parsed_output', task_ids='execute_notebook_group.parse-notebook')
        # Use the parsed_output for further processing
        print(f"Parsed Output: {parsed_output}")

    use_data = PythonOperator(
        task_id='use-data',
        python_callable=use_xcom_data,
        provide_context=True,
        dag=dag,
    )

    execute_notebook >> parse_notebook >> use_data
