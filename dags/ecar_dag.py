from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
import logging
import sys

if os.path.exists("/usr/local/airflow/ecar-pipeline"):
    PROJECT_ROOT = "/usr/local/airflow/ecar-pipeline"  # Docker içindeki path
else:
    PROJECT_ROOT = "/Users/kaancakir/data_projects_local/ecar-pipeline"  # Lokal path

sys.path.insert(0, PROJECT_ROOT)

EXTRACT_SCRIPT = os.path.join(PROJECT_ROOT, "src", "extract_ecar.py")
TRANSFORM_SCRIPT = os.path.join(PROJECT_ROOT, "src", "transform_ecar.py")
LOAD_SCRIPT = os.path.join(PROJECT_ROOT, "src", "load.py")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def run_script(script_path: str):
    logging.info(f"Running script: {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError(f"Script failed: {script_path}")
    logging.info(result.stdout)

with DAG(
    dag_id="ecar_etl_pipeline",
    default_args=default_args,
    description="End-to-End Electric Car ETL Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 24),
    catchup=False,
    tags=["ecar", "etl", "azure", "spark"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable=run_script,
        op_args=[EXTRACT_SCRIPT],
    )

    transform_task = PythonOperator(
        task_id="transform_and_validate",
        python_callable=run_script,
        op_args=[TRANSFORM_SCRIPT],
    )

    load_task = PythonOperator(
        task_id="load_to_azure",
        python_callable=run_script,
        op_args=[LOAD_SCRIPT],
    )

    extract_task >> transform_task >> load_task
