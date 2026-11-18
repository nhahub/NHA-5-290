import sys
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_ROOT = "/Users/elsayedkoweidah/projects/iot_pipeline"
PYTHON_EXEC = sys.executable  # Uses the exact Python environment running Airflow

default_args = {
    "owner": "elsayed_koweidah",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="iot_batch_etl_pipeline",
    default_args=default_args,
    description="Production Batch ETL for IoT Sensor Data with Data Quality & Archiving",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="Africa/Cairo"),
    catchup=False,
    max_active_runs=1,
    tags=["IoT", "ETL", "DEPI"],
) as dag:

    # Task 1: Verify that the target day's CSV exists and is non-empty
    check_csv_file = BashOperator(
        task_id="check_daily_csv_exists",
        bash_command=f"test -s '{PROJECT_ROOT}/data/raw_sensor_logs_{{{{ ds }}}}.csv'",
    )

    # Task 2: Run Idempotent Fact Load & Hourly Aggregations
    run_batch_etl = BashOperator(
        task_id="run_etl_and_aggregations",
        bash_command=f"cd '{PROJECT_ROOT}/etl' && '{PYTHON_EXEC}' batch_etl.py --date {{{{ ds }}}}",
    )

    # Task 3: Archive closed day files (skips archiving if ds is today's active file)
    archive_closed_csv = BashOperator(
        task_id="archive_processed_csv",
        bash_command=f"cd '{PROJECT_ROOT}/etl' && '{PYTHON_EXEC}' batch_etl.py --date {{{{ ds }}}} --archive",
    )

    check_csv_file >> run_batch_etl >> archive_closed_csv