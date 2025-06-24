from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime
import os
from ingest_script import upload_to_gcs


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = "dezoomcamp_hw3_2025_dtc-de-course-462612"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
YELLOW_TAXI_DATA = 'yellow'
YELLOW_URL_TEMPLATE = URL_PREFIX + YELLOW_TAXI_DATA + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_FILE_TEMPLATE = YELLOW_TAXI_DATA + '_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'trips_data_all')


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="week3_homework_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 7, 1),
    default_args=default_args,
    max_active_runs=4,
    tags=['dtc-de'],
) as dag:
    
    start_task = DummyOperator(
        task_id="start_task",
    )

    download_yellow_task = BashOperator(
        task_id='download_yellow_task',
        bash_command=f"wget {YELLOW_URL_TEMPLATE} -O {AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}; echo {AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}",
        do_xcom_push=True
    )

    upload_yellow_to_gcs_task = PythonOperator(
        task_id="upload_yellow_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"landing/{YELLOW_FILE_TEMPLATE}",
            "local_file": "{{ ti.xcom_pull(task_ids='download_yellow_task') }}",
        },
        provide_context=True,
    )

    cleanup_download_yellow_task = BashOperator(
        task_id="cleanup_download_yellow_task",
        bash_command="rm {{ ti.xcom_pull(task_ids='download_yellow_task') }}"
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    start_task >> download_yellow_task >> upload_yellow_to_gcs_task >> cleanup_download_yellow_task >> end_task

