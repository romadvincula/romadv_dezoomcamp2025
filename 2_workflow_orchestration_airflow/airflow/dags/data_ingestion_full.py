from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

from datetime import datetime
import os
from ingest_script import ingest_by_batch, ingest_by_read_row_group, upload_to_gcs

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
YELLOW_TAXI_DATA = 'yellow'
GREEN_TAXI_DATA = 'green'
FHV_TAXI_DATA = 'fhv'  # ex: https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet
YELLOW_URL_TEMPLATE = URL_PREFIX + YELLOW_TAXI_DATA + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_URL_TEMPLATE = URL_PREFIX + GREEN_TAXI_DATA + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_URL_TEMPLATE = URL_PREFIX + FHV_TAXI_DATA + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_FILE_TEMPLATE = YELLOW_TAXI_DATA + '_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_FILE_TEMPLATE = GREEN_TAXI_DATA + '_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_FILE_TEMPLATE = FHV_TAXI_DATA + '_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'trips_data_all')

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_full",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 8, 1),
    default_args=default_args,
    max_active_runs=4,
    tags=['dtc-de'],
) as dag:

    start_task = DummyOperator(
        task_id="start_task",
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    download_yellow_task = BashOperator(
        task_id='download_yellow_task',
        bash_command=f"wget {YELLOW_URL_TEMPLATE} -O {AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}; echo {AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}",
        do_xcom_push=True
    )

    download_green_task = BashOperator(
        task_id='download_green_task',
        bash_command=f"wget {GREEN_URL_TEMPLATE} -O {AIRFLOW_HOME}/{GREEN_FILE_TEMPLATE}; echo {AIRFLOW_HOME}/{GREEN_FILE_TEMPLATE}",
        do_xcom_push=True
    )

    download_fhv_task = BashOperator(
        task_id='download_fhv_task',
        bash_command=f"wget {FHV_URL_TEMPLATE} -O {AIRFLOW_HOME}/{FHV_FILE_TEMPLATE}; echo {AIRFLOW_HOME}/{FHV_FILE_TEMPLATE}",
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

    upload_green_to_gcs_task = PythonOperator(
        task_id="upload_green_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"landing/{GREEN_FILE_TEMPLATE}",
            "local_file": "{{ ti.xcom_pull(task_ids='download_green_task') }}"
        },
        provide_context=True,
    )

    upload_fhv_to_gcs_task = PythonOperator(
        task_id="upload_fhv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"landing/{FHV_FILE_TEMPLATE}",
            "local_file": "{{ ti.xcom_pull(task_ids='download_fhv_task') }}"
        },
        provide_context=True,
    )

    cleanup_download_yellow_task = BashOperator(
        task_id="cleanup_download_yellow_task",
        bash_command="rm {{ ti.xcom_pull(task_ids='download_yellow_task') }}"
    )

    cleanup_download_green_task = BashOperator(
        task_id="cleanup_download_green_task",
        bash_command="rm {{ ti.xcom_pull(task_ids='download_green_task') }}"
    )

    cleanup_download_fhv_task = BashOperator(
        task_id="cleanup_download_fhv_task",
        bash_command="rm {{ ti.xcom_pull(task_ids='download_fhv_task') }}"
    )

    # print_xcom = BashOperator(
    #     task_id='print_xcom',
    #     bash_command="echo {{ ti.xcom_pull(task_ids='upload_yellow_to_gcs_task', key='object_url') }}"
    # )

    chain(
        start_task, 
        [download_yellow_task, download_green_task, download_fhv_task], 
        [upload_yellow_to_gcs_task, upload_green_to_gcs_task, upload_fhv_to_gcs_task], 
        [cleanup_download_yellow_task, cleanup_download_green_task, cleanup_download_fhv_task],
        end_task
        )

