from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from ingest_script import ingest_by_batch, ingest_by_read_row_group

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

# local_workflow = DAG(
#     dag_id="ingestion_to_local_postgres",
#     schedule_interval="0 6 2 * *",
#     start_date=datetime(2019, 1, 1),
#     end_date=datetime(2022, 8, 1),
# )

# default_args = {
#     'max_active_runs': 1,
# }

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with DAG(
    dag_id="ingestion_to_local_postgres",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    # end_date=datetime(2022, 8, 1),
    end_date=datetime(2019, 4, 1),
    # default_args=default_args,
) as local_workflow:

    wget_task = BashOperator(
        task_id='wget_task',
        bash_command=f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="ingest_task",
        python_callable=ingest_by_batch,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            file=OUTPUT_FILE_TEMPLATE
        ),
    )

    wget_task >> ingest_task

