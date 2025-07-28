from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Variable

import requests
import datetime as dt


EXECUTION_DAY_OF_MONTH = 2
year_month = "{{ execution_date.strftime(\'%Y-%m\') }}"

def check_run_date_is_behind_2_months(**context):
    today = dt.datetime.now()
    two_months_ago = (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1) - dt.timedelta(days=1)
    end_date = two_months_ago.replace(day=EXECUTION_DAY_OF_MONTH)
    execution_date = context["execution_date"].replace(tzinfo=None)
    print("End Date: " + end_date.strftime('%Y-%m-%d'))
    print("Execution Date: " + execution_date.strftime('%Y-%m-%d'))

    if execution_date <= end_date:
        return 'yellow_download_to_gcs'
    else:
        days_deffered = (execution_date - end_date).days
        print("Deffered days: " + str(days_deffered))
        context['ti'].xcom_push(key='days_deffered', value=days_deffered)
        print("Skipped as execution date is not behind 2 months. (data not yet published)")
        return 'wait_for_deffered_days'
    
def call_cloud_run_func(year_month, service):
    url = Variable.get("cloud_run_func_url")
    headers={
            "Authorization": Variable.get("cloud_run_func_auth_key"),
            "Content-Type": "application/json"
            }
    data='{"year_month": "' + year_month + '", "service": "' + service + '"}'

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    print(response.text)

    
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 60,
    'start_date': dt.datetime(2023, 1, 1),
    # 'end_date': dt.datetime(2023, 1, 8),
}

with DAG(
    dag_id='nyc_taxi_elt',
    schedule_interval=f'0 6 {EXECUTION_DAY_OF_MONTH} * *',
    default_args=default_args,
    max_active_runs=3,
    catchup=True,
    tags=['romadv'],
) as dag:

    start_task = DummyOperator(task_id='start')

    end_task = DummyOperator(task_id='end')

    check_run_date_is_behind_2_months = BranchPythonOperator(
        task_id='check_run_date_is_behind_2_months',
        python_callable=check_run_date_is_behind_2_months,
        provide_context=True,
    )

    wait_for_deffered_days = TimeDeltaSensor(
        task_id='wait_for_deffered_days',
        mode='reschedule',
        delta=dt.timedelta(days=60),
        # delta=dt.timedelta(days=(lambda x: int(x))("{{ ti.xcom_pull(task_ids='check_run_date_is_behind_2_months', key='days_deffered') }}") ),
    )

    yellow_download_to_gcs = PythonOperator(
        task_id='yellow_download_to_gcs',
        python_callable=call_cloud_run_func,
        op_kwargs={"year_month": year_month, "service": "yellow"},
        trigger_rule='none_failed_min_one_success'
    )

    green_download_to_gcs = PythonOperator(
        task_id='green_download_to_gcs',
        python_callable=call_cloud_run_func,
        op_kwargs={"year_month": year_month, "service": "green"},
    )

    fhv_download_to_gcs = PythonOperator(
        task_id='fhv_download_to_gcs',
        python_callable=call_cloud_run_func,
        op_kwargs={"year_month": year_month, "service": "fhv"},
    )


    start_task >> check_run_date_is_behind_2_months >> yellow_download_to_gcs
    start_task >> check_run_date_is_behind_2_months >> wait_for_deffered_days
    yellow_download_to_gcs >> green_download_to_gcs >> fhv_download_to_gcs >> end_task
    wait_for_deffered_days >> yellow_download_to_gcs >> green_download_to_gcs >> fhv_download_to_gcs >> end_task
