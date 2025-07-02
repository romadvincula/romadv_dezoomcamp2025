from web_to_gcs import web_to_gcs
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow import DAG
from datetime import datetime


default_args = {
    "owner": "airflow",
    "retries": 0,
}
YEAR_MONTH = '{{ execution_date.strftime(\'%Y-%m\') }}'

with DAG(
    dag_id="week4_homework_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    max_active_runs=1,
    tags=['dtc-de', 'week4-homework']
) as dag:
    """
    DAG to load data for week 4 homework.
    """
    start_task = DummyOperator(
        task_id="start_task",
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    # web_to_gcs_green_task = PythonOperator(
    #     task_id='web_to_gcs_green_task',
    #     python_callable=web_to_gcs,
    #     op_kwargs={'year_month': YEAR_MONTH, 'service': 'green'}
    # )

    # web_to_gcs_yellow_task = PythonOperator(
    #     task_id='web_to_gcs_yellow_task',
    #     python_callable=web_to_gcs,
    #     op_kwargs={'year_month': YEAR_MONTH, 'service': 'yellow'}
    # )

    web_to_gcs_fhv_task = PythonOperator(
        task_id='web_to_gcs_fhv_task',
        python_callable=web_to_gcs,
        op_kwargs={'year_month': YEAR_MONTH, 'service': 'fhv'}
    )

    start_task >> web_to_gcs_fhv_task >> end_task
    # start_task >> [web_to_gcs_yellow_task, web_to_gcs_green_task] >> end_task

    
# @dag(
#     description="Load data for week 4 homework",
#     schedule_interval="0 6 2 * *",
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     default_args=default_args,
#     max_active_runs=2,
#     tags=['dtc-de', 'week4-homework'])
# def week4_homework_dag():
#     """
#     DAG to load data for week 4 homework.
#     """
#     start_task = DummyOperator(
#         task_id="start_task",
#     )

#     end_task = DummyOperator(
#         task_id="end_task",
#     )

#     @task
#     def web_to_gcs_green_2019():
#         web_to_gcs(year='2019', service='green')

#     @task
#     def web_to_gcs_green_2020():
#         web_to_gcs(year='2020', service='green')

#     @task
#     def web_to_gcs_yellow_2019():
#         web_to_gcs(year='2019', service='yellow')

#     @task
#     def web_to_gcs_yellow_2020():
#         web_to_gcs(year='2020', service='yellow')

#     @task
#     def web_to_gcs_fhv_2019():
#         web_to_gcs(year='2019', service='fhv')

#     chain(
#         start_task,
#         [web_to_gcs_green_2019(), web_to_gcs_green_2020()],        
#         [web_to_gcs_yellow_2019(), web_to_gcs_yellow_2020()],        
#         web_to_gcs_fhv_2019(),    
#         end_task
#     )

# week4_homework_dag