from datetime import timedelta
from extract.common_parser_operator import random_api_download
from load.pg_load import load_data
from connections.init_conn import init_connections
from delete.delete_by_name import delete_files

from airflow import DAG
from airflow.hooks.base import BaseHook

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'aero_test',
    default_args=default_args,
    schedule_interval=timedelta(hours=12),
    max_active_runs=1,
    start_date=days_ago(1),
)

init_connections()

export_task = PythonOperator(
    task_id='export_data',
    python_callable=random_api_download,
    provide_context=True,
    op_kwargs={
        'api_url': 'https://random-data-api.com/api/cannabis/random_cannabis?size=100&response_type=json',
        'retry_num_max': 5,
        'file_name': "data_{{ data_interval_end }}"
    },
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_pg',
    python_callable=load_data,
    provide_context=True,
    templates_dict={'filename': "{{ ti.xcom_pull(task_ids='export_data') }}"},
    params={'connection': BaseHook.get_hook(conn_id='pg_cannabis')},
    dag=dag,
)

delete_task = PythonOperator(
    task_id='delete_all_data',
    python_callable=delete_files,
    provide_context=True,
    templates_dict={'filename': "{{ ti.xcom_pull(task_ids='export_data') }}"},
    dag=dag,
)


export_task >> load_task >> delete_task
