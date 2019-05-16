from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators import BashOperator
import json

from helpers import set_max_active_runs

DAG_CONF = Variable.get('platform_dconf_middleware', deserialize_json=True)

DAG_OBJ = DAG(
    dag_id='el_middleware',
    description='extract data , running middleware, and load data',
    default_args={
        'owner': DAG_CONF['owner'],
        'start_date': datetime(2017, 3, 2),
        'email': DAG_CONF['emails'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
    max_active_runs=set_max_active_runs(),
    schedule_interval='@daily')

#-----------------------------------------------------------------------------------------------------------------------

def bq_check_platform(**kwargs):
    hook = BigQueryHook(bigquery_conn_id=DAG_CONF['bigquery_conn_id'])
    table_exists = hook.table_exists(
        project_id=DAG_CONF['project'],
        dataset_id=DAG_CONF['source_dataset'],
        table_id=DAG_CONF['source_table'])
    if table_exists:
        return 'middleware_process'
    else:
        return 'no_data'

def verify_records_digital_listening_func(ds, **kwargs):
    platform_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_platform_record')
    digital_listening_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_digital_listening_record')
    return platform_record == digital_listening_record
#-------------------------------------------------------------------------------

bigquery_platform_check_data = BranchPythonOperator(
    task_id='bigquery_platform_check_data',
    python_callable=bq_check_platform,
    provide_context=True,
    dag=DAG_OBJ)

middleware_process = BashOperator(
    task_id='running middleware script',
    bash_command='python /home/airflow/gcs/middleware/middleware.py'
    provide_context=True,
    dag=DAG_OBJ
)

verify_records_digital_listening = PythonOperator(
    task_id='verify_records_digital_listening',
    python_callable=verify_records_digital_listening_func,
    provide_context=True,
    dag=DAG_OBJ)

#-------------------------------------------------------------------------------
sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
"""
Bigquery_platform_record = BigQueryCheckOperator(
    task_id='Bigquery_platform_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['source_dataset'],
        'table': DAG_CONF['source_table'],
    },
    dag=DAG_OBJ)

sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
"""
Bigquery_digital_listening_record = BigQueryCheckOperator(
    task_id='Bigquery_digital_listening_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['source_dataset'],
        'table': DAG_CONF['destination_table'],
    },
    dag=DAG_OBJ)

#-------------------------------------------------------------------------------

no_data = DummyOperator(
    task_id='no_data',
    trigger_rule='one_success',
    dag=DAG_OBJ)

all_task_completed = DummyOperator(
    task_id='all_task_completed',
    trigger_rule='one_success',
    dag=DAG_OBJ)

bigquery_platform_check_data >> [no_data,middleware_process]
middleware_process >> Bigquery_digital_listening_record >> Bigquery_platform_record >> verify_records_digital_listening
all_task_completed << [no_data,verify_records_digital_listening]