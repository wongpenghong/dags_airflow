from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
import json

from helpers import set_max_active_runs

DAG_CONF = Variable.get('platform_dconf_mobile_web', deserialize_json=True)

DAG_OBJ = DAG(
    dag_id='etl_feedback_mobile_web',
    description='ETL feedback data from mobile and web',
    default_args={
        'owner': DAG_CONF['owner'],
        'start_date': datetime(2017, 2, 3),
        'email': DAG_CONF['emails'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
    max_active_runs=set_max_active_runs(),
    schedule_interval='@daily')

def bq_check_mobile(**kwargs):
    hook = BigQueryHook(bigquery_conn_id=DAG_CONF['bigquery_conn_id'])
    table_exists = hook.table_exists(
        project_id=DAG_CONF['project'],
        dataset_id=DAG_CONF['dataset_mobile'],
        table_id=DAG_CONF['table_mobile'])
    if table_exists:
        return 'Bigquery_load_mobile_data'
    else:
        return 'bigquery_website_check_data'

def bq_check_website(**kwargs):
    hook = BigQueryHook(bigquery_conn_id=DAG_CONF['bigquery_conn_id'])
    table_exists = hook.table_exists(
        project_id=DAG_CONF['project'],
        dataset_id=DAG_CONF['dataset_website'],
        table_id=DAG_CONF['table_website'])
    if table_exists:
        return 'Bigquery_load_website_data'
    else:
        return 'no_data'

def verify_records_func_mobile(ds, **kwargs):
    mobile_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_mobile_record')
    platform_mobile_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_mobile_platform_record')
    return mobile_record == platform_mobile_record

def verify_records_func_website(ds, **kwargs):
    website_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_website_record')
    platform_website_record = kwargs['ti'].xcom_pull(task_ids='Bigquery_website_platform_record')
    return website_record == platform_website_record

#-------------------------------------------------------------------------------

bigquery_mobile_check_data = BranchPythonOperator(
    task_id='bigquery_mobile_check_data',
    python_callable=bq_check_mobile,
    provide_context=True,
    dag=DAG_OBJ)

bigquery_website_check_data = BranchPythonOperator(
    task_id='bigquery_website_check_data',
    python_callable=bq_check_website,
    provide_context=True,
    dag=DAG_OBJ)

verify_record_mobile = PythonOperator(
    task_id='verify_record_mobile',
    python_callable=verify_records_func_mobile,
    provide_context=True,
    dag=DAG_OBJ)

verify_record_website = PythonOperator(
    task_id='verify_record_website',
    python_callable=verify_records_func_website,
    provide_context=True,
    dag=DAG_OBJ)

#-------------------------------------------------------------------------------

sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}' AND resource = '{{ params.source_table}}'
"""
Bigquery_website_platform_record = BigQueryCheckOperator(
    task_id='Bigquery_website_platform_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_transform'],
        'table': DAG_CONF['table_transform'],
        'source_table': DAG_CONF['table_website'],
    },
    dag=DAG_OBJ)

sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
"""
Bigquery_website_record = BigQueryCheckOperator(
    task_id='Bigquery_website_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_website'],
        'table': DAG_CONF['table_website'],
    },
    dag=DAG_OBJ)

sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}' AND resource = '{{ params.source_table}}'
"""
Bigquery_mobile_platform_record = BigQueryCheckOperator(
    task_id='Bigquery_mobile_platform_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_transform'],
        'table': DAG_CONF['table_transform'],
        'source_table': DAG_CONF['table_mobile'],
    },
    dag=DAG_OBJ)

sql = """
SELECT COUNT(1) AS total
FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
"""
Bigquery_mobile_record = BigQueryCheckOperator(
    task_id='Bigquery_mobile_record',
    bigquery_conn_id=DAG_CONF['bigquery_conn_id'],
    sql=sql,
    use_legacy_sql=False,
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_mobile'],
        'table': DAG_CONF['table_mobile'],
    },
    dag=DAG_OBJ)

#-------------------------------------------------------------------------------

sql = """
WITH
  mobile_ds AS (
  SELECT
    Review_Text AS description,
    DATE(Review_Submit_Date_and_Time) AS created_at,
    'mobile' AS resource
  FROM
    `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
  WHERE
    DATE(Review_Submit_Date_and_Time) = '{{ ds }}')
SELECT
  *
FROM
  mobile_ds
WHERE
  mobile_ds.description IS NOT NULL
"""
Bigquery_load_mobile_data = BigQueryOperator(
    task_id='Bigquery_load_mobile_data',
    sql=sql,
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    destination_dataset_table='{{ params.project }}:{{ params.dataset_transform }}.{{ params.table_transform }}${{ ds_nodash }}',
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_mobile'],
        'table': DAG_CONF['table_mobile'],
        'dataset_transform': DAG_CONF['dataset_transform'],
        'table_transform': DAG_CONF['table_transform'],
    },
    dag=dag)

sql = """
WITH
  mobile_ds AS (
  SELECT
    Review_Text AS description,
    DATE(Review_Submit_Date_and_Time) AS created_at,
    'mobile' AS resource
  FROM
    `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
  WHERE
    DATE(Review_Submit_Date_and_Time) = '{{ ds }}')
SELECT
  *
FROM
  mobile_ds
WHERE
  mobile_ds.description IS NOT NULL
"""
Bigquery_load_mobile_data = BigQueryOperator(
    task_id='Bigquery_load_mobile_data',
    sql=sql,
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    destination_dataset_table='{{ params.project }}:{{ params.dataset_transform }}.{{ params.table_transform }}${{ ds_nodash }}',
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_mobile'],
        'table': DAG_CONF['table_mobile'],
        'dataset_transform': DAG_CONF['dataset_transform'],
        'table_transform': DAG_CONF['table_transform'],
    },
    dag=dag)

sql = """
WITH
  website_ds AS (
  SELECT
    description AS description,
    DATE(created_at) AS created_at,
    'website' AS resource
  FROM
    `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
  WHERE
    DATE(_PARTITIONTIME) = '{{ ds }}')
SELECT
  *
FROM
  website_ds
WHERE
  website_ds.description IS NOT NULL
"""
Bigquery_load_website_data = BigQueryOperator(
    task_id='Bigquery_load_website_data',
    sql=sql,
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    destination_dataset_table='{{ params.project }}:{{ params.dataset_transform }}.{{ params.table_transform }}${{ ds_nodash }}',
    params={
        'project': DAG_CONF['project'],
        'dataset':DAG_CONF['dataset_website'],
        'table': DAG_CONF['table_website'],
        'dataset_transform': DAG_CONF['dataset_transform'],
        'table_transform': DAG_CONF['table_transform'],
    },
    dag=dag)

#-----------------------------------------------------------------------

no_data = DummyOperator(
    task_id='no_data',
    trigger_rule='one_success',
    dag=DAG_OBJ)

all_task_completed = DummyOperator(
    task_id='all_task_completed',
    trigger_rule='one_success',
    dag=DAG_OBJ)

bigquery_mobile_check_data >> bigquery_website_check_data >> Bigquery_load_website_data >> Bigquery_website_record >> Bigquery_website_platform_record >> verify_record_website >> all_task_completed
bigquery_mobile_check_data >> bigquery_website_check_data >> no_data >> all_task_completed
bigquery_mobile_check_data >> Bigquery_load_mobile_data >> Bigquery_mobile_record >> Bigquery_mobile_platform_record >> verify_record_mobile >> 
                                    bigquery_website_check_data >> Bigquery_load_website_data >> Bigquery_website_record >> Bigquery_website_platform_record >> verify_record_website >> all_task_completed
bigquery_website_check_data >> no_data >> all_task_completed

bigquery_mobile_check_data >> [Bigquery_load_mobile_data,bigquery_website_check_data]
bigquery_website_check_data >> [no_data,Bigquery_load_website_data]
Bigquery_load_website_data >> Bigquery_website_record >> Bigquery_website_platform_record >> verify_record_website >> all_task_completed
Bigquery_load_mobile_data >> Bigquery_mobile_record >> Bigquery_mobile_platform_record >> verify_record_mobile >> bigquery_website_check_data >> [no_data,Bigquery_load_website_data]
Bigquery_load_website_data >> Bigquery_website_record >> Bigquery_website_platform_record >> verify_record_website >> all_task_completed