from config import config

from airflow import DAG

from datetime import datetime
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import BranchPythonOperator

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def check_objects(**context):
  object_list = context['ti'].xcom_pull(task_ids='list_objects')
  insert_set = set()
  print(object_list)
  for object in object_list:
    if len(object.split('/'))>=4 and object.split('/')[2].split('.')[0] >= (context['execution_date']-timedelta(days=14)).strftime('%Y-%m-%d'):
      insert_set.add('/'.join(object.split('/')[:3])+'/*.parquet')

  print(insert_set)

  if len(insert_set) > 0:
    context['task_instance'].xcom_push(key='insert_list',value=list(insert_set))
    return 'full_refresh'
  else:
    return 'end'

with DAG(
    dag_id='bigquery_full_refresh_v1',
    start_date=datetime(2023, 12, 26),
    schedule='0 1 * * *',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    render_template_as_native_obj=True,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

  get_object_list = GCSListObjectsOperator(
    task_id = 'list_objects',
    bucket = 'gnimty_bucket',
    prefix = 'bigquery/solo/'
  )

  check_object = BranchPythonOperator(
    task_id = 'check_object',
    python_callable = check_objects,
  )

  full_refresh = GCSToBigQueryOperator(
    task_id='full_refresh',
    bucket='gnimty_bucket',
    source_objects="{{ ti.xcom_pull(key='insert_list') }}",
    source_format='parquet',
    destination_project_dataset_table=f"match_summary.solo",
    write_disposition='WRITE_TRUNCATE',
  )

  end = DummyOperator(task_id='end')

  get_object_list >> check_object >> [full_refresh, end]
  full_refresh >> end
