from config import config

from airflow import DAG

from datetime import datetime
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator


def check_object(**context):
  object_list = context['ti'].xcom_pull(task_ids='list_objects')
  delete_set = set()
  for object in object_list:
    if len(object.split('/'))>=4 and object.split('/')[2] <= (context['execution_date']-timedelta(days=14)).strftime('%Y-%m-%d'):
      delete_set.add('/'.join(object.split('/')[:3])+'/')

  if len(delete_set) > 0:
    context['task_instance'].xcom_push(key='delete_list',value=list(delete_set))
    return 'delete_object'
  else:
    return 'end'

with DAG(
    dag_id='delete_gcs_object_v1',
    start_date=datetime(2023, 12, 26),
    schedule='0 0 * * 1',
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
    prefix = 'raw/'
  )

  check_object = BranchPythonOperator(
    task_id = 'check_object',
    python_callable = check_object
  )

  delete_object = GCSDeleteObjectsOperator(
    task_id = 'delete_object',
    bucket_name = 'gnimty_bucket',
    objects = "{{ ti.xcom_pull(key='delete_list') }}"
  )
#   delete_object = DummyOperator(task_id='delete_object')
  end = DummyOperator(task_id='end')

  get_object_list >> check_object >> [end, delete_object]
  delete_object >> end
