from config import config

from airflow import DAG
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

from airflow.exceptions import AirflowSkipException

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator


pyspark_job = config.pyspark_job
pyspark_job['pyspark_job']['args'].append("--execution_date={{ ds }}")

tables = ['match','pick','ban','champion']

def check_object(**context):
  object_list = context['ti'].xcom_pull(task_ids='list_objects')
  if len(object_list) == 0:
    return 'end'
  else:
    return 'check_processed'

def check_spark_job_need(**context):
  success_list = context['ti'].xcom_pull(task_ids='check_processed')
  print(success_list)
  if len(success_list) == 4:
    return 'create_table_if_not_exists'
  else:
    for job in success_list:
      pyspark_job['pyspark_job']['args'].append(f"--{job.split('/')[1]}")
    return 'create_cluster'

def check_record_exist(execution_date,table):
  print(execution_date)
  hook = BigQueryHook(location='asia-northeast3', use_legacy_sql=False)
  sql = f"SELECT EXISTS(SELECT execution_date FROM `fourth-way-398009.summoner_match.{table}` WHERE date <= '{execution_date}' and execution_date='{execution_date}')"
  result = hook.get_first(sql=sql)[0]
  if result:
    raise AirflowSkipException

def create_table_if_not_exists():

  hook = BigQueryHook()

  for table, schema in config.schema.items():
    if hook.table_exists(dataset_id='summoner_match',
                          table_id=table,
                          project_id='fourth-way-398009'):
      continue
    resource = config.resource
    resource['schema'] = {'fields':schema}
    resource['tableReference']['tableId'] = table
    hook.create_empty_table(table_resource=resource)

with DAG(
    dag_id='raw_to_processed_v5',
    start_date=datetime(2023, 9, 8),
    end_date=datetime(2023, 9, 9),
    schedule='0 0 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
  start = BashOperator(
    task_id = 'start',
    bash_command= "echo {{ ds }}"
  )

  get_object_list = GCSListObjectsOperator(
    task_id = 'list_objects',
    bucket = 'summoner-match',
    prefix = 'raw/{{ ds }}/',
    delimiter = '.json'
  )

  check_object = BranchPythonOperator(
    task_id = 'check_object',
    python_callable = check_object,
  )

  check_processed = GCSListObjectsOperator(
    task_id = 'check_processed',
    bucket = 'summoner-match',
    prefix = 'processed',
    delimiter = '{{ ds }}.parquet/_SUCCESS',
  )

  check_spark_job_need = BranchPythonOperator(
    task_id = 'check_spark_job_need',
    python_callable = check_spark_job_need
  )

  spark_job = DataprocSubmitJobOperator(
    task_id = 'spark_job',
    region = 'asia-northeast3',
    job = pyspark_job,
  )

  create_table_if_not_exists = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_table_if_not_exists,
    trigger_rule='none_failed'
  )
  create_cluster = DummyOperator(task_id='create_cluster')

  # create_cluster = DataprocCreateClusterOperator(
  #   task_id='create_cluster',
  #   **config.cluster_config
  # )

  # delete_cluster = DataprocDeleteClusterOperator(
  #   task_id='delete_cluster',
  #   region=config.cluster_config['region'],
  #   cluster_name=config.cluster_config['cluster_name'],
  #   project_id=config.cluster_config['project_id']
  # )

  delete_raw_data =  DummyOperator(task_id='delete_raw_data', trigger_rule='none_failed')

  # GCSDeleteObjectsOperator(
  #   task_id = 'delete_raw_data',
  #   bucket_name = 'summoner-match',
  #   prefix = 'raw/{{ ds }}/',
  #   trigger_rule='none_failed'
  # )

  end = DummyOperator(task_id='end', trigger_rule='all_done')

  for table in tables:
    bigquery_check = PythonOperator(
      task_id=f'{table}_check',
      python_callable=check_record_exist,
      op_kwargs={
        'execution_date': "{{ds}}",
        'table': table
      }
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
      task_id = f'{table}_insert',
      bucket = 'summoner-match',
      source_objects = f'processed/{table}/'+'{{ ds }}.parquet/*.parquet',
      destination_project_dataset_table = f'fourth-way-398009.summoner_match.{table}',
      source_format = 'PARQUET',
      write_disposition = 'WRITE_APPEND',
    )

    create_table_if_not_exists >> bigquery_check >> gcs_to_bigquery >> delete_raw_data



  start >> get_object_list >> check_object >> [end, check_processed]
  check_processed >> check_spark_job_need
  check_spark_job_need >> [create_table_if_not_exists, create_cluster]
  create_cluster >> spark_job >> create_table_if_not_exists
  delete_raw_data >> end