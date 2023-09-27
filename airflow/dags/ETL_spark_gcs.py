from config import config

from airflow import DAG

from datetime import datetime
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator


pyspark_job = config.raw_to_processed
pyspark_job['pyspark_job']['args'].append("--execution_date={{ ds }}")

tables = ['match','champion']

def check_object(**context):
  object_list = context['ti'].xcom_pull(task_ids='list_objects')
  if len(object_list) == 0:
    return 'end'
  else:
    return 'check_processed'

def check_spark_job_need(**context):
  success_list = context['ti'].xcom_pull(task_ids='check_processed')
  print(success_list)
  if len(success_list) == 2:
    return 'end'
  else:
    for job in success_list:
      pyspark_job['pyspark_job']['args'].append(f"--{job.split('/')[1]}")
    return 'create_cluster'


with DAG(
    dag_id='raw_to_processed_gcs',
    start_date=datetime(2023, 9, 8),
    schedule='0 0 * * *',
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

  end = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='raw_to_processed_gcs',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

  start >> get_object_list >> check_object >> [end, check_processed]
  check_processed >> check_spark_job_need
  check_spark_job_need >> [end, create_cluster]
  create_cluster >> spark_job >> end
  delete_raw_data >> end