from config import config

from airflow import DAG
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator


pyspark_job = config.raw_to_processed
pyspark_job['pyspark_job']['args'].append("--execution_date={{ ds }}")

tables = ['match','champion']

def check_object(**context):
  object_list = context['ti'].xcom_pull(task_ids='list_objects')
  if len(object_list) == 0:
    return 'check_date'
  else:
    return 'check_processed'

def check_spark_job_need(**context):
  success_list = context['ti'].xcom_pull(task_ids='check_processed')
  print(success_list)
  if len(success_list) == 2:
    return 'check_date'
  else:
    for job in success_list:
      pyspark_job['pyspark_job']['args'].append(f"--{job.split('/')[1]}")
    return 'create_cluster'

def check_date(**context):
  # if context['execution_date'].strftime('%Y-%m-%d') == datetime.utcnow().strftime('%Y-%m-%d'):
  if context['execution_date'].strftime('%Y-%m-%d') == '2023-09-22':
    return 'check_version'
  else:
    return 'end'

def check_version(**context):
  # game_version = '.'.join(requests.get('https://ddragon.leagueoflegends.com/api/versions.json').json()[0].split('.')[:2])
  game_version = '13.16'
  date_30d = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
  context['task_instance'].xcom_push(key='game_version',value=game_version)
  context['task_instance'].xcom_push(key='start_date',value=date_30d)

scripts = ['champion_counter.py','champion_synergy.py', 'champion_tier.py', 'processed_to_statistic.py']


with DAG(
    dag_id='raw_to_processed_gcs_v6',
    start_date=datetime(2023, 8, 29),
    schedule='0 0 * * *',
    max_active_runs=1,
    max_active_tasks=1,
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
    delimiter = '.json',
    trigger_rule='all_done'
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

  create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    **config.cluster_config
  )

  delete_raw_data =  DummyOperator(task_id='delete_raw_data', trigger_rule='none_failed')


  check_date = BranchPythonOperator(
    task_id = 'check_date',
    python_callable = check_date,
  )

  delete_cluster = DummyOperator(task_id='delete_cluster', trigger_rule='none_skipped')
  # DataprocDeleteClusterOperator(
  #   task_id='delete_cluster',
  #   region=config.cluster_config['region'],
  #   cluster_name=config.cluster_config['cluster_name'],
  #   project_id=config.cluster_config['project_id'],
  #   trigger_rule='none_skipped'
  # )

  check_version = PythonOperator(
    task_id='check_version',
    python_callable=check_version,
    trigger_rule='none_failed'
  )

  end = DummyOperator(task_id='end')

  check_date >> [check_version, end]

  for script in scripts:
    elt_job = config.processed_to_mongodb
    elt_job['pyspark_job']['properties']['spark.mongodb.write.connection.uri'] = Variable.get('mongodb_conn_secret')
    elt_job['pyspark_job']['main_python_file_uri']=f'gs://summoner-match/pyspark/{script}'

    if script in ['champion_counter.py','champion_synergy.py']:
      elt_job['pyspark_job']['args']=["--start_date={{ ti.xcom_pull(key='start_date') }}"]
    elif script == 'champion_tier.py':
      elt_job['pyspark_job']['args']=["--game_version={{ ti.xcom_pull(key='game_version') }}"]
    else:
      elt_job['pyspark_job']['args']=["--execution_date={{ ds }}"]

    elt_spark_job = DataprocSubmitJobOperator(
      task_id = f'{script}',
      region = 'asia-northeast3',
      job = elt_job,
    )

    check_version >> elt_spark_job >> delete_cluster

  delete_cluster >> end
  start >> create_cluster >> get_object_list >> check_object >> [check_date, check_processed]
  check_processed >> check_spark_job_need
  check_spark_job_need >> [check_date, spark_job]
  spark_job >> delete_raw_data >> check_date
