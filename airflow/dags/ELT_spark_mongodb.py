import requests

from config import config

from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

def check_date(**context):
  if "{{ ds }}" == datetime.utcnow().strftime('%Y-%m-%d'):
    return 'create_cluster'
  else:
    return 'end'

def check_version(**context):
  game_version = '.'.join(requests.get('https://ddragon.leagueoflegends.com/api/versions.json').json()[0].split('.')[:2])
  date_30d = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
  context['task_instance'].xcom_push(key='game_version',value=game_version)
  context['task_instance'].xcom_push(key='start_date',value=date_30d)

scripts = ['champion_counter.py','champion_synergy.py', 'champion_tier.py', 'processed_to_statistic.py']
# counter, synergy 최근 30일, champion_tier game_version,

with DAG(
    dag_id='raw_to_processed_gcs',
    start_date=datetime(2023, 9, 8),
    schedule=None,
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

  check_date = BranchPythonOperator(
    task_id = 'check_date',
    python_callable = check_date,
  )

  create_cluster = DummyOperator(task_id='create_cluster')
  delete_cluster =  DummyOperator(task_id='delete_cluster', trigger_rule='all_done')
  check_version = PythonOperator(
    task_id='check_version',
    python_callable=check_version
  )

  create_cluster >> check_version

  for script in scripts:
    pyspark_job = config.processed_to_mongodb
    if script in ['champion_counter.py','champion_synergy.py']:
      pyspark_job['pyspark_job']['args'].append("--execution_date={{ ti.xcom_pull(key='start_date') }}")
    elif script == 'champion_tier.py':
      pyspark_job['pyspark_job']['args'].append("--game_version={{ ti.xcom_pull(key='game_version') }}")
    else:
      pyspark_job['pyspark_job']['args'].append("--execution_date={{ ds }}")
    pyspark_job['pyspark_job']['main_python_file_uri'].append(f'gs://summoner-match/pyspark/{script}')

    spark_job = DataprocSubmitJobOperator(
      task_id = f'{script}',
      region = 'asia-northeast3',
      job = pyspark_job,
    )

    check_version >> spark_job >> delete_cluster

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


