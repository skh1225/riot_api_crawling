import copy
from config import config

from airflow import DAG

from datetime import datetime
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator


match_types = ['solo','flex','aram']

with DAG(
    dag_id='gcs_to_mongo_v1',
    start_date=datetime(2023, 12, 26),
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

  create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    **config.cluster_config
  )

  delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    region=config.cluster_config['region'],
    cluster_name=config.cluster_config['cluster_name'],
    project_id=config.cluster_config['project_id'],
    trigger_rule='all_done'
  )

  ETL_conf, ELT_conf = {}, {}

  for match_type in match_types:
    ETL_conf[match_type] = copy.deepcopy(config.raw_to_processed)
    ELT_conf[match_type] = copy.deepcopy(config.processed_to_mongodb)

    ETL_conf[match_type]['pyspark_job']['args'].append("--execution_date={{ ds }}")
    ETL_conf[match_type]['pyspark_job']['main_python_file_uri'] = f"gs://gnimty_bucket/pyspark/{match_type}_raw.py"
    ELT_conf[match_type]['pyspark_job']['main_python_file_uri'] = f"gs://gnimty_bucket/pyspark/{match_type}_statistic.py"


    ETL_job = DataprocSubmitJobOperator(
        task_id = f'ETL_job_{match_type}',
        region = 'asia-northeast3',
        job = ETL_conf[match_type],
    )

    ELT_job = DataprocSubmitJobOperator(
        task_id = f'ELT_job_{match_type}',
        region = 'asia-northeast3',
        job = ELT_conf[match_type],
    )

    create_cluster >> ETL_job >> ELT_job >> delete_cluster


  end = DummyOperator(task_id='end')

  start >> create_cluster
  delete_cluster >> end
