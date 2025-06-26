from __future__ import annotations

import logging
import pendulum
import boto3

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)


S3_BUCKET = 'lab3dynamo1'
RAW_PREFIX = 'raw-data/'
ARCHIVE_PREFIX = 'archive/raw-data/'

# === HANDLER: On Failure Logging ===
def task_failure_alert(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    log.error(
        f" Task failed! DAG: {dag_id}, Task: {task_id}, "
        f"Run ID: {run_id}, Execution Date: {execution_date}"
    )

# === CHECK: If Data Already Processed ===
def check_if_processed():
    """
    Check if data has already been processed by checking if a corresponding
    file exists in the archive folder.
    """
    s3 = boto3.client('s3')
    prefix = RAW_PREFIX

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    for obj in response.get("Contents", []):
        file_key = obj["Key"]
        archive_key = file_key.replace(RAW_PREFIX, ARCHIVE_PREFIX)
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=archive_key)
            raise ValueError(f" File {file_key} has already been archived. Skipping.")
        except s3.exceptions.ClientError:
            log.info(f" File {file_key} is not archived yet. Proceeding.")
            return  # proceed if not archived

# === MOVE: Archive Raw File ===
def archive_raw_data():
    """
    Moves raw files from raw-data/ to archive/raw-data/ to avoid reprocessing.
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_PREFIX)

    for obj in response.get("Contents", []):
        source_key = obj["Key"]
        dest_key = source_key.replace(RAW_PREFIX, ARCHIVE_PREFIX)

        # Copy the file
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': source_key},
            Key=dest_key
        )
        log.info(f" Archived {source_key} to {dest_key}")

        # Delete the original
        s3.delete_object(Bucket=S3_BUCKET, Key=source_key)
        log.info(f" Removed original file {source_key} after archiving")

# === DAG Configuration ===
default_args = {
    'owner': 'Richard Amasah',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
    'on_failure_callback': task_failure_alert,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
}

with DAG(
    dag_id='music_stream_kpi_pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['stream', 'music', 'glue', 'etl'],
) as dag:

    start = DummyOperator(task_id='start_pipeline')

    wait_for_stream = SqsSensor(
        task_id='wait_for_stream_event',
        sqs_queue='https://sqs.eu-north-1.amazonaws.com/992382846559/music-data-trigger-queue',
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    check_processed = PythonOperator(
        task_id='check_if_data_processed',
        python_callable=check_if_processed
    )

    data_validation = GlueJobOperator(
        task_id='data_validation_job',
        job_name='Data-validation-job',
        aws_conn_id='aws_default',
        region_name='us-east-1'
    )

    validation_failed = DummyOperator(
        task_id='validation_failed_path',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    compute_kpis = GlueJobOperator(
        task_id='compute_kpis_job',
        job_name='kpi_computation_job',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_to_dynamo = GlueJobOperator(
        task_id='load_to_dynamo_job',
        job_name='load-to-dynamo',
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )

    archive_files = PythonOperator(
        task_id='archive_raw_data_files',
        python_callable=archive_raw_data,
    )

    end = DummyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # DAG Flow
    start >> wait_for_stream >> check_processed >> data_validation

    data_validation >> validation_failed >> end
    data_validation >> compute_kpis >> load_to_dynamo >> archive_files >> end
