from __future__ import annotations

import logging
import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

def task_failure_alert(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    log.error(
        f"🚨 Task failed! DAG: {dag_id}, Task: {task_id}, "
        f"Run ID: {run_id}, Execution Date: {execution_date}"
    )


# Define default arguments for the DAG
default_args = {
    'owner': 'Richard Amasah',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
    'on_failure_callback': task_failure_alert,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
}

# Instantiate the DAG
with DAG(
    dag_id='music_stream_kpi_pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['stream', 'music', 'glue', 'etl'],
) as dag:

    # --------------------------------------------------------------------------------------
    # 1. Pipeline Start Marker
    # --------------------------------------------------------------------------------------
    start = DummyOperator(
        task_id='start_pipeline',
    )
    log.info("DAG 'music_stream_kpi_pipeline' started.")

    # --------------------------------------------------------------------------------------
    # 2. SQS Sensor - Wait for New Data Event
    # --------------------------------------------------------------------------------------
    wait_for_stream = SqsSensor(
        task_id='wait_for_stream_event',
        sqs_queue='https://sqs.eu-north-1.amazonaws.com/992382846559/music-data-trigger-queue',
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )
    log.info("Waiting for new data stream event via SQS sensor.")

    # --------------------------------------------------------------------------------------
    # 3. AWS Glue Job 1: Data Validation and Initial Transformation
    # --------------------------------------------------------------------------------------
    data_validation = GlueJobOperator(
        task_id='data_validation_job',
        job_name='Data-validation-job',
        aws_conn_id='aws_default',
        region_name='us-east-1'
    )
    log.info("Triggered 'Data-validation-job' Glue job.")

    # --------------------------------------------------------------------------------------
    # 4. Error Handling Branch: Validation Failed
    # --------------------------------------------------------------------------------------
    validation_failed = DummyOperator(
        task_id='validation_failed_path',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    log.info("Defined path for 'validation_failed'.")

    # --------------------------------------------------------------------------------------
    # 5. AWS Glue Job 2: KPI Computation
    # --------------------------------------------------------------------------------------
    compute_kpis = GlueJobOperator(
        task_id='compute_kpis_job',
        job_name='kpi_computation_job',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    log.info("Triggered 'kpi_computation_job' Glue job.")

    # --------------------------------------------------------------------------------------
    # 6. AWS Glue Job 3: Load KPIs to DynamoDB
    # --------------------------------------------------------------------------------------
    load_to_dynamo = GlueJobOperator(
        task_id='load_to_dynamo_job',
        job_name='load-to-dynamo',
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )
    log.info("Triggered 'load-to-dynamo' Glue job.")

    # --------------------------------------------------------------------------------------
    # 7. Pipeline End Marker
    # --------------------------------------------------------------------------------------
    end = DummyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    log.info("Defined end point for the pipeline.")

    # --------------------------------------------------------------------------------------
    # 8. Define Task Dependencies (DAG Flow)
    # --------------------------------------------------------------------------------------
    start >> wait_for_stream >> data_validation

    data_validation >> validation_failed >> end
    data_validation >> compute_kpis >> load_to_dynamo >> end

    log.info("DAG flow dependencies established.")