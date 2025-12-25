"""
Sentiment Analysis Pipeline DAG
Event-driven orchestration with Dataproc, BigQuery, and Slack alerting via callbacks
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from modules import dag_helpers as hp
from modules import slack_config as sc


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': [sc.get_task_failure_notification()],
    'on_success_callback': [sc.get_task_success_notification()]
}

with DAG(
    dag_id='sentiment_pipeline',
    default_args=default_args,
    description='Hourly sentiment analysis with 30-min file wait',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sentiment-analysis', 'dataproc', 'bigquery'],
    on_success_callback=[sc.get_dag_success_notification()],
    on_failure_callback=[sc.get_dag_failure_notification()]
) as dag:

    pipeline_config = hp.PipelineConfig()

    wait_for_files = GCSObjectExistenceSensor(
        task_id='wait_for_new_files',
        bucket=pipeline_config.staging_bucket,
        object=f'{pipeline_config.data_prefix}*.csv',
        use_glob=True,
        mode='poke',
        poke_interval=60,
        timeout=1800,
        soft_fail=True,
    )

    create_reviews_table = BigQueryCreateTableOperator(
        task_id='create_reviews_table',
        dataset_id=pipeline_config.dataset_id,
        table_id='reviews_with_sentiment',
        project_id=pipeline_config.project_id,
        table_resource={
            "schema": {
                "fields": hp.load_reviews_schema()
            }
        },
        if_exists='log',
    )

    create_summary_table = BigQueryCreateTableOperator(
        task_id='create_summary_table',
        dataset_id=pipeline_config.dataset_id,
        table_id='product_sentiment_summary',
        project_id=pipeline_config.project_id,
        table_resource={
            "schema": {
                "fields": hp.load_summary_schema()
            }
        },
        if_exists='log',
    )

    validate_input = PythonOperator(
        task_id='validate_input_data',
        python_callable=hp.validate_input_callable
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=pipeline_config.project_id,
        region=pipeline_config.region,
        cluster_name=pipeline_config.cluster_name,
        cluster_config=lambda **context: hp.load_cluster_config(
            pipeline_config.project_id,
            pipeline_config.zone,
            pipeline_config.network
        ),
    )

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=pipeline_config.project_id,
        region=pipeline_config.region,
        job=hp.get_pyspark_job_config,
    )

    validate_output = PythonOperator(
        task_id='validate_output_data',
        python_callable=hp.validate_output_callable
    )

    load_reviews = BigQueryInsertJobOperator(
        task_id='load_reviews_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{pipeline_config.output_bucket}/reviews_with_sentiment_parquet/*.parquet"],
                "destinationTable": {
                    "projectId": pipeline_config.project_id,
                    "datasetId": pipeline_config.dataset_id,
                    "tableId": "reviews_with_sentiment"
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND"
            }
        },
        project_id=pipeline_config.project_id,
    )

    load_summary = BigQueryInsertJobOperator(
        task_id='load_summary_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{pipeline_config.output_bucket}/product_sentiment_summary_parquet/*.parquet"],
                "destinationTable": {
                    "projectId": pipeline_config.project_id,
                    "datasetId": pipeline_config.dataset_id,
                    "tableId": "product_sentiment_summary"
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND"
            }
        },
        project_id=pipeline_config.project_id,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=pipeline_config.project_id,
        region=pipeline_config.region,
        cluster_name=pipeline_config.cluster_name,
        trigger_rule='none_skipped'
    )

    cleanup_files = GCSToGCSOperator(
        task_id='move_files_to_processed',
        source_bucket=pipeline_config.staging_bucket,
        source_object='data/unprocessed/*.csv',
        destination_bucket=pipeline_config.staging_bucket,
        destination_object='data/processed/',
        move_object=True,
        trigger_rule='all_success'
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    wait_for_files >> [create_reviews_table, create_summary_table] >> validate_input >> create_cluster
    create_cluster >> submit_job >> validate_output >> [load_reviews, load_summary]
    submit_job >> delete_cluster
    [load_reviews, load_summary, delete_cluster] >> cleanup_files >> end