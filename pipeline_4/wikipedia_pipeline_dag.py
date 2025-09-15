"""
Wikipedia Hourly Pageviews Analytics Pipeline
Complete Airflow DAG for processing Wikipedia pageview data daily
"""

import logging
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define default arguments for all tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

PROJECT_ID = "project4-471620"
REGION = "us-central1"
CLUSTER_NAME = "wikipedia-processing-cluster"
LANDING_BUCKET = "project4-landing-zone"
PROCESSED_BUCKET = "project4-processed-zone"
DATAPROC_STAGING_BUCKET = "project4-dataproc-staging"
PYSPARK_SCRIPT_URI = "gs://us-central1-my-composer-82fad1a3-bucket/scripts/process_wiki_views.py"

# Dataproc cluster configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "software_config": {
        "image_version": "2.0-debian10"
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 5000}
    }
}

def download_wikipedia_files(**context):
    """
    Download 24 hourly Wikipedia pageview files for the given date
    """
    from datetime import datetime, timedelta

    # Check if manual date provided via DAG config
    dag_run_conf = context.get('dag_run').conf or {}
    manual_date = dag_run_conf.get('target_date')

    if manual_date:
        # Use manually specified date
        data_date_str = manual_date
        data_date_nodash = manual_date.replace('-', '')
        logging.info(f"Using manual target date: {manual_date}")
    else:
        # Get today's date and calculate yesterday
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        data_date_str = yesterday.strftime('%Y-%m-%d')
        data_date_nodash = yesterday.strftime('%Y%m%d')
        logging.info(f"Today is {today.strftime('%Y-%m-%d')}, downloading yesterday's data: {data_date_str}")

    year = data_date_str[:4]
    month = data_date_str[:7]

    logging.info(f"Downloading data for: {data_date_str}")
    logging.info(f"Saving to folder: {data_date_str}")

    successful_downloads = 0
    failed_downloads = []

    for hour in range(24):
        hour_str = f"{hour:02d}"
        source_url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{month}/pageviews-{data_date_nodash}-{hour_str}0000.gz"
        dest_path = f"gs://{LANDING_BUCKET}/{data_date_str}/pageviews-{data_date_nodash}-{hour_str}0000.gz"

        cmd = [
            "bash", "-c",
            f'curl -s "{source_url}" | gsutil cp - "{dest_path}"'
        ]

        try:
            logging.info(f"Downloading hour {hour_str}: {source_url}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logging.info(f"Successfully downloaded hour {hour_str}")
            successful_downloads += 1
        except Exception as e:
            logging.error(f"Failed to download hour {hour_str}: {e}")
            failed_downloads.append(hour_str)

    logging.info(f"Download summary: {successful_downloads}/24 files successful")

    if failed_downloads:
        logging.error(f"Failed downloads for hours: {failed_downloads}")
        if len(failed_downloads) > 6:
            raise Exception(f"Too many failed downloads: {len(failed_downloads)}/24")
        else:
            logging.warning(f"Proceeding with {successful_downloads} files")

    if successful_downloads == 0:
        raise Exception("No files downloaded successfully")

    logging.info(f"Download completed successfully for data date: {data_date_str}")


# Define the DAG
dag = DAG(
    'wikipedia_pageviews_pipeline',
    default_args=default_args,
    description='Daily Wikipedia pageviews analysis pipeline',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['wikipedia', 'pageviews', 'analytics']
)

# Task definitions
start_task = DummyOperator(
    task_id='start',
    dag=dag
)

download_data_task = PythonOperator( #fixme why python operator if using bash
    task_id='download_wikipedia_data',
    python_callable=download_wikipedia_files,
    provide_context=True,
    dag=dag
)

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag
)

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    project_id=PROJECT_ID,
    region=REGION,  # Keep the original region parameter
    job={
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                f"gs://{LANDING_BUCKET}",
                f"gs://{PROCESSED_BUCKET}",
                "{{ macros.ds_add(ds, -1) }}"
            ]
        }
    },
    dag=dag
)

load_parquet_to_bigquery = GCSToBigQueryOperator(
    task_id='load_parquet_to_bigquery',
    bucket=PROCESSED_BUCKET,
    source_objects=['{{ macros.ds_add(ds, -1) }}/*.parquet'],
    destination_project_dataset_table=f'{PROJECT_ID}.wikipedia_analytics.top_en_articles_daily',
    source_format='PARQUET',
    write_disposition='WRITE_APPEND',
    autodetect=True,  # Let it detect the schema automatically
    dag=dag
)

delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',
    dag=dag
)

end_task = DummyOperator(
    task_id='end',
    dag=dag
)

# Task dependencies
start_task >> download_data_task >> create_dataproc_cluster >> submit_pyspark_job
submit_pyspark_job >> [load_parquet_to_bigquery, delete_dataproc_cluster]
[load_parquet_to_bigquery, delete_dataproc_cluster] >> end_task