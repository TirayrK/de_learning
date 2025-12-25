import json
import os
from airflow.models import Variable
from modules.data_validator import DataValidator


class PipelineConfig:
    """Pipeline configuration from Airflow Variables"""

    def __init__(self):
        self.project_id = Variable.get('PROJECT_ID', default_var=None)
        self.region = Variable.get('REGION', default_var=None)
        self.zone = Variable.get('ZONE', default_var=None)
        self.staging_bucket = Variable.get('STAGING_BUCKET', default_var=None)
        self.output_bucket = Variable.get('OUTPUT_BUCKET', default_var=None)
        self.dataset_id = Variable.get('DATASET_ID', default_var=None)
        self.network = Variable.get('NETWORK', default_var=None)
        self.nltk_data_path = f"{self.staging_bucket}/requirements/nltk_data"
        self.cluster_name = "sentiment-cluster"
        self.data_prefix = "data/unprocessed/"
        self.processed_prefix = "data/processed/"
        self.script_file = "scripts/sentiment_analysis.py"
        self.dependencies_zip = "requirements/dependencies.zip"


def load_reviews_schema():
    """Load reviews table schema from JSON"""
    schema_path = os.path.join(os.path.dirname(__file__), '../config/reviews_schema.json')
    with open(schema_path, 'r') as f:
        return json.load(f)


def load_summary_schema():
    """Load summary table schema from JSON"""
    schema_path = os.path.join(os.path.dirname(__file__), '../config/summary_schema.json')
    with open(schema_path, 'r') as f:
        return json.load(f)


def validate_input_callable(**context):
    """Validate input CSV files"""
    config = PipelineConfig()
    validator = DataValidator(
        staging_bucket=config.staging_bucket,
        output_bucket=config.output_bucket,
        data_prefix=config.data_prefix,
        processed_prefix=config.processed_prefix
    )
    return validator.validate_input_csv()


def validate_output_callable(**context):
    """Validate output parquet files"""
    config = PipelineConfig()
    validator = DataValidator(
        staging_bucket=config.staging_bucket,
        output_bucket=config.output_bucket,
        data_prefix=config.data_prefix,
        processed_prefix=config.processed_prefix
    )
    return validator.validate_output_parquet()


def load_cluster_config(project_id, zone, network):
    """Load cluster config from JSON and inject dynamic values"""
    config_path = os.path.join(os.path.dirname(__file__), '../config/cluster_config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    config["gce_cluster_config"]["zone_uri"] = zone
    config["gce_cluster_config"]["network_uri"] = f"projects/{project_id}/global/networks/{network}"
    config["gce_cluster_config"]["service_account"] = f"dataproc-sentiment-sa@{project_id}.iam.gserviceaccount.com"

    return config


def get_pyspark_job_config(**context):
    """Generate PySpark job config at runtime with unprocessed files"""
    config = PipelineConfig()
    validator = DataValidator(
        staging_bucket=config.staging_bucket,
        output_bucket=config.output_bucket,
        data_prefix=config.data_prefix,
        processed_prefix=config.processed_prefix
    )
    files = validator.get_unprocessed_files()
    input_paths = [f"gs://{config.staging_bucket}/{f}" for f in files]

    return {
        "reference": {"project_id": config.project_id},
        "placement": {"cluster_name": config.cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{config.staging_bucket}/{config.script_file}",
            "args": [
                "--input-paths", ",".join(input_paths),
                "--reviews-output", f"gs://{config.output_bucket}/reviews_with_sentiment_parquet/",
                "--summary-output", f"gs://{config.output_bucket}/product_sentiment_summary_parquet/",
                "--nltk-path", f"gs://{config.nltk_data_path}"
            ],
            "python_file_uris": [
                f"gs://{config.staging_bucket}/{config.dependencies_zip}"
            ]
        }
    }