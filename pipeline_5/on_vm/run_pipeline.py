import subprocess
import sys
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_path="~/config.json"):
    with open(Path(config_path).expanduser()) as f:
        return json.load(f)

def run(cmd):
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)

def get_terraform_output(output_name):
    result = subprocess.run(
        f"cd ~/terraform && terraform output -raw {output_name}",
        shell=True, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()

def main():
    logger.info("Starting sentiment analysis pipeline")
    
    config = load_config()
    REGION = config['region']
    ZONE = config['zone']
    STAGING_BUCKET = config['staging_bucket']
    OUTPUT_BUCKET = config['output_bucket']
    DATASET_ID = config['dataset_id']
    CLUSTER_NAME = f"sentiment-job-{int(datetime.now().timestamp())}"
    
    logger.info(f"Cluster name: {CLUSTER_NAME}")
    
    logger.info("Applying terraform configuration")
    run("cd ~/terraform && terraform apply -auto-approve")
    
    SERVICE_ACCOUNT = config.get('service_account') or get_terraform_output("service_account_email")
    
    logger.info("Uploading scripts and data to GCS")
    run(f"gsutil -m cp ~/init_cluster.sh ~/scripts/sentiment_analysis_parquet.py gs://{STAGING_BUCKET}/scripts/")
    run(f"gsutil cp ~/data/Reviews.csv gs://{STAGING_BUCKET}/data/")
    
    logger.info("Creating Dataproc cluster")
    run(f"""gcloud dataproc clusters create {CLUSTER_NAME} \
        --region={REGION} \
        --service-account={SERVICE_ACCOUNT} \
        --scopes=https://www.googleapis.com/auth/cloud-platform \
        --max-idle=600s \
        --initialization-actions=gs://{STAGING_BUCKET}/scripts/init_cluster.sh \
        --autoscaling-policy=sentiment-analysis-autoscaling \
        --no-address \
        --network=dev-vpc""")
    
    logger.info("Submitting PySpark job")
    run(f"""gcloud dataproc jobs submit pyspark \
        gs://{STAGING_BUCKET}/scripts/sentiment_analysis_parquet.py \
        --cluster={CLUSTER_NAME} --region={REGION}""")
    
    logger.info("Deleting Dataproc cluster")
    run(f"gcloud dataproc clusters delete {CLUSTER_NAME} --region={REGION} --quiet")
    
    logger.info("Loading data into BigQuery")
    run(f"bq load --replace --source_format=PARQUET {DATASET_ID}.reviews_with_sentiment gs://{OUTPUT_BUCKET}/reviews_with_sentiment_parquet/*.parquet")
    run(f"bq load --replace --source_format=PARQUET {DATASET_ID}.product_sentiment_summary gs://{OUTPUT_BUCKET}/product_sentiment_summary_parquet/*.parquet")
    
    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
