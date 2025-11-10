# Amazon Reviews Sentiment Analysis Pipeline

A cloud-native data engineering solution that migrates sentiment analysis workloads from on-premises Docker infrastructure to Google Cloud Platform using Infrastructure as Code.

## Overview

This project demonstrates migration from a simulated on-premises environment to a fully managed cloud solution that:
- Processes 568,454 Amazon food reviews using Apache Spark
- Performs sentiment analysis with NLTK's VADER algorithm
- Generates detailed review-level and product-level aggregations
- Stores results in BigQuery for analytics
- Automates infrastructure provisioning with Terraform

## Architecture

### Pre-Migration (Phase 1)
```
Local VM → Docker Compose → Spark Cluster → CSV Output
```

### Post-Migration (Phase 2)
```
Terraform → Cloud Storage → Dataproc (Ephemeral) → Parquet → BigQuery
```

### Components
- **Data Source:** Amazon Fine Food Reviews dataset (568,454 records, ~300MB)
- **Processing:** Google Cloud Dataproc with autoscaling (2-10 workers)
- **Storage:** Cloud Storage (staging and output buckets)
- **Analytics:** BigQuery (two tables with complete schemas)
- **Orchestration:** Python script with Terraform integration

## Project Structure
```
~/
├── config.json
├── init_cluster.sh
├── run_pipeline.py
├── data/
│   └── Reviews.csv
├── scripts/
│   ├── sentiment_analysis.py
│   └── sentiment_analysis_parquet.py
├── docker/
│   └── docker-compose.yml
└── terraform/
    ├── provider.tf
    ├── backend.tf
    ├── variables.tf
    ├── terraform.tfvars
    ├── storage.tf
    ├── bigquery.tf
    ├── dataproc.tf
    ├── iam.tf
    └── outputs.tf
```

## Prerequisites

- Google Cloud SDK installed and configured
- Docker and Docker Compose installed
- Terraform installed
- Amazon Reviews dataset downloaded

## Deployment Instructions

### Phase 1: Local Environment Setup

#### 1. Install Dependencies
```bash
# Install Docker
sudo apt update && sudo apt upgrade -y
sudo apt install -y docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER

# Install Terraform
sudo snap install terraform --classic
```

#### 2. Set Up Local Spark Cluster
```bash
# Create project directories
mkdir -p data scripts output docker

# Start Docker containers
cd docker
docker compose up -d

# Install Python dependencies in containers
docker exec -it spark-master bash
apt-get update && apt-get install -y python3-pip
pip3 install nltk textblob
exit
```

#### 3. Run Local Sentiment Analysis
```bash
# Submit Spark job
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-scripts/sentiment_analysis.py
```

### Phase 2: Cloud Migration

#### 1. Set Up GCP Authentication
```bash
gcloud auth login
gcloud config set project YOUR-PROJECT-ID
gcloud auth application-default login
```

#### 2. Create Terraform State Bucket
```bash
gsutil mb -l us-central1 gs://YOUR-PROJECT-ID-terraform-state
gsutil versioning set on gs://YOUR-PROJECT-ID-terraform-state
```

#### 3. Deploy Infrastructure
```bash
cd ~/terraform
terraform init
terraform plan
terraform apply
```

#### 4. Execute Pipeline
```bash
cd ~
python3 run_pipeline.py
```

## Usage

### Generate Sentiment Analysis Reports

Execute the complete pipeline with a single command:
```bash
python3 run_pipeline.py
```

The pipeline automatically:
1. Applies Terraform to ensure infrastructure is current
2. Uploads scripts and data to Cloud Storage
3. Creates ephemeral Dataproc cluster with autoscaling
4. Processes 568,454 reviews with sentiment analysis
5. Writes Parquet results to Cloud Storage
6. Loads data into BigQuery
7. Deletes cluster to minimize costs

**Execution time:** 15-20 minutes

## Features

- **Infrastructure as Code:** Complete infrastructure defined in Terraform
- **Ephemeral Clusters:** Cost-optimized with automatic creation and deletion
- **Autoscaling:** Dynamic worker scaling (2-10 instances) based on workload
- **Parquet Optimization:** 70% storage cost reduction vs CSV
- **Service Account Security:** Least privilege IAM permissions
- **Automated Orchestration:** Single command deploys entire pipeline

## Sentiment Analysis Methodology

- **Algorithm:** NLTK's VADER Sentiment Intensity Analyzer
- **Score Range:** -1 (most negative) to +1 (most positive)
- **Classification:**
  - Very Dissatisfied: ≤ -0.7
  - Somewhat Dissatisfied: -0.7 to -0.1
  - Neutral: -0.1 to 0.1
  - Somewhat Satisfied: 0.1 to 0.7
  - Very Satisfied: > 0.7

## Output Tables

**reviews_with_sentiment:** 568,454 rows with original review data plus sentiment_score and satisfaction_level

**product_sentiment_summary:** ~74,000 rows with product-level aggregations, averages, and distribution percentages

## Technical Stack

- **Apache Spark 3.5.0** - Distributed data processing
- **PySpark** - Python API for Spark
- **NLTK VADER** - Sentiment analysis
- **Terraform** - Infrastructure provisioning
- **Google Cloud Dataproc** - Managed Spark clusters
- **Google BigQuery** - Serverless data warehouse
- **Google Cloud Storage** - Object storage
- **Docker Compose** - Local development environment

## Security Features

- Service account with minimal required permissions
- Separate IAM roles for storage and BigQuery access
- Remote Terraform state with versioning
- Parameterized configurations

## Troubleshooting

### Docker permission errors
```bash
sudo systemctl restart docker
```

### Terraform state conflicts
```bash
terraform force-unlock LOCK-ID
```

### Dataproc cluster failures
- Verify service account permissions
- Check organization policies
- Confirm machine types are approved

## Future Enhancements

- Migrate orchestration to Cloud Composer for robust workflow scheduling
- Add BigQuery table partitioning and clustering for improved query performance
- Implement further code and cost optimizations
