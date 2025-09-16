# Wikipedia Analytics Pipeline
**Automated Data Processing with Google Cloud Platform**

*Author: Monica Ghavalyan*  
*Date: September 2025*

---

## Project Overview

This project creates an automated system that analyzes Wikipedia traffic data every day. It downloads hourly pageview files from Wikipedia, processes them to find the top 10 most popular English articles, and stores the results for analysis. The entire process runs automatically using Google Cloud services.

### What the Pipeline Does
1. Downloads 24 hourly files from Wikipedia (one for each hour of the previous day)
2. Processes millions of pageview records using distributed computing
3. Filters for English Wikipedia articles only
4. Finds the top 10 most viewed articles
5. Stores results in a database for querying and analysis

### Key Benefits
- Fully automated - runs daily without manual intervention
- Cost-effective - only uses computing resources when processing data
- Scalable - can handle large amounts of data efficiently
- Reliable - includes error handling and retry logic

---

## Technical Architecture

### System Components
```
Wikipedia API → Cloud Storage → Dataproc (Spark) → BigQuery
                     ↑              ↑
              Cloud Composer (Airflow)
```

**Cloud Composer**: Orchestrates the entire workflow using Apache Airflow. Creates schedules, manages task dependencies, and handles error recovery.

**Cloud Storage**: Acts as data lake with three buckets:
- Landing zone for raw Wikipedia files
- Processed zone for transformed data  
- Staging area for temporary cluster files

**Dataproc**: Managed Apache Spark service that creates temporary computing clusters to process large datasets in parallel.

**BigQuery**: Data warehouse that stores final results for analysis and querying.

---

## Implementation Details

### Data Source
Wikipedia publishes hourly pageview data at:
`https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz`

Each file contains space-separated records:
- `domain_code`: Language identifier (en, en.m, fr, etc.)
- `page_title`: Article name
- `count_views`: Number of views in that hour
- `total_response_size`: Response size in bytes

### Processing Logic (PySpark Script)

The `process_wiki_views.py` script handles the data transformation:

1. **Schema Loading**: Reads data schema from external schema.json configuration file
2. **File Reading**: Loads all 24 hourly files simultaneously using Spark's distributed reading
3. **Filtering**: Keeps only English Wikipedia records (`en` and `en.m` domains)
4. **Aggregation**: Groups records by article title and sums view counts across all hours
5. **Ranking**: Sorts articles by total views and selects top 10
6. **Output**: Saves results as Parquet files with processing date added

Key features:
- Handles compressed .gz files automatically
- Validates data quality (checks for empty files)
- Uses Python logging framework for production-ready logging
- Uses efficient columnar storage format

### Workflow Orchestration (Airflow DAG)

The `wikipedia_pipeline_dag.py` manages the complete workflow:

**Task 1: Download Data**
- Uses Python function with shell commands for efficiency
- Downloads 24 files in parallel using curl and gsutil
- Streams data directly to Cloud Storage without local storage

**Task 2: Create Cluster**
- Loads cluster configuration from external cluster_config.json file
- Provisions ephemeral Dataproc cluster (1 master + 2 workers)
- Uses n1-standard-2 machines (2 vCPU, 7.5GB RAM each)
- Configures automatic deletion after 5000 seconds idle time

**Task 3: Process Data**
- Submits PySpark job to the cluster
- Passes input/output paths and target date as parameters

**Task 4: Load to BigQuery**
- Transfers Parquet files from Cloud Storage to BigQuery
- Appends new records to existing table
- Automatically detects schema from Parquet files

**Task 5: Cleanup**
- Deletes the temporary cluster to save costs
- Runs even if previous tasks fail (trigger_rule='all_done')
- Releases all computing resources
---

## Setup and Deployment

### Prerequisites
```bash
# Enable required Google Cloud APIs
gcloud services enable composer.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### Infrastructure Setup

**1. Create Storage Buckets**
```bash
gsutil mb gs://project4-landing-zone      # Raw data
gsutil mb gs://project4-processed-zone    # Processed results
gsutil mb gs://project4-dataproc-staging  # Cluster temp files
```

**2. Create BigQuery Resources**
```bash
# Create dataset
bq mk --dataset wikipedia_analytics

# Create results table
bq mk --table wikipedia_analytics.top_en_articles_daily \
  article_title:STRING,total_views:INTEGER,processing_date:DATE
```

**3. Deploy Cloud Composer Environment**
```bash
gcloud composer environments create my-composer \
  --location=us-central1 \
  --image-version=composer-2.14.1-airflow-2.10.5 \
  --python-version=3 \
  --node-count=3 \
  --machine-type=n1-standard-1
```

### Code Deployment

**1. Upload Processing Script**
```bash
# Get Composer bucket path
COMPOSER_BUCKET=$(gcloud composer environments describe my-composer \
  --location=us-central1 --format="get(config.dagGcsPrefix)")

# Upload PySpark script
gsutil cp process_wiki_views.py ${COMPOSER_BUCKET%/dags}/scripts/

# Create schemas folder and upload configuration files
gsutil mkdir ${COMPOSER_BUCKET%/dags}/schemas/
gsutil cp schema.json ${COMPOSER_BUCKET%/dags}/schemas/
gsutil cp cluster_config.json ${COMPOSER_BUCKET%/dags}/schemas/
```

**2. Upload Workflow Definition**
```bash
# Upload DAG file
gsutil cp wikipedia_pipeline_dag.py ${COMPOSER_BUCKET}/
```

### Running the Pipeline

**Manual Trigger**
```bash
gcloud composer environments run my-composer \
  --location=us-central1 \
  dags trigger wikipedia_pageviews_pipeline \
  --conf '{"target_date": "2025-09-13"}'
```

**Scheduled Execution**
- Runs automatically daily at 6 PM UTC
- Processes previous day's data when Wikipedia files are available

---

## Monitoring and Validation

### Data Validation
```sql
-- Check latest results
SELECT processing_date, article_title, total_views
FROM wikipedia_analytics.top_en_articles_daily
WHERE processing_date = CURRENT_DATE() - 1
ORDER BY total_views DESC;

```

### Troubleshooting

**Common Issues:**
1. **Download failures**: Wikipedia files temporarily unavailable
2. **Cluster creation**: Quota limits or regional capacity
4. **BigQuery loads**: Schema mismatches

**Monitoring Tools:**
- Airflow UI for workflow status and logs
- Dataproc console for cluster and job monitoring
- Cloud Logging for detailed error tracking

---

## Results

### Sample Output
| Article Title | Total Views | Processing Date |
|---------------|-------------|-----------------|
| Main_Page | 5,921,284 | 2025-09-13 |
| Charlie_Kirk | 2,118,390 | 2025-09-13 |
| Groypers | 898,995 | 2025-09-13 |
