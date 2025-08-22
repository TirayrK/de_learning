# Serverless Reporting Pipeline

A fully serverless, event-driven data pipeline on Google Cloud Platform that generates on-demand sales reports.

## Overview

This solution creates an automated reporting system that:
- Receives Pub/Sub messages requesting reports for specific regions
- Queries BigQuery for sales data
- Generates formatted HTML reports
- Saves reports to Google Cloud Storage

## Architecture

```
Pub/Sub Topic → Cloud Run Service → BigQuery → HTML Report → Cloud Storage
```

**Components:**
- **Data Source:** BigQuery View (`vw_regional_sales_summary`)
- **Processing:** Cloud Run (Python Flask application)
- **Trigger:** Pub/Sub Topic (`sales-reports`)
- **Destination:** Google Cloud Storage bucket
- **Language:** Python with Flask, Jinja2, and Google Cloud SDKs

## Project Structure

```
serverless-reporting/
├── main.py                 # Main Flask application
├── report_template.html            # Jinja2 HTML template
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container configuration
└── README.md            # This documentation
```

## Deployment Instructions

### Prerequisites
- Google Cloud SDK installed and configured
- Docker installed
- Project with BigQuery data set up

### 1. Build and Deploy

```bash
# Build Docker image
docker build -t pipeline2_reporting .

# Tag for Google Container Registry
docker tag pipeline2_reporting gcr.io/YOUR-PROJECT-ID/pipeline2_reporting

# Push to registry
docker push gcr.io/YOUR-PROJECT-ID/pipeline2_reporting

# Deploy to Cloud Run
gcloud run deploy pipeline2-reporting \
  --image gcr.io/YOUR-PROJECT-ID/pipeline2_reporting \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080
```

### 2. Set Up Pub/Sub

```bash
# Create topic
gcloud pubsub topics create sales-reports

# Create subscription (replace with your actual Cloud Run URL)
gcloud pubsub subscriptions create sales-reports-sub \
  --topic=sales-reports \
  --push-endpoint="YOUR-CLOUD-RUN-URL/"
```

### 3. Create Storage Bucket

```bash
# Create bucket for reports
gsutil mb gs://YOUR-BUCKET-NAME
```

## Usage

### Trigger Report Generation

To generate a report for a specific region:

```bash
gcloud pubsub topics publish sales-reports --message='{"region": "North America"}'
```

Available regions:
- `"North America"`
- `"Europe"`
- `"Asia"`

### Report Output

Reports are saved to Cloud Storage with the naming convention:
`report-{region}-{YYYYMMDD}.html`

Example: `report-North-America-20250821.html`

## Features

- **Serverless Architecture:** Fully managed, scales automatically
- **Event-Driven:** Triggered by Pub/Sub messages
- **Secure:** Uses parameterized BigQuery queries
- **Professional Reports:** Styled HTML with summary statistics
- **Error Handling:** Graceful handling of missing data and errors
- **Production Ready:** Containerized with proper logging

## Sample Report

The generated reports include:
- Region name and generation timestamp
- Summary statistics (total records, total sales amount)
- Detailed data table with product categories, dates, and amounts
- Professional styling and formatting

## Technical Implementation

- **Flask:** Web framework for handling HTTP requests
- **Jinja2:** Template engine for HTML report generation
- **Google Cloud BigQuery:** Data source with parameterized queries
- **Google Cloud Storage:** Report storage destination
- **Docker:** Containerization for Cloud Run deployment
- **Gunicorn:** Production WSGI server

## Security Features

- Parameterized BigQuery queries prevent SQL injection
- Cloud Run automatic authentication with Google Cloud services
- Input validation for Pub/Sub messages
- Error handling without exposing sensitive information