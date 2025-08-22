import os
import json
import base64
from datetime import datetime
from flask import Flask, request
from google.cloud import bigquery
from google.cloud import storage
from jinja2 import Environment, FileSystemLoader

app = Flask(__name__)

try:
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
except Exception as e:
    print(f"Warning: Could not initialize GCP clients: {e}")
    bigquery_client = None
    storage_client = None

project_id = "plated-bee-468212-d2"
dataset_id = "sales_data"
view_id = "vw_regional_sales_summary"
bucket_name = "pipeline2_reports"


def query_sales_data(region):
    """Query BigQuery view for specific region data"""
    query = f"""
    SELECT 
        product_category,
        sales_region,
        sale_date,
        sale_amount
    FROM `{project_id}.{dataset_id}.{view_id}`
    WHERE sales_region = @region
    ORDER BY sale_date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("region", "STRING", region)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    return [dict(row) for row in results]


def generate_html_report(region, data):
    """Generate HTML report using Jinja2 template"""
    # Calculate summary statistics
    total_records = len(data)
    total_amount = sum(float(row['sale_amount']) for row in data) if data else 0
    generation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Load template
    template_loader = FileSystemLoader('.')
    template_env = Environment(loader=template_loader)
    template = template_env.get_template('report_template.html')

    # Render template with data
    html_content = template.render(
        region=region,
        data=data,
        total_records=total_records,
        total_amount=total_amount,
        generation_date=generation_date
    )

    return html_content


def upload_to_gcs(content, filename):
    """Upload HTML content to Google Cloud Storage"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(content, content_type='text/html')
    #html content is string

    return f"gs://{bucket_name}/{filename}"

def create_filename(region):
    """Create filename following the required convention"""
    clean_region = region.replace(" ", "-").replace("&", "and")
    date_str = datetime.now().strftime("%Y%m%d")
    return f"report-{clean_region}-{date_str}.html"


@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handle Pub/Sub messages and generate reports"""
    try:
        envelope = request.get_json()
        if not envelope:
            return "Bad Request: no Pub/Sub message received", 400

        if not isinstance(envelope, dict) or "message" not in envelope:
            return "Bad Request: invalid Pub/Sub message format", 400

        pubsub_message = envelope["message"]

        if "data" in pubsub_message:
            message_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
            message_json = json.loads(message_data)
        else:
            return "Bad Request: no data in Pub/Sub message", 400

        region = message_json.get("region")
        if not region:
            return "Bad Request: no region specified in message", 400
        print(f"Processing report request for region: {region}")

        data = query_sales_data(region)
        print(f"Found {len(data)} records for region {region}")

        html_content = generate_html_report(region, data)

        filename = create_filename(region)
        gcs_path = upload_to_gcs(html_content, filename)

        print(f"Report uploaded successfully: {gcs_path}")

        return f"Report generated successfully: {gcs_path}", 200

    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)