# 1. Build Docker image from your code
docker build -t pipeline2_reporting .

# 2. Set up Docker authentication with Google Container Registry
gcloud auth configure-docker

# 3. Tag image with Google Container Registry format
docker tag pipeline2_reporting gcr.io/plated-bee-468212-d2/pipeline2_reporting

# 4. Upload image to Google Container Registry
docker push gcr.io/plated-bee-468212-d2/pipeline2_reporting

# 5. Deploy containerized service to Cloud Run
gcloud run deploy pipeline2-reporting \
  --image gcr.io/plated-bee-468212-d2/pipeline2_reporting \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080

# 6. Get the deployed service URL
gcloud run services describe pipeline2-reporting --region=us-central1 --format="value(status.url)"

# 7. Create Cloud Storage bucket for reports
gsutil mb gs://pipeline2_reports

# 8. Create Pub/Sub topic for triggering reports
gcloud pubsub topics create sales-reports

# 9. Create Pub/Sub subscription that sends messages to Cloud Run
gcloud pubsub subscriptions create sales-reports-sub \
  --topic=sales-reports \
  --push-endpoint="https://pipeline2-reporting-307863403330.us-central1.run.app/"

# 10. Test the pipeline by publishing a message
gcloud pubsub topics publish sales-reports --message='{"region": "North America"}'