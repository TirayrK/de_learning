# Service account for Dataproc cluster operations
resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sentiment-sa"
  display_name = "Dataproc Sentiment Analysis Service Account"
}

# Service account for Composer (Airflow) operations
resource "google_service_account" "composer_sa" {
  account_id   = "composer-sentiment-sa"
  display_name = "Composer Sentiment Analysis Service Account"
}