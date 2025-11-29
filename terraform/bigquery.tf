# BigQuery dataset for sentiment analysis results
resource "google_bigquery_dataset" "sentiment_dataset" {
  dataset_id = "sentiment_analysis"
  location   = "US"
}