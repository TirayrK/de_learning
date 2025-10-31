# Storage Outputs
output "staging_bucket_name" {
  description = "Name of the staging bucket"
  value       = google_storage_bucket.staging_bucket.name
}

output "staging_bucket_url" {
  description = "GCS URL of the staging bucket"
  value       = "gs://${google_storage_bucket.staging_bucket.name}"
}

output "output_bucket_name" {
  description = "Name of the output bucket"
  value       = google_storage_bucket.output_bucket.name
}

output "output_bucket_url" {
  description = "GCS URL of the output bucket"
  value       = "gs://${google_storage_bucket.output_bucket.name}"
}

# BigQuery Outputs
output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.sentiment_dataset.dataset_id
}

output "bigquery_dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.sentiment_dataset.location
}

output "bigquery_table_reviews" {
  description = "Full table ID for reviews with sentiment"
  value       = "${google_bigquery_dataset.sentiment_dataset.dataset_id}.${google_bigquery_table.reviews_with_sentiment.table_id}"
}

output "bigquery_table_summary" {
  description = "Full table ID for product sentiment summary"
  value       = "${google_bigquery_dataset.sentiment_dataset.dataset_id}.${google_bigquery_table.product_sentiment_summary.table_id}"
}

# Autoscaling Policy Outputs
output "autoscaling_policy_id" {
  description = "Full resource ID of the autoscaling policy"
  value       = google_dataproc_autoscaling_policy.sentiment_analysis_policy.id
}

output "autoscaling_policy_name" {
  description = "Name of the autoscaling policy for job submission"
  value       = google_dataproc_autoscaling_policy.sentiment_analysis_policy.policy_id
}

# Service Account Outputs
output "service_account_email" {
  description = "Service account email for Dataproc"
  value       = google_service_account.dataproc_sa.email
}
