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
output "dataproc_service_account_email" {
  description = "Service account email for Dataproc"
  value       = google_service_account.dataproc_sa.email
}

output "composer_service_account_email" {
  description = "Composer service account email"
  value       = google_service_account.composer_sa.email
}

# Composer Outputs
output "composer_environment_name" {
  description = "Name of the Composer environment"
  value       = google_composer_environment.sentiment_pipeline.name
}

output "composer_airflow_uri" {
  description = "The URI of the Airflow web interface"
  value       = google_composer_environment.sentiment_pipeline.config[0].airflow_uri
}

output "composer_gcs_bucket" {
  description = "The GCS bucket where DAGs should be uploaded"
  value       = google_composer_environment.sentiment_pipeline.config[0].dag_gcs_prefix
}

# Network Outputs
output "vpc_network_name" {
  description = "Name of the VPC network"
  value       = data.google_compute_network.dev_vpc.name
}

output "composer_subnet_name" {
  description = "Name of the Composer subnetwork"
  value       = google_compute_subnetwork.composer_subnet.name
}

output "composer_subnet_cidr" {
  description = "CIDR range of the Composer subnetwork"
  value       = google_compute_subnetwork.composer_subnet.ip_cidr_range
}

# Project and Region Outputs
output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP region"
  value       = var.region
}