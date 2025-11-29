# Dataproc Worker role at project level
resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Logs Writer for Dataproc to write logs
resource "google_project_iam_member" "dataproc_logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Storage bucket access for Dataproc
resource "google_storage_bucket_iam_member" "dataproc_bucket_access" {
  for_each = {
    staging = {
      bucket = google_storage_bucket.staging_bucket.name
      role   = "roles/storage.objectViewer"
    }
    output = {
      bucket = google_storage_bucket.output_bucket.name
      role   = "roles/storage.objectAdmin"
    }
  }

  bucket = each.value.bucket
  role   = each.value.role
  member = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# BigQuery dataset access for Dataproc
resource "google_bigquery_dataset_iam_member" "dataproc_dataset_editor" {
  dataset_id = google_bigquery_dataset.sentiment_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:dataproc-sentiment-sa@sada-tirayr.iam.gserviceaccount.com"
}

# Composer Worker role at project level
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Logs Writer for Composer to write logs
resource "google_project_iam_member" "composer_logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Monitoring Metric Writer for Composer
resource "google_project_iam_member" "composer_monitoring_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# BigQuery Job User for Composer to run BQ jobs
resource "google_project_iam_member" "composer_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Dataproc Editor role for Composer to create/delete clusters
resource "google_project_iam_member" "composer_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Service Account User role for Composer to use Dataproc SA
resource "google_project_iam_member" "composer_sa_user_on_dataproc" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Storage bucket access for Composer (staging + output)
resource "google_storage_bucket_iam_member" "composer_bucket_access" {
  for_each = {
    staging = {
      bucket = google_storage_bucket.staging_bucket.name
      role   = "roles/storage.objectAdmin"
    }
    output = {
      bucket = google_storage_bucket.output_bucket.name
      role   = "roles/storage.objectAdmin"
    }
  }

  bucket = each.value.bucket
  role   = each.value.role
  member = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Composer DAGs bucket access
resource "google_storage_bucket_iam_member" "composer_dags_bucket_access" {
  bucket = split("/", google_composer_environment.sentiment_pipeline.config[0].dag_gcs_prefix)[2]
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:composer-sentiment-sa@sada-tirayr.iam.gserviceaccount.com"

  depends_on = [google_composer_environment.sentiment_pipeline]
}

# BigQuery dataset access for Composer
resource "google_bigquery_dataset_iam_member" "composer_dataset_editor" {
  dataset_id = google_bigquery_dataset.sentiment_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:composer-sentiment-sa@sada-tirayr.iam.gserviceaccount.com"
}