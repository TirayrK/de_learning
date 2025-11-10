resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sentiment-sa"
  display_name = "Dataproc Sentiment Analysis Service Account"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_storage_bucket_iam_member" "bucket_access" {
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

resource "google_bigquery_dataset_iam_member" "dataset_editor" {
  dataset_id = google_bigquery_dataset.sentiment_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataproc_sa.email}"
}
