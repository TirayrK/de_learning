# GCS bucket for staging data
resource "google_storage_bucket" "staging_bucket" {
  name          = "${var.project_id}-sentiment-staging"
  location      = "US"
  uniform_bucket_level_access = true
}

# GCS bucket for output data
resource "google_storage_bucket" "output_bucket" {
  name          = "${var.project_id}-sentiment-output"
  location      = "US"
  uniform_bucket_level_access = true
}