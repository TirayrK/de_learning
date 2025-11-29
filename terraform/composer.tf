# Cloud Composer environment for Airflow orchestration
resource "google_composer_environment" "sentiment_pipeline" {
  name    = "sentiment-composer"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      network         = data.google_compute_network.dev_vpc.id
      subnetwork      = google_compute_subnetwork.composer_subnet.id
      service_account = google_service_account.composer_sa.email

      ip_allocation_policy {
        cluster_secondary_range_name  = "composer-pods"
        services_secondary_range_name = "composer-services"
      }
    }

    private_environment_config {
      enable_private_endpoint    = false
      master_ipv4_cidr_block     = "172.17.0.0/28"
      cloud_sql_ipv4_cidr_block  = "10.21.0.0/16"
    }
  }

  depends_on = [
    google_project_service.composer_api,
    google_compute_subnetwork.composer_subnet,
    google_service_account.composer_sa
  ]
}

# Enable Cloud Composer API
resource "google_project_service" "composer_api" {
  project            = var.project_id
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}