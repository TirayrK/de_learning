# Reference to existing VPC network
data "google_compute_network" "dev_vpc" {
  name = "dev-vpc"
}

# Subnetwork for Cloud Composer environment
resource "google_compute_subnetwork" "composer_subnet" {
  name          = "composer-subnet"
  region        = var.region
  network       = data.google_compute_network.dev_vpc.id
  ip_cidr_range = "10.10.0.0/24"

  # Secondary IP range for GKE pods
  secondary_ip_range {
    range_name    = "composer-pods"
    ip_cidr_range = "10.11.0.0/16"
  }

  # Secondary IP range for GKE services
  secondary_ip_range {
    range_name    = "composer-services"
    ip_cidr_range = "10.12.0.0/20"
  }
}