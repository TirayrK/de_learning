# Terraform version and provider requirements
terraform {
  required_version = "1.13.4"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.45.2"
    }
  }
}

# Google Cloud provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
  impersonate_service_account = "terraform-sa@sada-tirayr.iam.gserviceaccount.com"
}