# GCS backend for Terraform state storage
terraform {
  backend "gcs" {
    bucket = "sada-tirayr-terraform-state"
    prefix = "sentiment-analysis/state"
  }
}