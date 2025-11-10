terraform {
  backend "gcs" {
    bucket = "sada-tirayr-terraform-state"
    prefix = "sentiment-analysis/state"
  }
}
