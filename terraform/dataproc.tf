# Dataproc autoscaling policy for sentiment analysis workloads
resource "google_dataproc_autoscaling_policy" "sentiment_analysis_policy" {
  policy_id = "sentiment-analysis-autoscaling"
  location  = var.region

  worker_config {
    max_instances = 10
    min_instances = 2
  }

  basic_algorithm {
    yarn_config {
      graceful_decommission_timeout  = "30s"
      scale_up_factor                = 0.5
      scale_down_factor              = 1.0
      scale_up_min_worker_fraction   = 0.5
      scale_down_min_worker_fraction = 0.0
    }
  }
}