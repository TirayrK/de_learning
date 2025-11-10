resource "google_bigquery_dataset" "sentiment_dataset" {
  dataset_id = "sentiment_analysis"
  location   = "US"
}

resource "google_bigquery_table" "reviews_with_sentiment" {
  dataset_id = google_bigquery_dataset.sentiment_dataset.dataset_id
  table_id   = "reviews_with_sentiment"
  
  schema = jsonencode([
    {
      name = "Id"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "ProductId"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "UserId"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ProfileName"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "HelpfulnessNumerator"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "HelpfulnessDenominator"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "Score"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "Time"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "Summary"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Text"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "sentiment_score"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "satisfaction_level"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "product_sentiment_summary" {
  dataset_id = google_bigquery_dataset.sentiment_dataset.dataset_id
  table_id   = "product_sentiment_summary"
  
  schema = jsonencode([
    {
      name = "product_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "total_number_of_reviews"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "avg_sentiment_score"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "avg_review_score"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "pct_without_text_review"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "very_satisfied_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "somewhat_satisfied_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "neutral_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "somewhat_dissatisfied_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "very_dissatisfied_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "pct_without_score_review"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "score_5_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "score_4_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "score_3_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "score_2_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "score_1_pct"
      type = "FLOAT"
      mode = "NULLABLE"
    }
  ])
}
