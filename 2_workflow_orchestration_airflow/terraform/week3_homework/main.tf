resource "google_storage_bucket" "week3-homework-bucket" {
  name          = var.homework_bucket_name
  location      = var.location
  force_destroy = true

  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

data "google_bigquery_dataset" "nyctaxi_dataset" {
  dataset_id = var.BQ_DATASET
}

# create external table in BigQuery
resource "google_bigquery_table" "homework3_external_table" {
  dataset_id = data.google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id   = "homework3_external_table"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${google_storage_bucket.week3-homework-bucket.name}/landing/yellow_2024-*.parquet"
    ]
  }
}
