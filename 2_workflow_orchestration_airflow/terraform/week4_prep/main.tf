# terraform {
#   required_providers {
#     google = {
#       source  = "hashicorp/google"
#       version = "5.6.0"
#     }
#   }
# }

data "google_bigquery_dataset" "nyctaxi" {
  dataset_id = var.BQ_DATASET 
}

resource "google_bigquery_table" "fhv_external_table" {
  dataset_id = data.google_bigquery_dataset.nyctaxi.dataset_id # google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id   = var.FHV_EXTERNAL_TABLE_NAME
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = var.FHV_GCS_OBJECTS
  }
}