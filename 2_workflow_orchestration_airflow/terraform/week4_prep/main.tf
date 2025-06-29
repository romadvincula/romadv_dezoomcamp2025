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

# bucket for week 4 homework
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "homework4-data-lake-dtc-de-course-462612"
  location      = "US"
  force_destroy = true

  storage_class               = "STANDARD"
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

# dataset for week 4 homework
resource "google_bigquery_dataset" "homework4_dataset" {
  dataset_id = "homework4_dataset"
  location   = "US"
}

# external tables for week 4 homework
resource "google_bigquery_table" "hw4_yellow_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_yellow_nytaxi"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/yellow/yellow_tripdata_*.parquet"]
  }
}

resource "google_bigquery_table" "hw4_green_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_green_nytaxi"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/green/green_tripdata_*.parquet"]
  }
}

resource "google_bigquery_table" "hw4_fhv_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_fhv_nytaxi"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/fhv/fhv_tripdata_*.parquet"]
  }
}