terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
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

resource "google_bigquery_dataset" "nyctaxi_dataset" {
  dataset_id = var.BQ_DATASET
  location   = var.location
}

resource "google_bigquery_table" "yellow_external_table" {
  dataset_id          = google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id            = var.YELLOW_EXTERNAL_TABLE_NAME
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = var.YELLOW_GCS_OBJECTS
  }
}

resource "google_bigquery_table" "green_external_table" {
  dataset_id          = google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id            = var.GREEN_EXTERNAL_TABLE_NAME
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = var.GREEN_GCS_OBJECTS
  }
}

resource "google_bigquery_table" "fhv_external_table" {
  dataset_id          = google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id            = var.FHV_EXTERNAL_TABLE_NAME
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = var.FHV_GCS_OBJECTS
  }
}