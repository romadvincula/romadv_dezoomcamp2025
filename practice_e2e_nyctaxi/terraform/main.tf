resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.datalake_bucket
  location      = var.location
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

resource "google_bigquery_dataset" "nyctaxi_dataset" {
  dataset_id = var.NYCTAXI_DATASET
  location   = var.location
}

# add external tables later