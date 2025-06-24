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