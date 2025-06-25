variable "homework_bucket_name" {
  description = "Bucket for Week 3 Homework"
  default     = "dezoomcamp_hw3_2025_dtc-de-course-462612"
}

variable "location" {
  description = "Default location"
  default     = "US"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}