variable "project" {
  description = "Name of project in Google Cloud"
  default     = "dtc-de-course-462612"
}

variable "credentials" {
  description = "Project credentials"
  default     = "~/.google/credentials/google_credentials.json"
}

variable "region" {
  description = "Default region"
  default     = "australia-southeast1"
}

variable "gcs_bucket_name" {
  description = "Data Lake Bucket"
  default     = "dtc_data_lake_dtc-de-course-462612"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "location" {
  description = "Default location"
  default     = "US"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

