variable "project" {
  description = "Name of project in Google Cloud"
  default     = "dtc-de-course-462612"
}

variable "credentials" {
  description = "Project credentials"
  default     = "./keys/creds.json"
}

variable "region" {
  description = "Default region"
  default     = "australia-southeast1"
}

variable "bq_dataset_name" {
  description = "My Bigquery Dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "dtc-de-course-462612-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "location" {
  description = "Default location"
  default     = "US"
}

