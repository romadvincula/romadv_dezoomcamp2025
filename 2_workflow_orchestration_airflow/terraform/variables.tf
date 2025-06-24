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

variable "YELLOW_EXTERNAL_TABLE_NAME" {
  description = "Name for external table for the yellow NY Taxi dataset"
  type        = string
  default     = "external_yellow_nytaxi"
}

variable "YELLOW_GCS_OBJECTS" {
  description = "List of objects in GCS containing the yellow NY Taxi data"
  type        = list(string)
  default = [
    "gs://dtc_data_lake_dtc-de-course-462612/landing/yellow_2019-*.parquet",
    "gs://dtc_data_lake_dtc-de-course-462612/landing/yellow_2020-*.parquet",
    "gs://dtc_data_lake_dtc-de-course-462612/landing/yellow_2021-*.parquet",
  ]
}

variable "GREEN_EXTERNAL_TABLE_NAME" {
  description = "Name for external table for the green NY Taxi dataset"
  type        = string
  default     = "external_green_nytaxi"
}

variable "GREEN_GCS_OBJECTS" {
  description = "List of objects in GCS containing the green NY Taxi data"
  type        = list(string)
  default = [
    "gs://dtc_data_lake_dtc-de-course-462612/landing/green_2019-*.parquet",
    "gs://dtc_data_lake_dtc-de-course-462612/landing/green_2020-*.parquet",
    "gs://dtc_data_lake_dtc-de-course-462612/landing/green_2021-*.parquet"
  ]
}

variable "FHV_EXTERNAL_TABLE_NAME" {
  description = "Name for external table for the for-hire-vehicles (FHV) NY Taxi dataset"
  type        = string
  default     = "external_fhv_nytaxi"
}

variable "FHV_GCS_OBJECTS" {
  description = "List of objects in GCS containing the for-hire-vehicles (FHV) NY Taxi data"
  type        = list(string)
  default = [
    "gs://dtc_data_lake_dtc-de-course-462612/landing/fhv_2019-*.parquet"
  ]
}
