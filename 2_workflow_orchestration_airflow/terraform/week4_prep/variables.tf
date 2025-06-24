
variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
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
