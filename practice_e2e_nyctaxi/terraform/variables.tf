variable "project_id" {
  description = "Name of project for NY Taxi End-to-End Solution in Google Cloud"
  default     = "myproject"
}

variable "credentials" {
  description = "Path to credentials file"
  default     = "/path/to/credentials.json"
}

variable "region" {
  default = "australia-southeast1"
}

variable "datalake_bucket" {
  default     = "mybucket"
  description = "Datalake for NYC Taxi data"
}

variable "location" {
  default = "US"
}

variable "NYCTAXI_DATASET" {
  description = "BigQuery Dataset for NY Taxi data"
  type        = string
  default     = "mydataset"
}