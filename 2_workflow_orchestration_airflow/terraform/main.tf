module "week2_main" {
  source = "./week2_main"

  gcs_bucket_name            = var.gcs_bucket_name
  location                   = var.location
  gcs_storage_class          = var.gcs_storage_class
  BQ_DATASET                 = var.BQ_DATASET
  YELLOW_EXTERNAL_TABLE_NAME = var.YELLOW_EXTERNAL_TABLE_NAME
  YELLOW_GCS_OBJECTS         = var.YELLOW_GCS_OBJECTS
  GREEN_EXTERNAL_TABLE_NAME  = var.GREEN_EXTERNAL_TABLE_NAME
  GREEN_GCS_OBJECTS          = var.GREEN_GCS_OBJECTS
}

module "week3_homework" {
  source = "./week3_homework"
}

module "week4_prep" {
  source = "./week4_prep"

  # BQ_DATASET              = var.BQ_DATASET
  # FHV_EXTERNAL_TABLE_NAME = var.FHV_EXTERNAL_TABLE_NAME
  # FHV_GCS_OBJECTS         = var.FHV_GCS_OBJECTS
}