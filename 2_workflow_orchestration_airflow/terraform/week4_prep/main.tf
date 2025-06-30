# terraform {
#   required_providers {
#     google = {
#       source  = "hashicorp/google"
#       version = "5.6.0"
#     }
#   }
# }

data "google_bigquery_dataset" "nyctaxi" {
  dataset_id = var.BQ_DATASET 
}

resource "google_bigquery_table" "fhv_external_table" {
  dataset_id = data.google_bigquery_dataset.nyctaxi.dataset_id # google_bigquery_dataset.nyctaxi_dataset.dataset_id
  table_id   = var.FHV_EXTERNAL_TABLE_NAME
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = var.FHV_GCS_OBJECTS
  }
}

# bucket for week 4 homework
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "homework4-data-lake-dtc-de-course-462612"
  location      = "US"
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

# dataset for week 4 homework
resource "google_bigquery_dataset" "homework4_dataset" {
  dataset_id = "homework4_dataset"
  location   = "US"
}

# external tables for week 4 homework
resource "google_bigquery_table" "hw4_yellow_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_yellow_nytaxi"
  deletion_protection = false
  schema = <<EOF
[
  {
    "name": "VendorID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "tpep_pickup_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "tpep_dropoff_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "passenger_count",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "trip_distance",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "RatecodeID",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "store_and_fwd_flag",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "PULocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DOLocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payment_type",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "fare_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "extra",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "mta_tax",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tip_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tolls_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "improvement_surcharge",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "total_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "congestion_surcharge",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "airport_fee",
    "type": "FLOAT",
    "mode": "NULLABLE"
  }
]
EOF

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/yellow/yellow_tripdata_*.parquet"]
  }
}

resource "google_bigquery_table" "hw4_green_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_green_nytaxi"
  deletion_protection = false
  schema = <<EOF
[
  {
    "name": "VendorID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "lpep_pickup_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "lpep_dropoff_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "store_and_fwd_flag",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RatecodeID",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "PULocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DOLocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "passenger_count",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "trip_distance",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "fare_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "extra",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "mta_tax",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tip_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tolls_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "ehail_fee",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "improvement_surcharge",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "total_amount",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "payment_type",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "trip_type",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "congestion_surcharge",
    "type": "FLOAT",
    "mode": "NULLABLE"
  }
]
EOF

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/green/green_tripdata_*.parquet"]
  }
}

resource "google_bigquery_table" "hw4_fhv_external_table" {
  dataset_id = google_bigquery_dataset.homework4_dataset.dataset_id
  table_id   = "external_fhv_nytaxi"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = ["gs://homework4-data-lake-dtc-de-course-462612/fhv/fhv_tripdata_*.parquet"]
  }
}