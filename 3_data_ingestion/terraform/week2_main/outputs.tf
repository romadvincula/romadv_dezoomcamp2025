# Output for Google Storage Bucket - Data Lake
output "data_lake_name" {
    value = google_storage_bucket.data-lake-bucket.name
}

output "nyctaxi_dataset_id" {
    value = google_bigquery_dataset.nyctaxi_dataset.dataset_id
}

output "bq_yellow_external_table_id" {
    value = google_bigquery_table.yellow_external_table.table_id
}

output "bq_green_external_table_id" {
    value = google_bigquery_table.green_external_table.table_id
}