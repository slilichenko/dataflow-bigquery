output "bq-project-id" {
  value = google_bigquery_dataset.spanner_bigquery.project
}
output "bq-dataset" {
  value = google_bigquery_dataset.spanner_bigquery.dataset_id
}
output "dataflow-temp-bucket" {
  value = google_storage_bucket.dataflow-temp.id
}
output "orders_change_stream" {
  value = local.orders_change_stream
}

output "spanner-project-id" {
  value = google_spanner_instance.main.project
}
output "spanner-database" {
  value = google_spanner_database.fulfillment.name
}
output "spanner-instance" {
  value = google_spanner_instance.main.name
}