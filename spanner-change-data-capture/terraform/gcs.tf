resource "google_storage_bucket" "dataflow-temp" {
  name = "${var.project_id}-dataflow-spanner-bq-temp"
  uniform_bucket_level_access = true
  location = var.region
}
