resource "google_storage_bucket" "dataflow-temp" {
  name = "${var.project_id}-dataflow-spanner-bq-temp"
  uniform_bucket_level_access = true
  location = var.region
}

resource "google_storage_bucket_iam_member" "dataflow_sa_editor" {
  bucket = google_storage_bucket.dataflow-temp.name
  member = local.dataflow_sa_principal
  role   = "roles/storage.objectUser"
}