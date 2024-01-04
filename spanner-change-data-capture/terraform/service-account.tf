resource "google_service_account" "dataflow-sa" {
  account_id   = "dataflow-spanner-to-bq-sa"
  display_name = "Service Account to run Dataflow jobs"
}

locals {
  dataflow_sa_principal = "serviceAccount:${google_service_account.dataflow-sa.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  member  = local.dataflow_sa_principal
  project = var.project_id
  role    = "roles/dataflow.worker"
}
