resource "google_service_account" "dataflow-sa" {
  account_id   = "dataflow-spanner-to-bq-sa"
  display_name = "Service Account to run Dataflow jobs"
}
