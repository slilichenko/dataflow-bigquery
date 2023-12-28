resource "google_project_service" "dataflow-api" {
  service = "dataflow.googleapis.com"
}
resource "google_project_service" "spanner-api" {
  service = "spanner.googleapis.com"
}