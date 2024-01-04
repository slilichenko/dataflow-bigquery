variable "project_id" {
  type = string
}
variable "region" {
  type = string
  default = "us-central1"
}
variable "bigquery_dataset_location" {
  type = string
  default = "us-central1"
}
variable "spanner_location" {
  type = string
  default = "us-central1"
}
