variable "project_id" {
  type = string
}
variable "region" {
  type = string
}
variable "bigquery_dataset_location" {
  type = string
}
variable "number_of_tables" {
  type = number
  default = 10
}
