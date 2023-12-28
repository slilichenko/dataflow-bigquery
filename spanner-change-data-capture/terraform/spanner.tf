resource "google_spanner_instance" "main" {
  config       = "regional-${var.region}"
  display_name = "main-instance"
  num_nodes    = 1
}

locals {
  orders_change_stream = "orders_changes"
}

resource "google_spanner_database" "fulfillment" {
  instance = google_spanner_instance.main.name
  name     = "fulfillment"
  version_retention_period = "1d"
  ddl = [
    "CREATE TABLE orders (order_id INT64 NOT NULL, status STRING(10) NOT NULL, description STRING(64) NOT NULL) PRIMARY KEY(order_id)",
    "CREATE CHANGE STREAM ${local.orders_change_stream} FOR orders"
  ]
  deletion_protection = false
}