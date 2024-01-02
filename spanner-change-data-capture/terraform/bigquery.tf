resource "google_bigquery_dataset" "spanner_bigquery" {
  dataset_id    = "spanner_to_bigquery"
  friendly_name = "Demo of Spanner to BigQuery replication using CDC"
  location      = var.bigquery_dataset_location
}

resource "google_bigquery_table" "orders" {
  deletion_protection = false
  dataset_id          = google_bigquery_dataset.spanner_bigquery.dataset_id
  table_id            = "order"
  description         = "Replicated orders"
  clustering          = ["order_id"]
  table_constraints {
    primary_key { columns = ["order_id"] }
  }
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "order_id",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "status",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "description",
    "type": "STRING"
  }
]
EOF
}
