resource "google_bigquery_dataset" "bigquery_io" {
  dataset_id = "bigquery_io"
  friendly_name = "Event Monitoring Dataset"
  location = var.bigquery_dataset_location
}

output "bq-dataset" {
  value = google_bigquery_dataset.bigquery_io.dataset_id
}

resource "google_bigquery_table" "events" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.bigquery_io.dataset_id
  table_id = "events"
  description = "All events"
  time_partitioning {
    type = "DAY"
    field = "request_ts"
  }
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "request_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_sent",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_received",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "dst_hostname",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_ip",
    "type": "STRING",
    "maxLength": "15"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_port",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "src_ip",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "process_name",
    "type": "STRING"
  }
]
EOF
}
