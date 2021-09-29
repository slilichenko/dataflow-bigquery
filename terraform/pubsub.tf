resource "google_pubsub_topic" "event_topic" {
  name = "network-event-topic"
}

output "event-topic" {
  value = google_pubsub_topic.event_topic.id
}

resource "google_pubsub_subscription" "event_sub" {
  name = "network-event-sub"
  topic = google_pubsub_topic.event_topic.name
}

output "event-sub" {
  value = google_pubsub_subscription.event_sub.id
}