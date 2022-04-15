# Beam to BigQuery
This repo contains sample code for testing different BigQuery persistence
methods in Apache Beam pipelines

# Environment setup
```shell
export PROJECT_ID=<project_id>
export GCP_REGION=<region>
export BIGQUERY_REGION=us
source setup-env.sh
```

# Start event generation for streaming pipelines
```shell
./start-event-generation.sh <events-per-second>
```

# Run a pipeline
```shell
cd pipeline
./run-dataflow-<method>.sh <params>
```
For Storage Write API:
```shell
./run-dataflow-storage-streaming.sh <number-of-streams> <triggering-frequency-in-seconds>
```
