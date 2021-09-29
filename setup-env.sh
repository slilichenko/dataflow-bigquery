#!/usr/bin/env bash
set -u

export TF_VAR_project_id=${PROJECT_ID}
export TF_VAR_region=${GCP_REGION}
export TF_VAR_bigquery_dataset_location=${BIGQUERY_REGION}

cd terraform
terraform init && terraform apply

export EVENT_GENERATOR_TEMPLATE=$(terraform output event-generator-template)
export EVENT_TOPIC=$(terraform output event-topic)
export EVENT_SUB=$(terraform output event-sub)
export DATASET=$(terraform output bq-dataset)
export DATAFLOW_TEMP_BUCKET=gs://$(terraform output dataflow-temp-bucket)
export DATA_BUCKET=gs://$(terraform output data-bucket)
export REGION=$(terraform output region)

cd ..
