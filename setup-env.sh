#!/usr/bin/env bash
set -u

export TF_VAR_project_id=${PROJECT_ID}
export TF_VAR_region=${GCP_REGION}
export TF_VAR_bigquery_dataset_location=${BIGQUERY_REGION}

cd terraform
terraform init && terraform apply

export EVENT_GENERATOR_TEMPLATE=$(terraform output -raw event-generator-template)
export EVENT_TOPIC=$(terraform output -raw event-topic)
export EVENT_SUB=$(terraform output -raw event-sub)
export DATASET=$(terraform output -raw bq-dataset)
export DATAFLOW_TEMP_BUCKET=gs://$(terraform output -raw dataflow-temp-bucket)
export DATA_BUCKET=gs://$(terraform output -raw data-bucket)
export REGION=$(terraform output -raw region)

cd ..
