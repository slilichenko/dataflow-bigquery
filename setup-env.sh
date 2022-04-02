#!/usr/bin/env bash
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -u

export TF_VAR_project_id=${PROJECT_ID}
export TF_VAR_region=${GCP_REGION}
export TF_VAR_bigquery_dataset_location=${BIGQUERY_REGION}

cd terraform
terraform init && terraform apply

export EVENT_GENERATOR_TEMPLATE=$(terraform output -raw event-generator-template)
export EVENT_GENERATOR_NEW_COLUMN_TEMPLATE=$(terraform output -raw event-generator-template-new-column)
export EVENT_TOPIC=$(terraform output -raw event-topic)
export EVENT_SUB=$(terraform output -raw event-sub)
export DATASET=$(terraform output -raw bq-dataset)
export DATAFLOW_TEMP_BUCKET=gs://$(terraform output -raw dataflow-temp-bucket)
export DATA_BUCKET=gs://$(terraform output -raw data-bucket)
export REGION=$(terraform output -raw region)

cd ..
