#
# Copyright 2023 Google LLC
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

set -e
set -u

JOB_NAME=data-capture
gcloud dataflow jobs run ${JOB_NAME} \
    --gcs-location gs://dataflow-templates-${REGION}/latest/Cloud_PubSub_to_GCS_Text \
    --region ${REGION} \
    --staging-location ${DATAFLOW_TEMP_BUCKET} \
    --parameters \
inputTopic=${EVENT_TOPIC},\
outputDirectory=${DATA_BUCKET},\
outputFilenamePrefix=data-,\
outputFilenameSuffix=.json
