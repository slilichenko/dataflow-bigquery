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

set -e
set -u

MODE=$1
RUNNER=$2
PERSISTENCE=$3

JOB_NAME="data-processing-${MODE}-${PERSISTENCE//_/-}"

# EXPERIMENTS=enable_recommendations,enable_google_cloud_profiler,enable_google_cloud_heap_sampling
EXPERIMENTS=enable_recommendations

if [ ${MODE} = 'streaming' ] ; then
  PARAMS="--enableStreamingEngine --diskSizeGb=30 --subscriptionId=${EVENT_SUB}"
elif [ ${MODE} = 'batch' ]; then
  PARAMS="--fileList=${DATA_BUCKET}/*.json"
else
  echo "First parameter must be either 'streaming' or 'batch'";
  exit 1;
fi

if [ "$#" -eq 4 ]; then
  TRIGGERING_FREQUENCY=$4
  PARAMS="${PARAMS} --storageWriteApiTriggeringFrequencySec=${TRIGGERING_FREQUENCY}"
  JOB_NAME="${JOB_NAME}-${TRIGGERING_FREQUENCY}"
fi

set -x
./gradlew run -DmainClass=com.google.solutions.schema_change_pipeline.BigQueryWritePipeline -Pargs="--jobName=${JOB_NAME} \
 --project=${PROJECT_ID}\
 --region=${GCP_REGION}\
 --maxNumWorkers=10\
 --runner=${RUNNER}\
 --datasetName=${DATASET}\
 --experiments=${EXPERIMENTS}\
 --persistenceMethod=${PERSISTENCE}\
 ${PARAMS}"

