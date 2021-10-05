set -e
set -u

MODE=$1
RUNNER=$2
PERSISTENCE=$3

JOB_NAME="data-processing-${MODE}-${PERSISTENCE//_/-}"

if [ ${MODE} = 'streaming' ] ; then
  PARAMS="--enableStreamingEngine --diskSizeGb=30 --subscriptionId=${EVENT_SUB}"
elif [ ${MODE} = 'batch' ]; then
  PARAMS="--fileList=${DATA_BUCKET}/*.json"
else
  echo "First parameter must be either 'streaming' or 'batch'";
  exit 1;
fi

if [ "$#" -eq 5 ]; then
  NUMBER_OF_STREAMS=$4
  TRIGGERING_FREQUENCY=$5
  PARAMS="${PARAMS} --numStorageWriteApiStreams=${NUMBER_OF_STREAMS} --storageWriteApiTriggeringFrequencySec=${TRIGGERING_FREQUENCY}"
  JOB_NAME="${JOB_NAME}-${NUMBER_OF_STREAMS}-${TRIGGERING_FREQUENCY}"
fi

set -x
./gradlew run -DmainClass=com.google.solutions.pipeline.BigQueryWritePipeline -Pargs="--jobName=${JOB_NAME} \
 --project=${PROJECT_ID}\
 --region=${GCP_REGION}\
 --maxNumWorkers=10\
 --runner=${RUNNER}\
 --datasetName=${DATASET}\
 --experiments=enable_recommendations\
 --persistenceMethod=${PERSISTENCE}\
 ${PARAMS}"

