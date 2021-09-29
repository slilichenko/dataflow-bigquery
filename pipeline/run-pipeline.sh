set -e
set -u

MODE=$1
RUNNER=$2
PERSISTENCE=$3
if [ ${MODE} = 'streaming' ] ; then
  PARAMS="--enableStreamingEngine --diskSizeGb=30 --subscriptionId=${EVENT_SUB}"
elif [ ${MODE} = 'batch' ]; then
  PARAMS="--fileList=${DATA_BUCKET}/*.json"
else
  echo "First parameter must be either 'streaming' or 'batch'";
  exit 1;
fi

set -x
./gradlew run -DmainClass=com.google.solutions.pipeline.BigQueryWritePipeline -Pargs="--jobName=data-processing-${MODE} \
 --project=${PROJECT_ID}\
 --region=${GCP_REGION}\
 --maxNumWorkers=10\
 --runner=${RUNNER}\
 --datasetName=${DATASET}\
 --persistenceMethod=${PERSISTENCE}\
 ${PARAMS}"

