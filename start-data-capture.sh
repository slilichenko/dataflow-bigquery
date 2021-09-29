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
