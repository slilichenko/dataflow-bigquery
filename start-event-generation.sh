set -e
set -u

QPS=$1
JOB_NAME=data-generator-${QPS}
gcloud dataflow flex-template run ${JOB_NAME} \
    --project=${PROJECT_ID} \
    --region=${GCP_REGION} \
    --template-file-gcs-location=gs://dataflow-templates/latest/flex/Streaming_Data_Generator \
    --parameters schemaLocation=${EVENT_GENERATOR_TEMPLATE} \
    --parameters qps=${QPS} \
    --parameters topic=${EVENT_TOPIC}
