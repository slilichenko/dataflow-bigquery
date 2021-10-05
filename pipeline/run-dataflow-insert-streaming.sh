set -e
set -u

MODE=streaming
RUNNER=DataflowRunner
PERSISTENCE=STREAMING_INSERTS

./run-pipeline.sh ${MODE} ${RUNNER} ${PERSISTENCE}
