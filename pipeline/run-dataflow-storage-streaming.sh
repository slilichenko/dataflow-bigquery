set -e
set -u

MODE=streaming
RUNNER=DataflowRunner
PERSISTENCE=STORAGE_WRITE_API

./run-pipeline.sh ${MODE} ${RUNNER} ${PERSISTENCE}
