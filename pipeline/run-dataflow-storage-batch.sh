set -e
set -u

MODE=batch
RUNNER=DataflowRunner
PERSISTENCE=STORAGE_WRITE_API

./run-pipeline.sh ${MODE} ${RUNNER} ${PERSISTENCE}
