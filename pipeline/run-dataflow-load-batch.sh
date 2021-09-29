set -e
set -u

MODE=batch
RUNNER=DataflowRunner
PERSISTENCE=FILE_LOADS

./run-pipeline.sh ${MODE} ${RUNNER} ${PERSISTENCE}
