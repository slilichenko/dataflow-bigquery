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

MODE=streaming
RUNNER=DataflowRunner
PERSISTENCE=STORAGE_WRITE_API
NUMBER_OF_STREAMS=$1
TRIGGERING_FREQUENCY=$2

./run-pipeline.sh ${MODE} ${RUNNER} ${PERSISTENCE} ${NUMBER_OF_STREAMS} ${TRIGGERING_FREQUENCY}
