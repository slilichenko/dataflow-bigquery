#!/usr/bin/env bash
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

set -u
set -e

cd ..
echo "Setting up the environment..."
source setup-env.sh

echo "Starting the event generation pipeline..."
#./start-event-generation.sh 10

echo "Start the streaming pipeline..."
cd pipeline-schema-change
./run-dataflow-storage-streaming.sh 2 2

echo "Waiting for 10 minutes for the pipeline to start processing events: `date`"
sleep 600

echo "Changing the database schema"
bq query --use_legacy_sql=false  "ALTER TABLE ${PROJECT_ID}.${DATASET}.events ADD COLUMN newly_added STRING"

echo "Starting additional event generation with new field"
./start-event-generation-with-new-column.sh 10

echo "Waiting for 10 minutes for the new events to be processed"
sleep 600

echo "If there are records in the query below the schema change works."
bq query --use_legacy_sql=false  "SELECT newly_added, COUNT(*) as count FROM ${PROJECT_ID}.${DATASET}.events GROUP BY newly_added"
