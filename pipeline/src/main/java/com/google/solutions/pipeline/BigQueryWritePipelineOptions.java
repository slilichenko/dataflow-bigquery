/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.pipeline;

import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Interface to store pipeline options provided by the user
 */
public interface BigQueryWritePipelineOptions extends DataflowPipelineOptions {

  @Description("Pub/Sub subscription to receive raw messages from")
  String getSubscriptionId();

  void setSubscriptionId(String value);

  @Description("Google Cloud Storage files to process")
  String getFileList();

  void setFileList(String value);

  @Description("BigQuery dataset")
  @Required
  String getDatasetName();

  void setDatasetName(String value);

  @Description("Table name for events")
  @Required
  @Default.String("events")
  String getEventsTable();

  void setEventsTable(String value);

  @Description("Persistence method")
  @Required
  String getPersistenceMethod();
  void setPersistenceMethod(String value);

  @Description("Test data conversion error handling")
  @Required
  @Default.Boolean(false)
  boolean getTestErrorHandling();
  void setTestErrorHandling(boolean value);

  @Description("Test incompatible schema error handling")
  @Required
  @Default.Boolean(false)
  boolean getTestIncompatibleSchemaHandling();
  void setTestIncompatibleSchemaHandling(boolean value);
}