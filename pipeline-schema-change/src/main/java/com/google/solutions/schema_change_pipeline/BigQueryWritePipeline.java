/*
 * Copyright 2023 Google LLC
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

package com.google.solutions.schema_change_pipeline;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for the event processing pipeline.
 */
public class BigQueryWritePipeline {

  public static final Logger LOG = LoggerFactory.getLogger(
      BigQueryWritePipeline.class);

  public static final Counter totalEvents = Metrics
      .counter(BigQueryWritePipeline.class, "total-events");
  public static final Counter failedEvents = Metrics
      .counter(BigQueryWritePipeline.class, "failed-events");

  public static void main(String[] args) {
    BigQueryWritePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigQueryWritePipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline
   */
  public static void run(BigQueryWritePipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> input;

    input = pipeline.begin().apply("Read PubSub",
        PubsubIO.readStrings().fromSubscription(options.getSubscriptionId()));

    PCollection<TableRow> rows = input.apply("To TableRow", ParDo.of(new RawEventToTableRow()));

    Method method = getPersistenceMethod(options);

    Write<TableRow> bigQueryWriteTransform = BigQueryIO.writeTableRows()
        .to(projectId(options) + '.' +
            options.getDatasetName() + '.' + options.getEventsTable())
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withMethod(method);

    switch (method) {
      case STORAGE_API_AT_LEAST_ONCE:
        break;

      case STORAGE_WRITE_API:
          bigQueryWriteTransform = bigQueryWriteTransform
              // .withAutoSharding()
              .withNumStorageWriteApiStreams(2)
              .withAutoSchemaUpdate(true);
        break;

      default:
        throw new IllegalStateException("Unhandled method " + method);
    }

    WriteResult writeResult = rows.apply("Save Rows to BigQuery", bigQueryWriteTransform);

    switch (method) {
      case STORAGE_WRITE_API:
        var failedInserts = writeResult.getFailedStorageApiInserts();
        failedInserts.apply("Process errors", ParDo.of(
            new DoFn<BigQueryStorageApiInsertError, Void>() {
              @ProcessElement
              public void process(@Element BigQueryStorageApiInsertError error) {
                LOG.info("Failed: " + error);
                failedEvents.inc();
              }
            }));
        break;

      case STORAGE_API_AT_LEAST_ONCE:
        break;

      default:
        throw new IllegalStateException("Unhandled method " + method);
    }

    PipelineResult run = pipeline.run();
    if (options.getRunner().getName().equalsIgnoreCase("directrunner")) {
      LOG.info("Waiting until the job finishes.");
      run.waitUntilFinish();
    }
  }

  private static String projectId(BigQueryWritePipelineOptions options) {
    return options.getBigQueryProject() == null
        ? options.getProject()
        : options.getBigQueryProject();
  }

  private static Iterable<String> eventsWithIncompatibleValues() {
    return List.of(
        "{"
            + "\"bytes_sent\": 123, "
            + "\"destination_ip\": \"max length of the field is defined as 15 - should fail\", "
            + "\"destination_port\": 80, "
            + "\"process\": \"process1\", "
            + " \"bytes_received\": 200, \"source_ip\": \"192.6.6.1\", \"user\": \"user1\"}"
//        ,
//        "{\"bytes_sent\": \"Should fail on conversion of String to Integer\", "
//            + "\"destination_ip\": \"10.2.1.1\", \"destination_port\": 80, "
//            + "\"process\": \"process1\""
//            + ", \"bytes_received\": 200, \"source_ip\": \"192.6.6.1\", \"user\": \"user1\"}"

    );
  }

  private static Method getPersistenceMethod(BigQueryWritePipelineOptions options) {
    Set<String> validPersistenceMethods = Set
        .of(Method.STORAGE_WRITE_API.toString(), Method.FILE_LOADS.toString(),
            Method.STREAMING_INSERTS.toString(),
            Method.STORAGE_API_AT_LEAST_ONCE.toString());
    if (!validPersistenceMethods.contains(options.getPersistenceMethod())) {
      throw new IllegalArgumentException(
          "Persistence method must one of " + validPersistenceMethods);
    }
    return Method
        .valueOf(options.getPersistenceMethod());
  }

  static TableSchema getEventsSchema(boolean testIncompatibleSchemaHandling) {

    return new TableSchema().setFields(
        List.of(
            new TableFieldSchema()
                .setName("request_ts")
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("bytes_sent")
                .setType(testIncompatibleSchemaHandling ? "STRING" : "INTEGER")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("bytes_received")
                .setType("INTEGER")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("dst_hostname")
                .setType("STRING")
                .setMode("NULLABLE"),
            new TableFieldSchema()
                .setName("dst_ip")
                .setType("STRING")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("dst_port")
                .setType("INTEGER")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("src_ip")
                .setType("STRING")
                .setMode("NULLABLE"),
            new TableFieldSchema()
                .setName("user_id")
                .setType("STRING")
                .setMode("NULLABLE"),
            new TableFieldSchema()
                .setName("process_name")
                .setType("STRING")
                .setMode("NULLABLE")
        )
    );
  }

}
