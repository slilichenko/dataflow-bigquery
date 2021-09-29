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


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
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
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
    LOG.info("Args:" + Arrays.asList(args));
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
    if (options.getSubscriptionId() != null) {
      input = pipeline.begin().apply("Read Pub/Sub",
          PubsubIO.readStrings().fromSubscription(options.getSubscriptionId()));
    } else if (options.getFileList() != null) {
      input = pipeline.begin().apply("Read GCS Files",
          TextIO.read().from(options.getFileList()));
    } else {
      throw new RuntimeException("Either the subscription id or the file list should be provided.");
    }

    PCollection<TableRow> rows = input.apply("To TableRow", ParDo.of(new RawEventToTableRow()));

    Method method = getPersistenceMethod(options);

    Write<TableRow> bigQueryWriteTransform = BigQueryIO.writeTableRows()
        .to(options.getDatasetName() + '.' + options.getEventsTable())
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withMethod(method);

    switch (method) {
      case FILE_LOADS:
        break;

      case STORAGE_WRITE_API:
        TableSchema schema = getEventsSchema();
        bigQueryWriteTransform = bigQueryWriteTransform
            .withSchema(schema)
            .withNumStorageWriteApiStreams(100);
        break;

      case STREAMING_INSERTS:
        bigQueryWriteTransform = bigQueryWriteTransform.withExtendedErrorInfo();
        break;

      default:
        throw new IllegalStateException("Unhandled method " + method);
    }

    WriteResult writeResult = rows.apply("Save Rows to BigQuery", bigQueryWriteTransform);
    switch (method) {
      case STREAMING_INSERTS:
        writeResult.getFailedInsertsWithErr()
            .apply("Every 2 Minutes", Window.into(FixedWindows.of(Duration.standardMinutes(2))))
            .apply("Process Insert Failure",
                ParDo.of(new DoFn<BigQueryInsertError, String>() {
                  @ProcessElement
                  public void process(@Element BigQueryInsertError error) {
                    LOG.error(
                        "Failed to insert: " + error.getError() + ", row: " + error.getRow());
                  }
                })
            );
        break;

      case FILE_LOADS:
        // TODO: no reason to check anything - no errors returned?
        break;

      case STORAGE_WRITE_API:
        // TODO: check if there is a way to get errors back.
        break;

      default:
        throw new IllegalStateException("Unhandled method " + method);
    }

//    outputs.userEventFindings.apply("Persist User Event Findings",
//        new PTransform<>() {
//          @Override
//          public POutput expand(PCollection<UserEventFinding> input1) {
//            return input1
//                .apply("User Event Findings to Row", ParDo.of(new UserEventFindingToTableRow()))
//                .apply("Save to BigQuery",
//                    BigQueryIO.writeTableRows().to(
//                        options.getDatasetName() + '.' + options.getUserEventFindingsTable())
//                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
//                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
//                        .withMethod(Method.STORAGE_WRITE_API)
//                        .withNumStorageWriteApiStreams(3)
//                        .withSchema(schema)
//                        .withTriggeringFrequency(Duration.standardSeconds(60)));
//          }
//        });

    PipelineResult run = pipeline.run();
    if (options.getRunner().getName().equalsIgnoreCase("directrunner")) {
      run.waitUntilFinish();
    }
  }

  private static Method getPersistenceMethod(BigQueryWritePipelineOptions options) {
    Set<String> validPersistenceMethods = Set
        .of(Method.STORAGE_WRITE_API.toString(), Method.FILE_LOADS.toString(),
            Method.STREAMING_INSERTS.toString());
    if (!validPersistenceMethods.contains(options.getPersistenceMethod())) {
      throw new IllegalArgumentException(
          "Persistence method must one of " + validPersistenceMethods);
    }
    return Method
        .valueOf(options.getPersistenceMethod());
  }

  static TableSchema getEventsSchema() {

    return new TableSchema().setFields(
        List.of(
            new TableFieldSchema()
                .setName("request_ts")
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName("bytes_sent")
                .setType("INTEGER")
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
