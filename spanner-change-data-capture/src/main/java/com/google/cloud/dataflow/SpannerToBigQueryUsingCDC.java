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

package com.google.cloud.dataflow;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.Timestamp;
import com.google.cloud.dataflow.model.OrderMutation;
import com.google.cloud.dataflow.model.OrderMutation.OrderMutationCoder;
import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SpannerToBigQueryUsingCDC {
  public interface Options extends GcpOptions {
    String getSpannerProjectId();
    void setSpannerProjectId(String value);

    String getSpannerInstanceId();
    void setSpannerInstanceId(String value);

    String getSpannerDatabaseId();
    void setSpannerDatabaseId(String value);

    String getSpannerOrdersStreamId();
    void setSpannerOrdersStreamId(String value);

    @Default.String("order")
    String getBigQueryOrdersTableName();
    void setBigQueryOrdersTableName(String value);

    String getBigQueryDataset();
    void setBigQueryDataset(String value);

    @Default.String("orders")
    String getSpannerTableName();
    void setSpannerTableName(String value);

    String getBigQueryProjectId();
    void setBigQueryProjectId(String value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    run(options, p);
  }

  private static void run(Options options, Pipeline p) {
    SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId(options.getSpannerProjectId())
        .withInstanceId(options.getSpannerInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId());

    PCollection<DataChangeRecord> dataChangeRecords = p.apply("Read Change Stream", SpannerIO
        .readChangeStream()
        .withSpannerConfig(spannerConfig)
        .withChangeStreamName(options.getSpannerOrdersStreamId())
        .withRpcPriority(RpcPriority.MEDIUM)
        .withInclusiveStartAt(Timestamp.now()));

    TableReference ordersTableReference = new TableReference();
    ordersTableReference.setProjectId(options.getBigQueryProjectId());
    ordersTableReference.setTableId(options.getBigQueryOrdersTableName());
    ordersTableReference.setDatasetId(options.getBigQueryDataset());

    WriteResult writeResult =
        dataChangeRecords
            .apply("To OrderMutations", ParDo.of(new DataChangeRecordToOrderMutation()))
            .setCoder(new OrderMutationCoder())
            .apply("Save To BigQuery", BigQueryIO
                .<OrderMutation>write()
                .to(ordersTableReference)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withFormatFunction(new OrderMutationToTableRow())
                .withRowMutationInformationFn(
                    orderMutation -> orderMutation.getMutationInformation()));

    writeResult.getFailedStorageApiInserts()
        .apply("Validate no records failed", new BigQueryFailedInsertProcessor());

    p.run();
  }

}
