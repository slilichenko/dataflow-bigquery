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
import com.google.cloud.dataflow.SpannerToBigQueryUsingCDC.Options;
import com.google.cloud.dataflow.model.Order;
import com.google.cloud.dataflow.model.OrderMutation;
import com.google.cloud.dataflow.model.OrderMutation.OrderMutationCoder;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SpannerToBigQueryUsingCDCIT {

  @Rule
  public TestPipeline p = TestPipeline.create();

  private Options options;
  private Spanner spanner;
  private DatabaseClient dbClient;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.as(Options.class);
    options.setSpannerDatabaseId("fulfillment");
    options.setSpannerProjectId("event-processing-demo");
    options.setSpannerTableName("orders");
    options.setSpannerInstanceId("main");
    options.setSpannerOrdersStreamId("orders_changes");
    options.setBigQueryProjectId("event-processing-demo");
    options.setBigQueryDataset("spanner_to_bigquery");
    options.setBigQueryOrdersTableName("orders");

    spanner = SpannerOptions.newBuilder().build().getService();
    DatabaseId spannerDb = DatabaseId.of(options.getSpannerProjectId(),
        options.getSpannerInstanceId(), options.getSpannerDatabaseId());
    dbClient = spanner.getDatabaseClient(spannerDb);
  }

  @After
  public void tearDown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @Test
  public void testCDCIngestion() throws CannotProvideCoderException, NoSuchSchemaException {

    long orderNumber = getNextOrderNumber();

    Order[] orders = new Order[]{
        new Order(orderNumber++, "Phone"),
        new Order(orderNumber++, "Tablet"),
        new Order(orderNumber++, "Desktop"),
        new Order(orderNumber++, "Monitor")
    };

    Timestamp startTime = Timestamp.now();

    Timestamp endTime = createNewOrders(orders);

    Pipeline p = Pipeline.create(options);

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
        .withInclusiveStartAt(startTime)
        .withInclusiveEndAt(endTime));

    TableReference ordersTableReference = new TableReference();
    ordersTableReference.setProjectId(options.getBigQueryProjectId());
    ordersTableReference.setTableId(options.getBigQueryOrdersTableName());
    ordersTableReference.setDatasetId(options.getBigQueryDataset());

    dataChangeRecords
        .apply("To OrderMutations", ParDo.of(new DataChangeRecordToOrderMutation()))
        .apply("Save To BigQuery", BigQueryIO
            .<OrderMutation>write()
            .to(ordersTableReference)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
            .withFormatFunction(new OrderMutationToTableRow())
            .withRowMutationInformationFn(orderMutation -> orderMutation.getMutationInformation()));

    p.run();
  }

  private long getNextOrderNumber() {
    try (ResultSet resultSet =
        dbClient
            .singleUse() // Execute a single read or query against Cloud Spanner.
            .executeQuery(Statement.of("SELECT MAX(order_id) max_order_id FROM orders"))) {
      resultSet.next();
      if (resultSet.isNull("max_order_id")) {
        return 1;
      } else {
        return resultSet.getLong("max_order_id") + 1;
      }
    }
  }

  private Timestamp createNewOrders(Order[] orders) {
    List<Mutation> mutations = new ArrayList<>();
    for (Order order : orders) {
      mutations.add(
          Mutation.newInsertBuilder(options.getSpannerTableName())
              .set("order_id")
              .to(order.getId())
              .set("status")
              .to(order.getStatus())
              .set("description")
              .to(order.getDescription())
              .build());
    }

    return dbClient.write(mutations);
  }
}