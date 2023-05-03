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

package com.google.bigquery.ingestion;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int64Value;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Standalone application to demo basic usage of the Storage Write API.
 * <p>
 * Note that this is not production quality code. Error handling, efficient usage of the
 * bidirectional gRPC are not implemented in this demo.
 */
public class StorageWriteAPIIngestion {

  public static final Logger log = Logger.getLogger(StorageWriteAPIIngestion.class.getName());
  public static final String USAGE =
      "Expecting 4 required positional parameters - project id, dataset name, table name and records file name.\n"
          + "Optional fifth parameter - stream mode: committed | pending | buffered | default";

  public static void main(String[] args) throws Exception {
    if (args.length < 4 || args.length > 5) {
      log.warning(USAGE);
      System.exit(-1);
    }
    String projectId = args[0];
    String datasetName = args[1];
    String tableName = args[2];
    String inputFile = args[3];

    WriteStream.Type streamType = null;

    if (args.length == 5) {
      switch (args[4]) {
        case "committed":
          streamType = WriteStream.Type.COMMITTED;
          break;

        case "pending":
          streamType = WriteStream.Type.PENDING;
          break;

        case "buffered":
          streamType = WriteStream.Type.BUFFERED;
          break;

        case "default":
          break;

        default:
          log.warning("Unrecognized streaming mode: " + args[4]);
          log.warning(USAGE);
          System.exit(-1);
      }
    }

    try (JsonNewLineDelimitedFileDataProvider provider = new JsonNewLineDelimitedFileDataProvider(
        inputFile)) {
      StorageWriteAPIIngestion writer = new StorageWriteAPIIngestion();
      writer.write(projectId, datasetName, tableName, streamType, provider.getData());
    }
  }

  public void write(String projectId, String datasetName, String tableName,
      WriteStream.Type writeType, Iterator<JSONObject> dataProvider)
      throws Descriptors.DescriptorValidationException, InterruptedException, IOException, ExecutionException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);
    log.info("Writing to " + parentTable + " using " +
        (writeType == null ? "default stream" : writeType.toString()) + " mode");

    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      WriteStream writeStream =
          writeType == null ? null : createWriteStream(writeType, parentTable, client);

      int recordCount = appendRows(
          writeStream == null ? parentTable.toString() : writeStream.getName(), client,
          dataProvider);

      onCompletionOfAppends(writeType, parentTable, client, writeStream, recordCount);
    }
  }

  private static void onCompletionOfAppends(WriteStream.Type writeType, TableName parentTable,
      BigQueryWriteClient client, WriteStream writeStream, int recordCount) {
    if (writeType == null) {
      // This means we are writing to the default stream and nothing else needs to be done.
      log.info("Rows added: " + recordCount);
      return;
    }
    switch (writeType) {
      case PENDING:
        FinalizeWriteStreamResponse finalizeResponse = client.finalizeWriteStream(
            writeStream.getName());
        log.info("Rows finalized: " + finalizeResponse.getRowCount());

        // Commit the streams.
        BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
            .setParent(parentTable.toString()).addWriteStreams(writeStream.getName()).build();
        BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(
            commitRequest);
        // If the response does not have a commit time, it means the commit operation failed.
        if (!commitResponse.hasCommitTime()) {
          for (StorageError err : commitResponse.getStreamErrorsList()) {
            log.warning(err.getErrorMessage());
          }
          throw new RuntimeException("Error committing the streams");
        }
        log.info("Appended and committed records successfully.");
        break;

      case BUFFERED:
        FlushRowsRequest flushRowsRequest = FlushRowsRequest.newBuilder()
            .setWriteStream(writeStream.getName())
            .setOffset(Int64Value.of(recordCount - 1)) // Advance the cursor to the latest record.
            .build();
        FlushRowsResponse flushRowsResponse = client.flushRows(flushRowsRequest);
        log.info("Flushed up to offset " + flushRowsResponse.getOffset());
        finalizeResponse = client.finalizeWriteStream(writeStream.getName());
        log.info("Rows finalized: " + finalizeResponse.getRowCount());
        break;

      case COMMITTED:
        finalizeResponse = client.finalizeWriteStream(writeStream.getName());
        log.info("Rows finalized: " + finalizeResponse.getRowCount());
        break;

      default:
        throw new RuntimeException("Unimplemented type " + writeType);
    }
  }

  private static int appendRows(String writeDestination, BigQueryWriteClient client,
      Iterator<JSONObject> dataProvider)
      throws InterruptedException, ExecutionException, IOException, Descriptors.DescriptorValidationException {
    int recordCount = 0;
    int batchSize = 10;
    try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(writeDestination, client).build()) {
      JSONArray jsonArr = new JSONArray();
      while (dataProvider.hasNext()) {
        JSONObject record = dataProvider.next();
        ++recordCount;
        jsonArr.put(record);
        if (jsonArr.length() % batchSize == 0) {
          ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
          future.get();

          jsonArr = new JSONArray();
        }
      }
      if (jsonArr.length() > 0) {
        ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
        var response = future.get();
        if (response.getRowErrorsCount() > 0) {
          throw new RuntimeException("Errors appending rows:" + response.getRowErrorsList());
        }
      }
    }
    return recordCount;
  }

  private static WriteStream createWriteStream(WriteStream.Type writeType, TableName parentTable,
      BigQueryWriteClient client) {
    WriteStream streamPrototype = WriteStream.newBuilder().setType(writeType).build();

    CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
        .setParent(parentTable.toString()).setWriteStream(streamPrototype).build();
    return client.createWriteStream(createWriteStreamRequest);
  }
}