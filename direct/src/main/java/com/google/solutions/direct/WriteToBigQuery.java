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

package com.google.solutions.direct;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.core.ApiFuture;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Streamingbuffer;
import com.google.api.services.bigquery.model.Table;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Int64Value;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class WriteToBigQuery {
  public static final Logger log = Logger.getLogger(WriteToBigQuery.class.getName());


  private Bigquery bigquery;

  public void printStreamingBufferStats(String where, TableName tableName)
      throws GeneralSecurityException, IOException {
    Bigquery bigquery = getBigquery();

    Table table = bigquery.tables().get(tableName.getProject(), tableName.getDataset(),
        tableName.getTable()).execute();
    Streamingbuffer streamingBuffer = table.getStreamingBuffer();
    if (streamingBuffer == null) {
      log.info(where + " No streaming buffer");
    } else {
      log.info(where + " Streaming buffer: " + streamingBuffer);
    }
  }

  private Bigquery getBigquery() throws IOException, GeneralSecurityException {
    if (bigquery != null) {
      return bigquery;
    }
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);

    Bigquery bigquery = new Bigquery.Builder(
        GoogleNetHttpTransport.newTrustedTransport(), new GsonFactory(), requestInitializer
    )
        .setApplicationName("BigQuery load test")
        .build();

    this.bigquery = bigquery;
    return bigquery;
  }

  public void write(String projectId, String datasetName, String tableName,
      WriteStream.Type writeType, Iterator<JSONObject> dataProvider)
      throws DescriptorValidationException, InterruptedException, IOException, ExecutionException, GeneralSecurityException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);

    log.info("Writing to " + parentTable + " using " + writeType + " mode");
    printStreamingBufferStats("Before writing", parentTable);

    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      WriteStream writeStream = createWriteStream(writeType, parentTable, client);

      int recordCount = appendRows(writeStream, dataProvider);

      switch (writeType) {
        case PENDING:
          printStreamingBufferStats("Before finalize", parentTable);
          FinalizeWriteStreamResponse finalizeResponse =
              client.finalizeWriteStream(writeStream.getName());
          log.info("Rows finalized: " + finalizeResponse.getRowCount());

          // Commit the streams.
          printStreamingBufferStats("After finalize, before commit", parentTable);
          BatchCommitWriteStreamsRequest commitRequest =
              BatchCommitWriteStreamsRequest.newBuilder()
                  .setParent(parentTable.toString())
                  .addWriteStreams(writeStream.getName())
                  .build();
          BatchCommitWriteStreamsResponse commitResponse =
              client.batchCommitWriteStreams(commitRequest);
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
          printStreamingBufferStats("Before flush", parentTable);
          FlushRowsRequest flushRowsRequest =
              FlushRowsRequest.newBuilder()
                  .setWriteStream(writeStream.getName())
                  .setOffset(
                      Int64Value.of(recordCount - 1)) // Advance the cursor to the latest record.
                  .build();
          FlushRowsResponse flushRowsResponse = client.flushRows(flushRowsRequest);
          log.info("Flushed up to offset " + flushRowsResponse.getOffset());
          printStreamingBufferStats("After flush, before finalize", parentTable);
          finalizeResponse = client.finalizeWriteStream(writeStream.getName());
          log.info("Rows finalized: " + finalizeResponse.getRowCount());
          break;

        case COMMITTED:
          printStreamingBufferStats("Before finalize", parentTable);
          finalizeResponse = client.finalizeWriteStream(writeStream.getName());
          log.info("Rows finalized: " + finalizeResponse.getRowCount());
          break;

        default:
          throw new RuntimeException("Unimplemented type " + writeType);
      }
    }
    printStreamingBufferStats("After completion", parentTable);
  }

  private static int appendRows(WriteStream writeStream, Iterator<JSONObject> dataProvider)
      throws InterruptedException, ExecutionException, DescriptorValidationException, IOException {
    int recordCount = 0;
    int batchSize = 10;
    try (JsonStreamWriter writer =
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema())
            .build()) {
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
      }
    }
    return recordCount;
  }

  private static WriteStream createWriteStream(WriteStream.Type writeType, TableName parentTable,
      BigQueryWriteClient client) {
    WriteStream streamPrototype = WriteStream.newBuilder().setType(writeType).build();

    CreateWriteStreamRequest createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(parentTable.toString())
            .setWriteStream(streamPrototype)
            .build();
    return client.createWriteStream(createWriteStreamRequest);
  }

  public static void main(String[] args)
      throws InterruptedException, DescriptorValidationException, IOException, ExecutionException, GeneralSecurityException {
    if (args.length != 3) {
      // TODO: convert to CLI parser if it gets more complex than this.
      log.warning(
          "WriteToBigQuery expects 3 positional parameters - project id, dataset name and table name.");
      System.exit(-1);
    }
    String projectId = args[0];
    String datasetName = args[1];
    String tableName = args[2];
    WriteToBigQuery writer = new WriteToBigQuery();
    writer.write(projectId, datasetName, tableName, WriteStream.Type.PENDING,
        new EventRecordGenerator().getData(20, "pending"));
    writer.write(projectId, datasetName, tableName, WriteStream.Type.BUFFERED,
        new EventRecordGenerator().getData(30, "buffered"));
    writer.write(projectId, datasetName, tableName, WriteStream.Type.COMMITTED,
        new EventRecordGenerator().getData(40, "committed"));
  }
}
