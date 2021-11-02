package com.google.solutions.direct;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1beta2.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1beta2.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1beta2.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1beta2.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1beta2.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1beta2.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1beta2.StorageError;
import com.google.cloud.bigquery.storage.v1beta2.TableName;
import com.google.cloud.bigquery.storage.v1beta2.WriteStream;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Int64Value;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import org.json.JSONArray;
import org.json.JSONObject;

public class WriteToBigQuery {

  public static void write(String projectId, String datasetName, String tableName,
      WriteStream.Type writeType)
      throws DescriptorValidationException, InterruptedException, IOException, ExecutionException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);

    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      WriteStream writeStream = createWriteStream(writeType, parentTable, client);

      int recordCount = appendRows(writeType, writeStream);

      switch (writeType) {
        case PENDING:
          FinalizeWriteStreamResponse finalizeResponse =
              client.finalizeWriteStream(writeStream.getName());
          System.out.println("Rows finalized: " + finalizeResponse.getRowCount());

          // Commit the streams.
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
              System.out.println(err.getErrorMessage());
            }
            throw new RuntimeException("Error committing the streams");
          }
          System.out.println("Appended and committed records successfully.");
          break;

        case BUFFERED:
          FlushRowsRequest flushRowsRequest =
              FlushRowsRequest.newBuilder()
                  .setWriteStream(writeStream.getName())
                  .setOffset(
                      Int64Value.of(recordCount - 1)) // Advance the cursor to the latest record.
                  .build();
          FlushRowsResponse flushRowsResponse = client.flushRows(flushRowsRequest);
          System.out.println("Flushed up to offset " + flushRowsResponse.getOffset());
          break;

        case COMMITTED:
          System.out.println("Auto committed " + recordCount + " rows.");
          break;

        default:
          throw new RuntimeException("Unimplemented type " + writeType);
      }
    }
  }

  private static int appendRows(WriteStream.Type writeType, WriteStream writeStream)
      throws InterruptedException, ExecutionException, DescriptorValidationException, IOException {
    int recordCount = 0;
    try (JsonStreamWriter writer =
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema())
            .build()) {
      // Write two batches to the streamPrototype, each with 10 JSON records.
      for (int i = 0; i < 2; i++) {
        // Create a JSON object that is compatible with the table schema.
        JSONArray jsonArr = new JSONArray();
        for (int j = 0; j < 10; j++) {
          JSONObject record = new JSONObject();
          record.put("user_id",
              String.format("user %s-%03d-%03d", writeType.toString().toLowerCase(), i, j));
          record.put("request_ts", Instant.now().toEpochMilli());
          record.put("bytes_sent", 234);
          record.put("bytes_received", 567);
          record.put("dst_ip", "1.2.3.4");
          record.put("dst_port", 8080);
          jsonArr.put(record);
          ++recordCount;
        }
        ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
        AppendRowsResponse response = future.get();
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
    WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);
    return writeStream;
  }

  public static void main(String[] args)
      throws InterruptedException, DescriptorValidationException, IOException, ExecutionException {
    String projectId = "event-processing-demo";
    String datasetName = "datasetName";
    String tableName = "tableName";
    write(projectId, datasetName, tableName, WriteStream.Type.PENDING);
    write(projectId, datasetName, tableName, WriteStream.Type.BUFFERED);
    write(projectId, datasetName, tableName, WriteStream.Type.COMMITTED);
  }
}
