package com.google.solutions.direct;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1beta2.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1beta2.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1beta2.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1beta2.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1beta2.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1beta2.StorageError;
import com.google.cloud.bigquery.storage.v1beta2.TableName;
import com.google.cloud.bigquery.storage.v1beta2.WriteStream;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import org.json.JSONArray;
import org.json.JSONObject;

public class WriteToBigQuery {

  public static void writePendingStream(String projectId, String datasetName, String tableName)
      throws DescriptorValidationException, InterruptedException, IOException {
    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      // Initialize a write streamPrototype for the specified table.
      // For more information on WriteStream.Type, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/WriteStream.Type.html
      WriteStream streamPrototype = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();
      TableName parentTable = TableName.of(projectId, datasetName, tableName);
      CreateWriteStreamRequest createWriteStreamRequest =
          CreateWriteStreamRequest.newBuilder()
              .setParent(parentTable.toString())
              .setWriteStream(streamPrototype)
              .build();
      WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

      // Use the JSON streamPrototype writer to send records in JSON format.
      // For more information about JsonStreamWriter, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/JsonStreamWriter.html
      try (JsonStreamWriter writer =
          JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema())
              .build()) {
        // Write two batches to the streamPrototype, each with 10 JSON records.
        for (int i = 0; i < 2; i++) {
          // Create a JSON object that is compatible with the table schema.
          JSONArray jsonArr = new JSONArray();
          for (int j = 0; j < 10; j++) {
            JSONObject record = new JSONObject();
            record.put("user_id", String.format("user %03d-%03d", i, j));
            record.put("request_ts", Instant.now().toEpochMilli());
            record.put("bytes_sent", 234);
            record.put("bytes_received", 567);
            record.put("dst_ip", "1.2.3.4");
            record.put("dst_port", 8080);
            jsonArr.put(record);
          }
          ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
          AppendRowsResponse response = future.get();
        }
        FinalizeWriteStreamResponse finalizeResponse =
            client.finalizeWriteStream(writeStream.getName());
        System.out.println("Rows written: " + finalizeResponse.getRowCount());
      }

      // Commit the streams.
      BatchCommitWriteStreamsRequest commitRequest =
          BatchCommitWriteStreamsRequest.newBuilder()
              .setParent(parentTable.toString())
              .addWriteStreams(writeStream.getName())
              .build();
      BatchCommitWriteStreamsResponse commitResponse =
          client.batchCommitWriteStreams(commitRequest);
      // If the response does not have a commit time, it means the commit operation failed.
      if (commitResponse.hasCommitTime() == false) {
        for (StorageError err : commitResponse.getStreamErrorsList()) {
          System.out.println(err.getErrorMessage());
        }
        throw new RuntimeException("Error committing the streams");
      }
      System.out.println("Appended and committed records successfully.");
    } catch (ExecutionException e) {
      // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
      // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
      // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
      System.out.println(e);
    }
  }
  public static void main(String[] args)
      throws InterruptedException, DescriptorValidationException, IOException {
    writePendingStream("event-processing-demo", "bigquery_io", "events");
  }
}
