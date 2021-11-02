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
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import org.json.JSONArray;
import org.json.JSONObject;

public class WriteToBigQuery {

  public static void write(String projectId, String datasetName, String tableName,
      WriteStream.Type writeType, Iterator<JSONObject> dataProvider)
      throws DescriptorValidationException, InterruptedException, IOException, ExecutionException {
    TableName parentTable = TableName.of(projectId, datasetName, tableName);

    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      WriteStream writeStream = createWriteStream(writeType, parentTable, client);

      int recordCount = appendRows(writeStream, dataProvider);

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
        future.get();
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
      throws InterruptedException, DescriptorValidationException, IOException, ExecutionException {
    if (args.length != 3) {
      // TODO: convert to CLI parser if it gets more complex than this.
      System.err.println(
          "WriteToBigQuery expects 3 positional parameters - project id, dataset name and table name.");
      System.exit(-1);
    }
    String projectId = args[0];
    String datasetName = args[1];
    String tableName = args[2];
    write(projectId, datasetName, tableName, WriteStream.Type.PENDING,
        new EventRecordGenerator().getData(20, "pending"));
    write(projectId, datasetName, tableName, WriteStream.Type.BUFFERED,
        new EventRecordGenerator().getData(30, "buffered"));
    write(projectId, datasetName, tableName, WriteStream.Type.COMMITTED,
        new EventRecordGenerator().getData(40, "committed"));
  }
}
