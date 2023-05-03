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

public class StorageWriteAPIIngestion {
    public static final Logger log = Logger.getLogger(StorageWriteAPIIngestion.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            log.warning("Expecting 4 positional parameters - project id, dataset name, table name and records file name.");
            System.exit(-1);
        }
        String projectId = args[0];
        String datasetName = args[1];
        String tableName = args[2];
        String inputFile = args[3];

        try (DataProvider provider = new DataProvider(inputFile)) {

            StorageWriteAPIIngestion writer = new StorageWriteAPIIngestion();
//        writer.write(projectId, datasetName, tableName, WriteStream.Type.PENDING, new EventRecordGenerator().getData(20, "pending"));
//        writer.write(projectId, datasetName, tableName, WriteStream.Type.BUFFERED, new EventRecordGenerator().getData(30, "buffered"));
            writer.write(projectId, datasetName, tableName, WriteStream.Type.COMMITTED, provider.getData());
        }
    }

    private static Iterator<JSONObject> getJSONRecordsFromFile(String file) {
        return null;
    }

    public void write(String projectId, String datasetName, String tableName, WriteStream.Type writeType, Iterator<JSONObject> dataProvider) throws Descriptors.DescriptorValidationException, InterruptedException, IOException, ExecutionException {
        TableName parentTable = TableName.of(projectId, datasetName, tableName);

        log.info("Writing to " + parentTable + " using " + writeType + " mode");

        try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
            WriteStream writeStream = createWriteStream(writeType, parentTable, client);

            int recordCount = appendRows(writeStream, dataProvider);

            switch (writeType) {
                case PENDING:
                    FinalizeWriteStreamResponse finalizeResponse = client.finalizeWriteStream(writeStream.getName());
                    log.info("Rows finalized: " + finalizeResponse.getRowCount());

                    // Commit the streams.
                    BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder().setParent(parentTable.toString()).addWriteStreams(writeStream.getName()).build();
                    BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
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
                    FlushRowsRequest flushRowsRequest = FlushRowsRequest.newBuilder().setWriteStream(writeStream.getName()).setOffset(Int64Value.of(recordCount - 1)) // Advance the cursor to the latest record.
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
    }

    private static int appendRows(WriteStream writeStream, Iterator<JSONObject> dataProvider) throws InterruptedException, ExecutionException, IOException, Descriptors.DescriptorValidationException {
        int recordCount = 0;
        int batchSize = 10;
        try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build()) {
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

    private static WriteStream createWriteStream(WriteStream.Type writeType, TableName parentTable, BigQueryWriteClient client) {
        WriteStream streamPrototype = WriteStream.newBuilder().setType(writeType).build();

        CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder().setParent(parentTable.toString()).setWriteStream(streamPrototype).build();
        return client.createWriteStream(createWriteStreamRequest);
    }
}