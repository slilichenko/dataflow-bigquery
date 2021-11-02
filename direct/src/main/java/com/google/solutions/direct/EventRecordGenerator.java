package com.google.solutions.direct;

import java.time.Instant;
import java.util.Iterator;
import org.json.JSONObject;

public class EventRecordGenerator {

  public Iterator<JSONObject> getData(final int numberOfRecords, final String userNamePrefix) {
    return new Iterator<>() {
      private int recordCount = 0;

      @Override
      public boolean hasNext() {
        return recordCount < numberOfRecords;
      }

      @Override
      public JSONObject next() {
        JSONObject record = new JSONObject();
        record.put("user_id", String.format("%s-%04d", userNamePrefix, recordCount));
        var now = Instant.now();
        record.put("request_ts", now.toEpochMilli() * 1000 + now.getNano() / 1000); // microseconds
        record.put("bytes_sent", 234);
        record.put("bytes_received", 567);
        record.put("dst_ip", "1.2.3.4");
        record.put("dst_port", 8080);

        ++recordCount;
        return record;
      }
    };
  }
}
