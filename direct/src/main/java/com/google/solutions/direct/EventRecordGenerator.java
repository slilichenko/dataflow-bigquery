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
        record.put("bytes_sent", 234.23);
        record.put("bytes_received", 567);
        record.put("dst_ip", "1.2.3.4");
        record.put("dst_port", 8080);

        ++recordCount;
        return record;
      }
    };
  }
}
