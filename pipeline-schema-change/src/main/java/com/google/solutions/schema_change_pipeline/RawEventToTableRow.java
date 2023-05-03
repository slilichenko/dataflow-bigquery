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

package com.google.solutions.schema_change_pipeline;import com.google.api.client.json.GenericJson;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawEventToTableRow extends DoFn<String, TableRow> {
  private final static long serialVersionUID = 1L;
  public static final Logger LOG = LoggerFactory.getLogger(
      RawEventToTableRow.class);
  private GsonFactory gson;

  @StartBundle
  public void initGson() {
    gson = GsonFactory.getDefaultInstance();
  }

  @ProcessElement
  public void process(@Element String rawEvent, OutputReceiver<TableRow> out) {
    BigQueryWritePipeline.totalEvents.inc();
    GenericJson event;
    try {
      event = gson.createJsonParser(rawEvent).parse(GenericJson.class);
    } catch (IOException e) {
      BigQueryWritePipeline.failedEvents.inc();
      LOG.error("Failed to parse payload: ", e);
      return;
    }
    TableRow row = new TableRow();
    String newlyAdded = "newly_added";
    if(event.containsKey(newlyAdded)) {
      Object value = event.get(newlyAdded);
      row.set(newlyAdded, value);
      LOG.debug("Adding row with new column and value: " + value);
    }

    row.set("request_ts", Instant.now().toString());
    row.set("dst_ip", event.get("destination_ip"));
    row.set("dst_port", ((BigDecimal)event.get("destination_port")).longValue());
    row.set("src_ip", event.get("source_ip"));
    if(event.containsKey("random_id")) {
      row.set("random_id", event.get("random_id"));
    }
    row.set("bytes_sent", ((BigDecimal)event.get("bytes_sent")).longValue());
    row.set("bytes_received", ((BigDecimal)event.get("bytes_received")).longValue());
    row.set("user_id", event.get("user"));
    row.set("process_name", event.get("process"));

    out.output(row);
  }
}
