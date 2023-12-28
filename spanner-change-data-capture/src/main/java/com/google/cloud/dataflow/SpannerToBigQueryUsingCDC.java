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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SpannerToBigQueryUsingCDC {
  public interface Options extends GcpOptions {
    String getSpannerProjectId();
    void setSpannerProjectId(String value);

    String getSpannerInstanceId();
    void setSpannerInstanceId(String value);

    String getSpannerDatabaseId();
    void setSpannerDatabaseId(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.as(Options.class);
    Pipeline p = Pipeline.create(options);
    run(options, p);
  }

  private static void run(Options options, Pipeline p) {
    SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId(options.getSpannerProjectId())
        .withInstanceId(options.getSpannerInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId());

    Timestamp startTime = Timestamp.now();
    Timestamp endTime = Timestamp.ofTimeSecondsAndNanos(
        startTime.getSeconds() + (10 * 60),
        startTime.getNanos()
    );

    SpannerIO
        .readChangeStream()
        .withSpannerConfig(spannerConfig)
        .withChangeStreamName("my-change-stream")
        // .withMetadataInstance("my-meta-instance-id")
        // .withMetadataDatabase("my-meta-database-id")
        // .withMetadataTable("my-meta-table-name")
        .withRpcPriority(RpcPriority.MEDIUM)
        .withInclusiveStartAt(startTime)
        .withInclusiveEndAt(endTime);

    p.run();
  }

}
