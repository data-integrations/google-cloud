/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.gcp.publisher;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Google Publisher sink to write to a Google PubSub topic.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GooglePublisher")
@Description("Writes to a Google Cloud Pub/Sub topic. Cloud Pub/Sub brings the scalability, " +
  "flexibility, and reliability of enterprise message-oriented middleware to the cloud. By providing many-to-many, " +
  "asynchronous messaging that decouples senders and receivers, it allows for secure and highly available " +
  "communication between independently written applications")
public class GooglePublisher extends BatchSink<StructuredRecord, NullWritable, Text> {
  private final Config config;

  @SuppressWarnings("unused")
  public GooglePublisher(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    Configuration configuration = new Configuration();
    PubSubOutputFormat.configure(configuration, config);
    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(PubSubOutputFormat.class, configuration)));
    // record field level lineage information
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null && inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      FieldOperation operation = new FieldWriteOperation("Write", "Wrote to Google Cloud Pub/Sub.",
                                                         EndPoint.of(context.getNamespace(), config.referenceName),
                                                         inputSchema.getFields().stream().map(Schema.Field::getName)
                                                           .collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String body = StructuredRecordStringConverter.toJsonString(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(body)));
  }

  /**
   * PubSub Publisher config
   */
  public static class Config extends GCPReferenceSinkConfig {
    @Description("Cloud Pub/Sub topic to publish records to")
    @Macro
    public String topic;

    // batching options
    @Description("Maximum count of messages in a batch. The default value is 100.")
    @Macro
    @Nullable
    public Long messageCountBatchSize;

    @Description("Maximum size of a batch in kilo bytes. The default value is 1KB.")
    @Macro
    @Nullable
    public Long requestThresholdKB;

    @Description("Maximum delay in milli-seconds for publishing the batched messages. The default value is 1 ms.")
    @Macro
    @Nullable
    public Long publishDelayThresholdMillis;

    @Description("Maximum number of message publishing failures to tolerate per partition " +
      "before the pipeline will be failed. The default value is 0.")
    @Macro
    @Nullable
    public Long errorThreshold;

    @Description("Maximum amount of time in seconds to spend retrying publishing failures. " +
      "The default value is 30 seconds.")
    @Macro
    @Nullable
    public Integer retryTimeoutSeconds;


    public void validate() {
      if (!containsMacro("messageCountBatchSize") && messageCountBatchSize != null && messageCountBatchSize < 1) {
        throw new IllegalArgumentException("Maximum count of messages in a batch should be positive for Pub/Sub");
      }
      if (!containsMacro("requestThresholdKB") && requestThresholdKB != null && requestThresholdKB < 1) {
        throw new IllegalArgumentException("Maximum size of a batch (KB) should be positive for Pub/Sub");
      }
      if (!containsMacro("publishDelayThresholdMillis") &&
        publishDelayThresholdMillis != null && publishDelayThresholdMillis < 1) {
        throw new IllegalArgumentException("Delay threshold for publishing a batch should be positive for Pub/Sub");
      }
      if (!containsMacro("errorThreshold") && errorThreshold != null && errorThreshold < 0) {
        throw new IllegalArgumentException("Error threshold for publishing should be zero or more for Pub/Sub");
      }
      if (!containsMacro("retryTimeoutSeconds") && retryTimeoutSeconds != null && retryTimeoutSeconds < 1) {
        throw new IllegalArgumentException("Max retry timeout for retrying failed publish " +
                                             "should be positive for Pub/Sub");
      }
    }

    public long getRequestBytesThreshold() {
      return requestThresholdKB == null ? 1024 : requestThresholdKB * 1024;
    }

    public long getMessageCountBatchSize() {
      return messageCountBatchSize == null ? 100 : messageCountBatchSize;
    }

    public long getPublishDelayThresholdMillis() {
      return publishDelayThresholdMillis == null ? 1 : publishDelayThresholdMillis;
    }

    public long getErrorThreshold() {
      return errorThreshold == null ? 0 : errorThreshold;
    }

    public int getRetryTimeoutSeconds() {
      return retryTimeoutSeconds == null ? 30 : retryTimeoutSeconds;
    }
  }
}
