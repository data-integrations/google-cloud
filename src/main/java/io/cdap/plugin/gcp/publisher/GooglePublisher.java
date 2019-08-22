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

package io.cdap.plugin.gcp.publisher;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
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
  public void prepareRun(BatchSinkContext context) throws IOException {
    config.validate();

    TopicAdminSettings.Builder topicAdminSettings = TopicAdminSettings.newBuilder();
    String serviceAccountPath = config.getServiceAccountFilePath();
    if (serviceAccountPath != null) {
      topicAdminSettings.setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(serviceAccountPath));
    }
    String projectId = config.getProject();
    ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, config.topic);

    if (!context.isPreviewEnabled()) {
      try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings.build())) {
        try {
          topicAdminClient.getTopic(projectTopicName);
        } catch (NotFoundException e) {
          try {
            topicAdminClient.createTopic(projectTopicName);
          } catch (AlreadyExistsException e1) {
            // can happen if there is a race condition. Ignore this error since all that matters is the topic exists
          } catch (ApiException e1) {
            throw new IOException(
              String.format("Could not auto-create topic '%s' in project '%s'. "
                              + "Please ensure it is created before running the pipeline, "
                              + "or ensure that the service account has permission to create the topic.",
                            config.topic, projectId), e);
          }
        }
      }
    }

    Schema inputSchema = context.getInputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);

    Configuration configuration = new Configuration();
    PubSubOutputFormat.configure(configuration, config);
    context.addOutput(Output.of(config.getReferenceName(),
                                new SinkOutputFormatProvider(PubSubOutputFormat.class, configuration)));

    // record field level lineage information
    if (inputSchema != null && inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Pub/Sub.",
                                  inputSchema.getFields().stream().map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
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
    private String topic;

    // batching options
    @Description("Maximum count of messages in a batch. The default value is 100.")
    @Macro
    @Nullable
    private Long messageCountBatchSize;

    @Description("Maximum size of a batch in kilo bytes. The default value is 1KB.")
    @Macro
    @Nullable
    private Long requestThresholdKB;

    @Description("Maximum delay in milli-seconds for publishing the batched messages. The default value is 1 ms.")
    @Macro
    @Nullable
    private Long publishDelayThresholdMillis;

    @Description("Maximum number of message publishing failures to tolerate per partition " +
      "before the pipeline will be failed. The default value is 0.")
    @Macro
    @Nullable
    private Long errorThreshold;

    @Description("Maximum amount of time in seconds to spend retrying publishing failures. " +
      "The default value is 30 seconds.")
    @Macro
    @Nullable
    private Integer retryTimeoutSeconds;


    public Config(String referenceName, String topic, @Nullable Long messageCountBatchSize,
                  @Nullable Long requestThresholdKB, @Nullable Long publishDelayThresholdMillis,
                  @Nullable Long errorThreshold, @Nullable Integer retryTimeoutSeconds) {
      this.referenceName = referenceName;
      this.topic = topic;
      this.messageCountBatchSize = messageCountBatchSize;
      this.requestThresholdKB = requestThresholdKB;
      this.publishDelayThresholdMillis = publishDelayThresholdMillis;
      this.errorThreshold = errorThreshold;
      this.retryTimeoutSeconds = retryTimeoutSeconds;
    }

    public void validate() {
      // TODO: (vinisha) add failure collector
      super.validate(null);
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

    public String getTopic() {
      return topic;
    }

    @Nullable
    public Long getRequestThresholdKB() {
      return requestThresholdKB;
    }
  }
}
