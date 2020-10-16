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
package io.cdap.plugin.gcp.publisher.source;

import com.google.cloud.Timestamp;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.reflect.ClassTag;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read messages from Google PubSub.")
public class GoogleSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSubscriber.class);
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                            Schema.of(Schema.Type.STRING)))
    );

  private SubscriberConfig config;

  public GoogleSubscriber(SubscriberConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    Schema schema = DEFAULT_SCHEMA;
    // record dataset lineage
    context.registerLineage(config.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from Pub/Sub",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {
    String serviceAccount = config.getServiceAccount();
    SparkGCPCredentials credentials = new GCPCredentialsProvider(serviceAccount, config.isServiceAccountFilePath());

    boolean autoAcknowledge = true;
    if (streamingContext.isPreviewEnabled()) {
      autoAcknowledge = false;
    }
    Option<String> topic = Option.apply(config.topic);
    ReceiverInputDStream<SparkPubsubMessage> stream =
      PubsubUtils.createStream(streamingContext.getSparkStreamingContext().ssc(), config.getProject(),
                               topic, config.subscription, credentials,
                               StorageLevel.MEMORY_ONLY(), autoAcknowledge);
    ClassTag<SparkPubsubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(SparkPubsubMessage.class);
    JavaReceiverInputDStream<SparkPubsubMessage> pubSubMessages = new JavaReceiverInputDStream<>(stream, tag);

    return pubSubMessages.map(pubSubMessage -> {
      // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
      HashMap<String, String> hashMap = new HashMap<>();
      if (pubSubMessage.getAttributes() != null) {
        hashMap.putAll(pubSubMessage.getAttributes());
      }

      return StructuredRecord.builder(DEFAULT_SCHEMA)
              .set("message", pubSubMessage.getData())
              .set("id", pubSubMessage.getMessageId())
              .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime()))
              .set("attributes", hashMap)
              .build();
    });
  }

  private ZonedDateTime getTimestamp(String publishTime) {
    Timestamp timestamp = Timestamp.parseTimestamp(publishTime);
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  /**
   * Configuration class for the subscriber source.
   */
  public static class SubscriberConfig extends GCPReferenceSourceConfig {

    @Description("Cloud Pub/Sub subscription to read from. If a subscription with the specified name does not " +
      "exist, it will be automatically created if a topic is specified. Messages published before the subscription " +
      "was created will not be read.")
    @Macro
    private String subscription;

    @Description("Cloud Pub/Sub topic to create a subscription on. This is only used when the specified  " +
      "subscription does not already exist and needs to be automatically created. If the specified " +
      "subscription already exists, this value is ignored.")
    @Macro
    @Nullable
    private String topic;

    public void validate(FailureCollector collector) {
      super.validate(collector);
      String regAllowedChars = "[A-Za-z0-9-.%~+_]*$";
      String regStartWithLetter = "[A-Za-z]";
      if (!getSubscription().matches(regAllowedChars)) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Unexpected Character. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().startsWith("goog")) {
        collector.addFailure("Subscription Name does not match naming convention.",
          " Cannot Start with String goog. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (!getSubscription().substring(0, 1).matches(regStartWithLetter)) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Name must start with a letter. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().length() < 3  || getSubscription().length() > 255) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Character Length must be between 3 and 255 characters. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      collector.getOrThrowException();
    }

    public String getSubscription() {
      return subscription;
    }
  }
}
