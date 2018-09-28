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
package co.cask.gcp.publisher.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import com.google.cloud.Timestamp;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials.Builder;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
    );

  private SubscriberConfig config;

  public GoogleSubscriber(SubscriberConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    SparkGCPCredentials credentials;
    if (serviceAccountFilePath != null) {
      // if credential file is provided then use it to build SparkGCPCredentials
      LOG.debug("Building credentials from specified service account file.");
      credentials = new Builder().jsonServiceAccount(serviceAccountFilePath).build();
    } else {
      // this will use the application default credential which is available when running on google cloud
      LOG.debug("No service account file specified. Building credentials using Application Default Credentials.");
      credentials = new Builder().build();
    }

    JavaReceiverInputDStream<SparkPubsubMessage> pubSubMessages =
      PubsubUtils.createStream(streamingContext.getSparkStreamingContext(), config.getProject(), config.topic,
                               config.subscription, credentials, StorageLevel.MEMORY_ONLY());

    return pubSubMessages.map((Function<SparkPubsubMessage, StructuredRecord>) pubSubMessage -> StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("message", pubSubMessage.getData())
      .set("id", pubSubMessage.getMessageId())
      .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime())).build());
  }

  private ZonedDateTime getTimestamp(String publishTime) {
    Timestamp timestamp = Timestamp.parseTimestamp(publishTime);
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  public static class SubscriberConfig extends GCPReferenceSourceConfig {

    @Description("Cloud Pub/Sub subscription to read from. If the subscription does not exist it will be " +
      "automatically created if a topic is specified. Messages published before the subscription was created will " +
      "not be read.")
    @Macro
    private String subscription;

    @Description("Cloud Pub/Sub topic to subscribe messages from. If a topic is provided and the given subscription " +
      "does not exists it will be created. Note: If a subscription does not exists and is created only the messages " +
      "arrived after the creation of subscription will be received.")
    @Macro
    @Nullable
    private String topic;
  }
}
