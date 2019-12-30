/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;


/**
 *
 */
public class PubsubDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubDStream.class);
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING))));

  private final String projectId;
  private final String subscriptionId;
  private SubscriberStub subscriber;
  private String serviceAccountPath;
  private JavaStreamingContext context;

  /**
   *
   * @param ssc
   * @param project
   * @param subscription
   * @param serviceAccountPath
   */
  public PubsubDStream(StreamingContext streamingContext, org.apache.spark.streaming.StreamingContext ssc,
                       String project, String subscription,
                       String serviceAccountPath) {
    super(ssc, scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class));
    this.projectId = project;
    this.subscriptionId = subscription;
    this.serviceAccountPath = serviceAccountPath;
    this.context = streamingContext.getSparkStreamingContext();
  }

  @Override
  public void start() {
    try {
      SubscriberStubSettings subscriberStubSettings =
        SubscriberStubSettings.newBuilder()
          .setTransportChannelProvider(
            SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
              .build()).setCredentialsProvider(
          FixedCredentialsProvider.create(GCPUtils.loadServiceAccountCredentials(serviceAccountPath))).build();
      subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
    } catch (Exception e) {
      LOG.error("Error occurred: " + e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    if (subscriber != null) {
      subscriber.close();
    }
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    // String projectId = "my-project-id";
    // String subscriptionId = "my-subscription-id";
    // int numOfMessages = 10;   // max number of messages to be pulled
    String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(10000)
        .setReturnImmediately(false) // return immediately if messages are not available
        .setSubscription(subscriptionName)
        .build();

    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
    List<ReceivedMessage> receivedMessagesList = pullResponse.getReceivedMessagesList();
    List<String> ackIds = new ArrayList<>(receivedMessagesList.size());
    List<StructuredRecord> records = new ArrayList<>(receivedMessagesList.size());

    for (ReceivedMessage message : receivedMessagesList) {
      PubsubMessage pubSubMessage = message.getMessage();
      // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
      HashMap<String, String> hashMap = new HashMap<>();
      if (pubSubMessage.getAttributesMap() != null) {
        hashMap.putAll(pubSubMessage.getAttributes());
      }

      records.add(StructuredRecord.builder(DEFAULT_SCHEMA)
        .set("message", pubSubMessage.getData())
        .set("id", pubSubMessage.getMessageId())
        .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime()))
        .set("attributes", hashMap)
        .build());
      ackIds.add(message.getAckId());
    }
    // acknowledge received messages
    AcknowledgeRequest acknowledgeRequest =
      AcknowledgeRequest.newBuilder()
        .setSubscription(subscriptionName)
        .addAllAckIds(ackIds)
        .build();
    // use acknowledgeCallable().futureCall to asynchronously perform this operation
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
    RDD<StructuredRecord> structuredRecordRDD = JavaRDD.<StructuredRecord>toRDD(
      context.sparkContext().parallelize(records));
    return Option.apply(structuredRecordRDD);
  }

  private ZonedDateTime getTimestamp(Timestamp timestamp) {
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }
}
