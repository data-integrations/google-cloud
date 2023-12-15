/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.utils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents PubSub client.
 */
public class PubSubClient {

  private static final Logger logger = LoggerFactory.getLogger(PubSubClient.class);

  public static Topic createTopic(String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      return topicAdminClient.createTopic(topicName);
    }
  }

  /**
   * Creates a subscription for the specified subscription ID and topic ID.
   *
   * @param subscriptionId The ID of the subscription to be created.
   * @param topicId The ID of the topic to which the subscription is associated.
   * @throws IOException If an I/O error occurs while interacting with the Subscription Admin API.
   */
  public static void createSubscription(String subscriptionId, String topicId) throws IOException {
    ProjectSubscriptionName subscriptionName = null;
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient
      .create(SubscriptionAdminSettings.newBuilder().build())) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      subscriptionName = ProjectSubscriptionName.of(PluginPropertyUtils.
                                                      pluginProp(ConstantsUtil.PROJECT_ID), subscriptionId);
      subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 60);
      logger.info("Subscription created: " + subscriptionName);

    } catch (AlreadyExistsException e) {
      Assert.assertTrue("Subscription is null", subscriptionName != null);
      logger.info("Subscription already exists: " + subscriptionName);
    }
  }

  public static void deleteTopic(String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      topicAdminClient.deleteTopic(topicName);
    }
  }

  public static Topic getTopic(String topicId) throws IOException {
    Topic topic;
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      topic = topicAdminClient.getTopic(topicName);
      return topic;
    }
  }

  public static String getTopicCmekKey(String topicId) throws IOException {
    return getTopic(topicId).getKmsKeyName();
  }

  /**
   * Publishes messages to a specified Pub/Sub topic with an error handler to handle success or failure asynchronously.
   *
   * @param projectId The ID of the Google Cloud Platform project.
   * @param topicId The ID of the Pub/Sub topic to which messages are published.
   * @throws IOException If an I/O error occurs during the publishing process.
   */
  public static void publishMessagesWithPubsub(String projectId, String topicId, List<String> messages)
    throws IOException, InterruptedException {
    TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topicName).build();
      for (final String message : messages) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> future = publisher.publish(pubsubMessage);
        // Adding an asynchronous callback to handle success / failure
        ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

          @Override
          public void onFailure(Throwable throwable) {
            if (throwable instanceof ApiException) {
              ApiException apiException = ((ApiException) throwable);
              // details on the API exception
              logger.info(apiException.getStatusCode().getCode().toString());
              logger.info(Boolean.toString(apiException.isRetryable()));
            }
            logger.info("Error publishing message : " + message);
          }

          @Override
          public void onSuccess(String messageId) {
            // Once published, returns server-assigned message ids (unique within the topic)
            logger.info("Published message ID: " + messageId);
          }
        }, MoreExecutors.directExecutor());
      }
    } finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }

  public static void subscribeAsync(String projectId, String subscriptionId) {
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of
      (PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), subscriptionId);
    // Instantiate an asynchronous message receiver.
    MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
      // Handle incoming message, then ack the received message.
      logger.info("Id: " + message.getMessageId());
      logger.info("Data: " + message.getData().toStringUtf8());
      consumer.ack();
    };
    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
      // Start the subscriber.
      subscriber.startAsync().awaitRunning();
      logger.info("Listening for messages on %s:\n", subscriptionName);
      // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
      subscriber.awaitTerminated(300, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      // Shut down the subscriber after 30s. Stop receiving messages.
      subscriber.stopAsync();
    }
  }

  public static Schema createAvroSchema(String schemaId, String avscFile) throws IOException {
    ProjectName projectName = ProjectName.of(PluginPropertyUtils.pluginProp("projectId"));
    SchemaName schemaName = SchemaName.of(PluginPropertyUtils.pluginProp("projectId"), schemaId);

    // Read an Avro schema file formatted in JSON as a string.
    String avscSource = new String(Files.readAllBytes(Paths.get(avscFile)));
    try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create()) {
      Schema schema = schemaServiceClient.createSchema(projectName, Schema.newBuilder().
        setName(schemaName.toString()).setType(Schema.Type.AVRO).setDefinition(avscSource).build(), schemaId);
      logger.info("Created a schema using an Avro schema:\n" + schema);
      return schema;
    } catch (AlreadyExistsException e) {
      logger.info(schemaName + "already exists.");
      return null;
    }
  }

  public static void createTopicWithSchema(String topicId, String schemaId, Encoding encoding)
    throws IOException, InterruptedException {
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(10);
    TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp("projectId"), topicId);
    SchemaName schemaName = SchemaName.of(PluginPropertyUtils.pluginProp("projectId"), schemaId);
    SchemaSettings schemaSettings = SchemaSettings.newBuilder().setSchema
      (schemaName.toString()).setEncoding(encoding).build();
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      Topic topic = topicAdminClient.createTopic(Topic.newBuilder().setName
        (topicName.toString()).setSchemaSettings(schemaSettings).build());
      logger.info("Created topic with schema: " + topic.getName());
    } catch (AlreadyExistsException e) {
      logger.info(schemaName + "already exists.");
    }
  }

  public static void publishAvroRecords(String projectId, String topicId) throws
    IOException, ExecutionException, InterruptedException {
    Encoding encoding = null;
    TopicName topicName = TopicName.of(projectId, topicId);
    // Get the topic encoding type.
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      encoding = topicAdminClient.getTopic(topicName).getSchemaSettings().getEncoding();
    }
    // Instantiate an avro-tools-generated class defined in `us-states.avsc`.
    State state = State.newBuilder().setName("Alaska").setPostAbbr("AK").build();
    Publisher publisher = null;
    block:
    try {
      publisher = Publisher.newBuilder(topicName).build();
      // Prepare to serialize the object to the output stream.
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      Encoder encoder = null;
      // Prepare an appropriate encoder for publishing to the topic.
      switch (encoding) {
        case BINARY:
          logger.info("Preparing a BINARY encoder...");
          encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /*reuse=*/ null);
          break;
        case JSON:
          logger.info("Preparing a JSON encoder...");
          encoder = EncoderFactory.get().jsonEncoder(State.getClassSchema(), byteStream);
          break;
        default:
          break block;
      }
      // Encode the object and write it to the output stream.
      state.customEncode(encoder);
      encoder.flush();
      // Publish the encoded object as a Pub/Sub message.
      ByteString data = ByteString.copyFrom(byteStream.toByteArray());
      PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
      logger.info("Publishing message: " + message);

      ApiFuture<String> future = publisher.publish(message);
      logger.info("Published message ID: " + future.get());

    } finally {
      if (publisher != null) {
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }

  public static void deleteSchema(String projectId, String schemaId) throws IOException {
    SchemaName schemaName = SchemaName.of(projectId, schemaId);
    try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create()) {
      schemaServiceClient.deleteSchema(schemaName);
    } catch (NotFoundException e) {
      logger.info(schemaName + "not found.");
    }
  }

  public static void deleteSubscription(String projectId, String subscriptionId) throws IOException {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
      try {
        subscriptionAdminClient.deleteSubscription(subscriptionName);
      } catch (NotFoundException e) {
        logger.info(e.getMessage());
      }
    }
  }
}
