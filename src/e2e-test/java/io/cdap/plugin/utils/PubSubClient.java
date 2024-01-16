/*
 * Copyright © 2024 Cask Data, Inc.
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
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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
   * Create the subscription.
   */
  public static void createSubscription(String subscriptionId, String topicId) throws IOException {
    ProjectSubscriptionName subscriptionName = null;
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder().build())) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      subscriptionName = ProjectSubscriptionName.of
        (PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), subscriptionId);
      subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 60);
      logger.info("Subscription created: " + subscriptionName.toString());
    } catch (StatusRuntimeException e) {
      if ("ALREADY_EXISTS".equals(e.getStatus().getCode().name())) {
        logger.info("Subscription already exists: {}", subscriptionName.toString());
      } else {
        logger.info("Error creating subscription", e);
      }
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


  public static void publishMessagesWithPubSub(String projectId, String topicId, List<String> dataMessages)
    throws IOException, InterruptedException {
    TopicName topicName = TopicName.of(projectId, topicId);
    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topicName).build();
      for (final String message : dataMessages) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> future = publisher.publish(pubsubMessage);

        /**
         * Adding an asynchronous callback to handle success / failure.
         */
        ApiFutures.addCallback(future,
      new ApiFutureCallback<String>() {
          @Override
    public void onFailure(Throwable throwable) {
      if (throwable instanceof ApiException) {
      ApiException apiException = ((ApiException) throwable);
      /**
      * details on the API exception.
      */
      logger.info(String.valueOf(apiException.getStatusCode().getCode()));
      logger.info(String.valueOf(apiException.isRetryable()));
      }
      logger.info("Error publishing message : " + message);
      }
      @Override
    public void onSuccess(String messageId) {
      /**
      * Once published, returns server-assigned message ids (unique within the topic).
      */
      logger.info("Published message ID: " + messageId);
      }
      },
      MoreExecutors.directExecutor());
      }
    } finally {
      if (publisher != null) {
        /**
         * When finished with the publisher, shutdown to free up resources.
         */
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }

  public static void subscribeAsyncExample(String projectId, String subscriptionId) {
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
    /**
     * Instantiate an asynchronous message receiver.
     */
    MessageReceiver receiver =
      (PubsubMessage message, AckReplyConsumer consumer) -> {
        /**
         *  Handle incoming message, then ack the received message.
         */
        logger.info("Id: " + message.getMessageId());
        logger.info("Data: " + message.getData().toStringUtf8());
        consumer.ack();
      };

    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
      /**
       * Start the subscriber.
       */
      subscriber.startAsync().awaitRunning();
      logger.info("Listening for messages on %s:\n", subscriptionName);
      /**
       * Allow the subscriber to run for 30s unless an unrecoverable error occurs.
       */
      subscriber.awaitTerminated(200, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      logger.error("Timeout exception: {e}");
      /**
       * Shut down the subscriber after 30s. Stop receiving messages.
       */
      subscriber.stopAsync();
    }
  }
}
