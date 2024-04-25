/*
 * Copyright © 2023 Cask Data, Inc.
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
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Iterator for PubSub RDD.
 * Fetches a fixed amount of messages at a time and acknowledges them.
 * Finishes when the acknowledged messages are completed and the batch time limit is met.
 * Returns immediately if there are no more messages to read.
 */
public class PubSubRDDIterator implements Iterator<PubSubMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubRDDIterator.class);
  private static final int MAX_MESSAGES = 1000;
  private static final int MAX_MESSAGE_SIZE = 20 * 1024 * 1024; //20 MB. Max size for "data" field of a message is 10MB.
  private static final int RETRY_DELAY = 100;
  // Used to set the retryable code settings for pubsub clients
  // The default codes are Code.ABORTED, Code.UNAVAILABLE, Code.UNKNOWN
  // for current client version 1.108.1
  private static final Set<Code> RETRYABLE_CLIENT_CODES =
      Stream.of(Code.RESOURCE_EXHAUSTED, Code.ABORTED, Code.CANCELLED, Code.INTERNAL, Code.UNKNOWN,
              Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED).collect(Collectors.toSet());

  private final long startTime;
  private final PubSubSubscriberConfig config;
  private final TaskContext context;
  private final long batchDuration;
  private final String subscriptionFormatted;
  private final boolean autoAcknowledge;
  private final Queue<ReceivedMessage> receivedMessages;

  private PullRequest pullRequest;
  private SubscriberStub subscriber;
  private long messageCount;

  public PubSubRDDIterator(PubSubSubscriberConfig config, TaskContext context, Time batchTime, long batchDuration,
                           boolean autoAcknowledge) {
    this.config = config;
    this.context = context;
    this.batchDuration = batchDuration;
    this.startTime = batchTime.milliseconds();
    this.autoAcknowledge = autoAcknowledge;
    subscriptionFormatted = ProjectSubscriptionName.format(this.config.getProject(), this.config.getSubscription());
    receivedMessages = new ConcurrentLinkedDeque<>();
  }

  @Override
  public boolean hasNext() {
    // Complete processing of acknowledged messages before finishing the batch.
    if (!receivedMessages.isEmpty()) {
      return true;
    }

    long currentTimeMillis = System.currentTimeMillis();
    long expiryTimeMillis = startTime + batchDuration;
    if (currentTimeMillis >= expiryTimeMillis) {
      LOG.debug("Time exceeded for batch. Total time is {} millis. Total messages returned is {} .",
                currentTimeMillis - startTime, messageCount);
      return false;
    }

    try {
      List<ReceivedMessage> messages = fetchAndAck(expiryTimeMillis);
      //If there are no messages to process, continue.
      if (messages.isEmpty()) {
        LOG.debug("No more messages. Total messages returned is {} .", messageCount);
        return false;
      }
      receivedMessages.addAll(messages);
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Error reading messages from Pub/Sub. ", e);
    }
  }

  @Override
  public PubSubMessage next() {
    if (receivedMessages.isEmpty()) {
      // This should not happen, if hasNext() returns true, then a message should be available in queue.
      throw new IllegalStateException("Unexpected state. No messages available.");
    }

    ReceivedMessage currentMessage = receivedMessages.poll();
    messageCount += 1;
    return new PubSubMessage(currentMessage);
  }

  private SubscriberStub buildSubscriberClient() throws IOException {
    SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
    Credentials credentials = PubSubSubscriberUtil.createCredentials(config.getServiceAccount(),
                                                                     config.isServiceAccountFilePath());
    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
    builder.setTransportChannelProvider(
      SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
        .setMaxInboundMessageSize(MAX_MESSAGE_SIZE).build());
    builder.getSubscriptionSettings().setRetrySettings(PubSubSubscriberUtil.getRetrySettings())
        .setRetryableCodes(RETRYABLE_CLIENT_CODES);
    return GrpcSubscriberStub.create(builder.build());
  }

  private List<ReceivedMessage> fetchAndAck(long expiryTimeMillis) throws IOException {
    if (this.subscriber == null) {
      this.subscriber = buildSubscriberClient();
      context.addTaskCompletionListener(context1 -> {
        if (subscriber != null && !subscriber.isShutdown()) {
          subscriber.shutdown();
          try {
            subscriber.awaitTermination(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            LOG.warn("Exception in shutting down subscriber. ", e);
          }
        }
      });
      this.pullRequest = PullRequest.newBuilder()
        .setMaxMessages(MAX_MESSAGES)
        .setSubscription(subscriptionFormatted)
        .build();
    }

    // From  the docs - "A pull response that comes with 0 messages must not be used as an indicator that there are no
    // messages in the backlog. It's possible to get a response with 0 messages and have a subsequent
    // request that returns messages." Since 0 messages do not indicate a lack of available messages, a fixed delay
    // is included  between retries instead of exponential backoff.
    // Messages will be read till batch duration completes, to account for messages published
    // later in the batch. No documented quota on the number of synchronous subscriber requests and no issues observed
    // in testing when repeated calls are made when no data is available in the subscription.
    while (System.currentTimeMillis() < expiryTimeMillis) {
      PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
      List<ReceivedMessage> receivedMessagesList = pullResponse.getReceivedMessagesList();
      if (receivedMessagesList.isEmpty()) {
        // Delay before re-attempt
        try {
          TimeUnit.MILLISECONDS.sleep(RETRY_DELAY);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for retry. ", e);
          return Collections.EMPTY_LIST;
        }
        continue;
      }

      List<String> ackIds =
        receivedMessagesList.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());
      ackMessages(ackIds, autoAcknowledge, subscriptionFormatted);
      return receivedMessagesList;
    }
    return Collections.EMPTY_LIST;
  }

  private void ackMessages(List<String> ackIds, boolean autoAcknowledge, String subscriptionFormatted) {
    if (!autoAcknowledge) {
      return;
    }
    // Acknowledge received messages.
    AcknowledgeRequest acknowledgeRequest =
      AcknowledgeRequest.newBuilder()
        .setSubscription(subscriptionFormatted)
        .addAllAckIds(ackIds)
        .build();
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
  }
}
