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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Iterator for PubSub subscription
 */
public class PubSubRDDIterator implements Iterator<PubSubMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubRDDIterator.class);
  private static final int MAX_MESSAGES = 1000;
  private final long startTime;
  private final PubSubSubscriberConfig config;
  private final TaskContext context;
  private final long batchDuration;
  private final Credentials credentials;
  private final String subscriptionFormatted;
  private PullRequest pullRequest;
  private SubscriberStub subscriber;
  private boolean autoAcknowledge;

  private Queue<ReceivedMessage> receivedMessages;
  private List<String> ackIds;

  private long messageCount;

  public PubSubRDDIterator(PubSubSubscriberConfig config, TaskContext context, long batchDuration,
                           boolean autoAcknowledge, Credentials credentials) {
    this.config = config;
    this.context = context;
    this.batchDuration = batchDuration;
    this.credentials = credentials;
    this.startTime = System.currentTimeMillis();
    this.autoAcknowledge = autoAcknowledge;
    subscriptionFormatted = ProjectSubscriptionName.format(this.config.getProject(), this.config.getSubscription());
    receivedMessages = new ConcurrentLinkedDeque<>();
    ackIds = new ArrayList<>();
  }

  @Override
  public boolean hasNext() {
    // Complete processing of acknowledged messages
    if (!receivedMessages.isEmpty()) {
      return true;
    }

    long currentTimeMillis = System.currentTimeMillis();
    /* LOG.error("In PubSubRDDIterator hasNext() , current time in millis is {} , start + batch is {} .",
              currentTimeMillis,
              (startTime + batchDuration)); */
    if (currentTimeMillis >= (startTime + batchDuration)) {
      LOG.error("Time exceeded for batch. Total time is {} millis. Total messages acked is {} .",
                currentTimeMillis - startTime, messageCount);
      return false;
    }



    try {
      List<ReceivedMessage> messages = fetchAndAck();
      //LOG.error("Received {} messages .", messages.size());
      //If there are no messages to process, continue.
      if (messages.isEmpty()) {
        LOG.error("No more messages. Total messages acked is {} .", messageCount);
        return false;
      }
      receivedMessages.addAll(messages);
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PubSubMessage next() {
    LOG.error("In PubSubRDDIterator next() .");
    if (receivedMessages.isEmpty()) {
      throw new RuntimeException("Unexpected state. No messages available.");
    }

    ReceivedMessage currentMessage = receivedMessages.poll();
    messageCount += 1;
    return new PubSubMessage(currentMessage);
  }

  private SubscriberStub buildSubscriberClient() throws IOException {
    SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
    // TODO - retry settings ?
    return GrpcSubscriberStub.create(builder.build());
  }

  // https://cloud.google.com/pubsub/docs/samples/pubsub-subscriber-sync-pull-with-lease
  private List<ReceivedMessage> fetchAndAck() throws IOException {
    if (this.subscriber == null) {
      this.subscriber = buildSubscriberClient();
      context.addTaskCompletionListener(context1 -> {
        if (subscriber != null && !subscriber.isShutdown()) {
          subscriber.shutdown();
          try {
            subscriber.awaitTermination(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            // TODO - log
          }
        }
      });
      this.pullRequest = PullRequest.newBuilder()
        .setMaxMessages(MAX_MESSAGES)
        .setSubscription(subscriptionFormatted)
        .build();
    }

    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
    List<ReceivedMessage> receivedMessagesList = pullResponse.getReceivedMessagesList();
    if (receivedMessagesList.isEmpty()) {
      return receivedMessagesList;
    }

    List<String> ackIds =
      receivedMessagesList.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());
    ackMessages(ackIds, autoAcknowledge, subscriptionFormatted);
    return receivedMessagesList;
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
