/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.TopicName;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Spark Receiver for Pub/Sub Messages.
 * <p>
 * If backpressure is enabled, the message ingestion rate for this receiver will be managed by Spark.
 */
public class PubSubReceiver extends Receiver<PubSubMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubReceiver.class);
  private static final String CREATE_SUBSCRIPTION_ERROR_MSG =
    "Failed to create subscription '%s'.";
  private static final String CREATE_SUBSCRIPTION_ADMIN_CLIENT_ERROR_MSG =
    "Failed to create subscription client to manage subscription '%s'.";
  private static final String CREATE_SUBSCRIPTION_RETRY_ERROR_MSG =
    "Failed to create subscription '%s' after 5 attempts";
  private static final String MISSING_TOPIC_ERROR_MSG =
    "Failed to create subscription. Topic '%s' was not found in project '%s'.";
  private static final String SUBSCRIBER_ERROR_MSG =
    "Failed to create subscriber using subscription '%s' for project '%s'.";
  private static final String FETCH_ERROR_MSG =
    "Failed to fetch new messages using subscription '%s' for project '%s'.";
  private static final String INTERRUPTED_EXCEPTION_MSG =
    "Interrupted Exception when sleeping during backoff.";

  // Retryable status codes. These need to be handled in case Pub/Sub throws a StatusRuntimeException.
  private static final int RESOURCE_EXHAUSTED = StatusCode.Code.RESOURCE_EXHAUSTED.getHttpStatusCode();
  private static final int CANCELLED = StatusCode.Code.CANCELLED.getHttpStatusCode();
  private static final int INTERNAL = StatusCode.Code.INTERNAL.getHttpStatusCode();
  private static final int UNAVAILABLE = StatusCode.Code.UNAVAILABLE.getHttpStatusCode();
  private static final int DEADLINE_EXCEEDED = StatusCode.Code.DEADLINE_EXCEEDED.getHttpStatusCode();
  private static final Set<Integer> RETRYABLE_STATUS_CODES = Collections.unmodifiableSet(new HashSet<Integer>() {{
    add(RESOURCE_EXHAUSTED);
    add(CANCELLED);
    add(INTERNAL);
    add(UNAVAILABLE);
    add(DEADLINE_EXCEEDED);
  }});

  private final PubSubSubscriberConfig config;
  private final boolean autoAcknowledge;
  private final BackoffConfig backoffConfig;
  private int previousFetchRate = -1;

  //Transient properties used by the receiver in the worker node.
  private transient String project;
  private transient String topic;
  private transient String subscription;
  private transient Credentials credentials;
  private transient ScheduledThreadPoolExecutor executor;
  private transient SubscriberStub subscriber;
  private transient AtomicInteger bucket;

  public PubSubReceiver(PubSubSubscriberConfig config, boolean autoAcknowledge, StorageLevel storageLevel) {
    this(config, autoAcknowledge, storageLevel, BackoffConfig.defaultInstance());
  }

  public PubSubReceiver(PubSubSubscriberConfig config, boolean autoAcknowledge, StorageLevel storageLevel,
                        BackoffConfig backoffConfig) {
    super(storageLevel);

    this.config = config;
    this.autoAcknowledge = autoAcknowledge;
    this.backoffConfig = backoffConfig;
  }

  @VisibleForTesting
  public PubSubReceiver(String project, String topic, String subscription, Credentials credentials,
                        boolean autoAcknowledge, StorageLevel storageLevel, BackoffConfig backoffConfig,
                        ScheduledThreadPoolExecutor executor, SubscriberStub subscriber, AtomicInteger bucket) {
    super(storageLevel);
    this.backoffConfig = backoffConfig;
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.autoAcknowledge = autoAcknowledge;
    this.credentials = credentials;
    this.executor = executor;
    this.subscriber = subscriber;
    this.bucket = bucket;
    this.config = null;
  }

  @Override
  public void onStart() {
    //Configure Executor Service
    this.executor = new ScheduledThreadPoolExecutor(3, new LoggingRejectedExecutionHandler());
    this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    this.executor.setRemoveOnCancelPolicy(true);

    //Create counter used to restrict the number of messages we fetch every second.
    this.bucket = new AtomicInteger();

    //Configure properties
    this.project = config.getProject();
    this.subscription = ProjectSubscriptionName.format(config.getProject(), config.getSubscription());

    // Try to initialize credentials.
    this.credentials = createCredentials();

    //Create subscription if the topic is specified.
    if (config.getTopic() != null) {
      this.topic = TopicName.format(config.getProject(), config.getTopic());
      createSubscription();
    }

    // Try to create the subscriber client.
    this.subscriber = createSubscriberClient();

    //Schedule tasks to set the message rate and start the receiver worker.
    scheduleTasks();

    LOG.info("Receiver started execution");
  }

  @Override
  public void onStop() {
    //Shutdown thread pool executor
    if(Snapshot) {
      SeekRequest request = SeekRequest.newBuilder().setSnapshot("prev-snapshot").build();
      Pubsub.Projects.Subscriptions(subscription, request);
    }
    Snapshot snapshot = Snapshot.newBuilder().setName("prev-snapshot")
                          .setTopic(topic).build();
    if (executor != null && !executor.isShutdown()) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for executor to shutdown.");
      }
    }

    //Clean up subscriber stub used by the Google Cloud client.
    if (subscriber != null && !subscriber.isShutdown()) {
      subscriber.shutdown();
      try {
        subscriber.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for subscriber to shutdown.");
      }
    }

    LOG.info("Receiver completed execution");
  }

  /**
   * Build credentials for our Pub/Sub client.
   *
   * @return the instance of GCP Credentials from configuration, or null if the credentials are not specified.
   */
  @Nullable
  protected Credentials createCredentials() {
    if (isStopped()) {
      return null;
    }

    try {
      return config.getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(),
                                                      config.isServiceAccountFilePath());
    } catch (IOException e) {
      stop("Unable to get credentials for receiver.", e);
    }

    return null;
  }

  /**
   * Create a new subscription (if needed) for the supplied topic.
   *
   * If the subscription cannot be created, the receiver is stopped.
   */
  protected void createSubscription() {
    if (isStopped()) {
      return;
    }

    int backoff = backoffConfig.getInitialBackoffMs();
    int attempts = 5;

    ApiException lastApiException = null;

    while (!isStopped() && attempts-- > 0) {

      try (SubscriptionAdminClient subscriptionAdminClient = buildSubscriptionAdminClient()) {

        int ackDeadline = 60; // 60 seconds before resending the message.
        subscriptionAdminClient.createSubscription(
          subscription, topic, PushConfig.getDefaultInstance(), ackDeadline);
        return;

      } catch (ApiException ae) {

        lastApiException = ae;

        //If the subscription already exists, ignore the error.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.ALREADY_EXISTS)) {
          return;
        }

        //This error is thrown is the Topic does not exist.
        // Call the stop method so the pipeline fails.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
          String message = String.format(MISSING_TOPIC_ERROR_MSG, topic, project);
          stop(message, ae);
          return;
        }

        //Retry if the exception is retryable.
        if (isApiExceptionRetryable(ae)) {
          backoff = sleepAndIncreaseBackoff(backoff);
          continue;
        }

        //Report that we were not able to create the subscription and stop the receiver.
        stop(String.format(CREATE_SUBSCRIPTION_ERROR_MSG, subscription), ae);
        return;
      } catch (IOException ioe) {
        //Report that we were not able to create the subscription admin client and stop the receiver.
        stop(String.format(CREATE_SUBSCRIPTION_ADMIN_CLIENT_ERROR_MSG, subscription), ioe);
        return;
      }
    }

    //If we were not able to create the subscription after 5 attempts, stop the pipeline and report the error.
    stop(String.format(CREATE_SUBSCRIPTION_RETRY_ERROR_MSG, subscription), lastApiException);
  }

  /**
   * Create a new Subscriber Client.
   *
   * This method stops the receiver is an exception is thrown when creating the subscriber client.
   */
  @Nullable
  public SubscriberStub createSubscriberClient() {
    if (isStopped()) {
      return null;
    }

    try {
      return buildSubscriberClient();
    } catch (IOException ioe) {
      //This exception is thrown when the subscriber could not be created.
      //Report the exception and stop the receiver.
      String message =
        String.format(SUBSCRIBER_ERROR_MSG, subscription, project);
      stop(message, ioe);
    }

    return null;
  }

  /**
   * Schedule tasks
   */
  public void scheduleTasks() {
    if (!this.isStopped()) {
      executor.scheduleAtFixedRate(this::updateMessageRateAndFillBucket, 0, 1, TimeUnit.SECONDS);
      executor.scheduleWithFixedDelay(this::receiveMessages, 100, 100, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Fetch new messages for our subscription.
   * Implements exponential backoff strategy when a retryable exception is received.
   * This method stops the receiver if a non retryable ApiException is thrown by the Google Cloud subscriber client.
   */
  protected void receiveMessages() {
    int backoff = backoffConfig.getInitialBackoffMs();

    //Try with backoff until stopped or the task succeeds.
    while (!isStopped()) {
      try {
        fetchAndAck();
        return;
      } catch (ApiException ae) {
        // A retryable exception means the request can be safely retried at a later time.
        // In this case, the exponential backoff logic starts increasing the delay for requests).

        // A non-retryable exception means that we will not be able to pull any more messages from this topic.
        // This can be related to credentials, the subscription getting deleted, or an error from the Pub/Sub service.
        // In this case, the receiver gets restarted, credentials re-initialized, subscription recreated if needed,
        // and then the receiver starts fetching messages once again.
        if (isApiExceptionRetryable(ae)) {
          backoff = sleepAndIncreaseBackoff(backoff);
        } else {
          // Restart the receiver if the exception is not retryable.
          String message =
            String.format(FETCH_ERROR_MSG, subscription, project);
          restart(message, ae);
          break;
        }
      }
    }
  }

  /**
   * Fetch new messages, store in Spark's memory, and ack messages.
   * Based on SubscribeSyncExample.java in Google's PubSub examples.
   */
  protected void fetchAndAck() {
    //Get the maximun number of messages to get. If this number is less or equal than 0, do not fetch.
    int maxMessages = bucket.get();
    if (maxMessages <= 0) {
      return;
    }

    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(maxMessages)
        .setSubscription(subscription)
        .build();
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

    List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

    //If there are no messages to process, continue.
    if (receivedMessages.isEmpty()) {
      return;
    }

    //Decrement number of available messages in bucket.
    bucket.updateAndGet(x -> x - receivedMessages.size());

    //Exit if the receiver is stopped before storing and acknowledging.
    if (isStopped()) {
      LOG.trace("Receiver stopped before store and ack.");
      return;
    }

    List<PubSubMessage> messages = receivedMessages.stream().map(PubSubMessage::new).collect(Collectors.toList());

    store(messages.iterator());

    if (autoAcknowledge) {
      List<String> ackIds =
        messages.stream().map(PubSubMessage::getAckId).collect(Collectors.toList());

      // Acknowledge received messages.
      AcknowledgeRequest acknowledgeRequest =
        AcknowledgeRequest.newBuilder()
          .setSubscription(subscription)
          .addAllAckIds(ackIds)
          .build();
      subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }
  }

  /**
   * Get Subscriber admin settings instance.
   *
   * @return the Subscriber Admin Stub settings needed to create to a Pub/Sub subscription.
   * @throws IOException if the Subscription Admin Settings could not be created.
   */
  protected SubscriptionAdminSettings buildSubscriptionAdminSettings() throws IOException {
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();

    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }

    return builder.build();
  }

  /**
   * Get Subscriber settings instance.
   *
   * @return the Subscriber Stub settings needed to subscribe to a Pub/Sub topic.
   * @throws IOException if the Subscriber Settings could not be created.
   */
  protected SubscriberStubSettings buildSubscriberSettings() throws IOException {
    SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();

    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }

    return builder.build();
  }

  /**
   * Get Subscription Admin Client instance
   *
   * @return Subscription Admin Client instance.
   * @throws IOException if the subscription admin client could not be created.
   */
  protected SubscriptionAdminClient buildSubscriptionAdminClient() throws IOException {
    return SubscriptionAdminClient.create(buildSubscriptionAdminSettings());
  }

  /**
   * Get Subscriber Stub instance
   *
   * @return a new Subscriber Stub Instance
   * @throws IOException if the subscriber client could not be created.
   */
  protected SubscriberStub buildSubscriberClient() throws IOException {
    return GrpcSubscriberStub.create(buildSubscriberSettings());
  }

  /**
   * Sleep for a given number of milliseconds, calculate new backoff time and return.
   *
   * This method stops the receiver is an InterruptedException is thrown.
   *
   * @param backoff the time in milliseconds to delay execution.
   * @return the new backoff delay in milliseconds
   */
  protected int sleepAndIncreaseBackoff(int backoff) {
    try {
      if (!isStopped()) {
        LOG.trace("Backoff - Sleeping for {} ms.", backoff);
        Thread.sleep(backoff);
      }
    } catch (InterruptedException e) {
      stop(INTERRUPTED_EXCEPTION_MSG, e);
    }

    return calculateUpdatedBackoff(backoff);
  }

  /**
   * Calculate the updated backoff period baded on the Backoff configuration parameters.
   *
   * @param backoff the previous backoff period
   * @return the updated backoff period for the next cycle.
   */
  protected int calculateUpdatedBackoff(int backoff) {
    return Math.min((int) (backoff * backoffConfig.getBackoffFactor()), backoffConfig.getMaximumBackoffMs());
  }

  /**
   * Get the rate at which this receiver should pull messages and set this rate in the bucket we use for rate control.
   * The default rate is Integer.MAX_VALUE if the receiver has not been able to calculate a rate.
   */
  protected void updateMessageRateAndFillBucket() {
    int messageRate = (int) Math.min(supervisor().getCurrentRateLimit(), Integer.MAX_VALUE);

    if (messageRate != previousFetchRate) {
      previousFetchRate = messageRate;
      LOG.trace("Receiver fetch rate is set to: {}", messageRate);
    }

    bucket.set(messageRate);
  }

  /**
   * Method to determine if an API Exception is retryable. This uses the built-in method in API Exception as well
   * as checking the status code.
   * <p>
   * In testing, we noticed the client was wrapping some network exceptions declaring those as not retryable,
   * even when the Pub/Sub documentation states that the request may be retryable.
   *
   * @param ae the API Exception
   * @return boolean stating whether we should retry this request.
   */
  protected boolean isApiExceptionRetryable(ApiException ae) {
    return ae.isRetryable() || RETRYABLE_STATUS_CODES.contains(ae.getStatusCode().getCode().getHttpStatusCode());
  }

  /**
   * Class used to configure exponential backoff for Pub/Sub API requests.
   */
  public static class BackoffConfig implements Serializable {
    final int initialBackoffMs;
    final int maximumBackoffMs;
    final double backoffFactor;

    static final BackoffConfig defaultInstance() {
      return new BackoffConfig(100, 10000, 2.0);
    }

    public BackoffConfig(int initialBackoffMs, int maximumBackoffMs, double backoffFactor) {
      this.initialBackoffMs = initialBackoffMs;
      this.maximumBackoffMs = maximumBackoffMs;
      this.backoffFactor = backoffFactor;
    }

    public int getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public int getMaximumBackoffMs() {
      return maximumBackoffMs;
    }

    public double getBackoffFactor() {
      return backoffFactor;
    }
  }

  /**
   * Builder class for BackoffConfig
   */
  public static class BackoffConfigBuilder implements Serializable {
    public int initialBackoffMs = 100;
    public int maximumBackoffMs = 10000;
    public double backoffFactor = 2.0;

    protected BackoffConfigBuilder() {
    }

    public static BackoffConfigBuilder getInstance() {
      return new BackoffConfigBuilder();
    }

    public BackoffConfig build() {
      if (initialBackoffMs > maximumBackoffMs) {
        throw new IllegalArgumentException("Maximum backoff cannot be smaller than Initial backoff");
      }

      return new BackoffConfig(initialBackoffMs, maximumBackoffMs, backoffFactor);
    }

    public int getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public BackoffConfigBuilder setInitialBackoffMs(int initialBackoffMs) {
      this.initialBackoffMs = initialBackoffMs;
      return this;
    }

    public int getMaximumBackoffMs() {
      return maximumBackoffMs;
    }

    public BackoffConfigBuilder setMaximumBackoffMs(int maximumBackoffMs) {
      this.maximumBackoffMs = maximumBackoffMs;
      return this;
    }

    public double getBackoffFactor() {
      return backoffFactor;
    }

    public BackoffConfigBuilder setBackoffFactor(int backoffFactor) {
      this.backoffFactor = backoffFactor;
      return this;
    }
  }

  /**
   * Rejected execution handler which logs a message when a task is rejected.
   */
  protected static class LoggingRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      LOG.error("Thread Pool rejected execution of a task.");
    }
  }
}
