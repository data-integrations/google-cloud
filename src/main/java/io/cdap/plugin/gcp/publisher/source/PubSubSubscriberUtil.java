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
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.PushConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class to create a JavaDStream of received messages.
 */
public final class PubSubSubscriberUtil {

  protected static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriberUtil.class);

  // Retryable status codes. These need to be handled in case Pub/Sub throws a StatusRuntimeException.
  private static final int RESOURCE_EXHAUSTED = StatusCode.Code.RESOURCE_EXHAUSTED.getHttpStatusCode();
  private static final int CANCELLED = StatusCode.Code.CANCELLED.getHttpStatusCode();
  private static final int INTERNAL = StatusCode.Code.INTERNAL.getHttpStatusCode();
  private static final int UNAVAILABLE = StatusCode.Code.UNAVAILABLE.getHttpStatusCode();
  private static final int DEADLINE_EXCEEDED = StatusCode.Code.DEADLINE_EXCEEDED.getHttpStatusCode();

  private static final Set<Integer> RETRYABLE_STATUS_CODES =
    Stream.of(RESOURCE_EXHAUSTED, CANCELLED, INTERNAL, UNAVAILABLE, DEADLINE_EXCEEDED).
      collect(Collectors.toSet());
  private static final int MAX_ATTEMPTS = 5;

  private PubSubSubscriberUtil() {
  }

  /**
   * Get a JavaDStream of received PubSubMessages.
   *
   * @param streamingContext the screaming context
   * @param config           The subscriver configuration
   * @return JavaDStream of all received pub/sub messages.
   * @throws Exception when the credentials could not be loaded.
   */
  public static <T> JavaDStream<T> getStream(
    StreamingContext streamingContext, PubSubSubscriberConfig config,
    SerializableFunction<PubSubMessage, T> mappingFunction) throws Exception {

    boolean autoAcknowledge = true;
    if (streamingContext.isPreviewEnabled()) {
      autoAcknowledge = false;
    }

    return getInputDStream(streamingContext, config, autoAcknowledge, mappingFunction);
  }

  /**
   * Get a merged JavaDStream containing all received messages from multiple receivers.
   *
   * @param streamingContext the streaming context
   * @param config           subscriber config
   * @param autoAcknowledge  if the messages should be acknowleged or not.
   * @return JavaDStream containing all received messages.
   */
  @SuppressWarnings("unchecked")
  protected static <T> JavaDStream<T> getInputDStream(StreamingContext streamingContext,
                                                      PubSubSubscriberConfig config,
                                                      boolean autoAcknowledge,
                                                      SerializableFunction<PubSubMessage, T> mappingFn) {
    if (streamingContext.isStateStoreEnabled()) {
      ClassTag<PubSubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class);
      PubSubDirectDStream pubSubDirectDStream = new PubSubDirectDStream(streamingContext, config,
                                                                        streamingContext.getBatchInterval(),
                                                                        autoAcknowledge,
                                                                        mappingFn);
      return new JavaDStream<>(pubSubDirectDStream, tag);
    }

    ArrayList<DStream<PubSubMessage>> receivers = new ArrayList<>(config.getNumberOfReaders());
    ClassTag<PubSubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class);

    for (int i = 1; i <= config.getNumberOfReaders(); i++) {
      ReceiverInputDStream<PubSubMessage> receiverInputDStream =
        new PubSubInputDStream(streamingContext.getSparkStreamingContext().ssc(), config, StorageLevel.MEMORY_ONLY(),
                               autoAcknowledge);
      receivers.add(receiverInputDStream);
    }

    DStream<PubSubMessage> dStream = streamingContext.getSparkStreamingContext().ssc()
      .union(JavaConverters.collectionAsScalaIterableConverter(receivers).asScala().toSeq(), tag);

    return new JavaDStream<>(dStream, tag).map(message -> mappingFn.apply(message));
  }

  /**
   * Create a new subscription (if needed) for the supplied topic.
   *
   * @param preCheck             Any checks that need to be applied before each retry.
   * @param backoffConfig        {@link BackoffConfig} for retries.
   * @param subscription         Subscription name string.
   * @param topic                Topic name string.
   * @param clientSupplier       Supplier for creating {@link SubscriptionAdminClient}
   * @param isRetryableException Predicate for checking if the exception is retryable.
   * @throws InterruptedException If the wait for retry is interrupted.
   * @throws IOException          If {@link SubscriptionAdminClient} cannot be created.
   */
  public static void createSubscription(BooleanSupplier preCheck, BackoffConfig backoffConfig, String subscription,
                                        String topic, Supplier<SubscriptionAdminClient> clientSupplier,
                                        Predicate<ApiException> isRetryableException)
    throws InterruptedException, IOException {

    int backoff = backoffConfig.getInitialBackoffMs();
    int attempts = 5;

    ApiException lastApiException = null;

    while (preCheck.getAsBoolean() && attempts-- > 0) {

      try {
        SubscriptionAdminClient subscriptionAdminClient = clientSupplier.get();
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

        //Retry if the exception is retryable.
        if (isRetryableException.test(ae)) {
          backoff = sleepAndIncreaseBackoff(preCheck, backoff, backoffConfig);
          continue;
        }
        throw ae;
      }
    }

    throw new RuntimeException(lastApiException);
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
  public static boolean isApiExceptionRetryable(ApiException ae) {
    return ae.isRetryable() || RETRYABLE_STATUS_CODES.contains(ae.getStatusCode().getCode().getHttpStatusCode());
  }

  /**
   * Return the mapping function to convert PubSubMessage to StructuredRecord.
   *
   * @param config {@link GoogleSubscriberConfig}
   * @return {@link SerializableFunction}
   */
  public static SerializableFunction<PubSubMessage, StructuredRecord>
  getMappingFunction(GoogleSubscriberConfig config) {
    return new PubSubStructuredRecordConverter(config);
  }

  public static RetrySettings getRetrySettings() {
    BackoffConfig backoffConfig = BackoffConfig.defaultInstance();
    return RetrySettings.newBuilder()
      .setInitialRetryDelay(Duration.ofMillis(backoffConfig.getInitialBackoffMs()))
      .setMaxRetryDelay(Duration.ofMillis(backoffConfig.getMaximumBackoffMs()))
      .setRetryDelayMultiplier(backoffConfig.getBackoffFactor()).setMaxAttempts(MAX_ATTEMPTS).build();
  }

  private static SubscriptionAdminClient buildSubscriptionAdminClient(Credentials credentials) throws IOException {
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
    return SubscriptionAdminClient.create(builder.build());
  }

  private static int sleepAndIncreaseBackoff(BooleanSupplier preCheck, int backoff,
                                             BackoffConfig backoffConfig) throws InterruptedException {
    if (preCheck.getAsBoolean()) {
      LOG.trace("Backoff - Sleeping for {} ms.", backoff);
      Thread.sleep(backoff);
    }

    return calculateUpdatedBackoff(backoff, backoffConfig);
  }

  private static int calculateUpdatedBackoff(int backoff, BackoffConfig backoffConfig) {
    return Math.min((int) (backoff * backoffConfig.getBackoffFactor()), backoffConfig.getMaximumBackoffMs());
  }
}
