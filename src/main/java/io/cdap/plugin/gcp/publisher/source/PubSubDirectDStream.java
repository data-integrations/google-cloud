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
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.CreateSnapshotRequest;
import com.google.pubsub.v1.ProjectSnapshotName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.TopicName;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * PubSubDirectDStream implementation.
 * A snapshot is taken and saved before each batch and deleted on successful completion.
 * On retrying a batch, snapshot is restored.
 *
 * @param <T> Type of object returned by RDD
 */
public class PubSubDirectDStream<T> extends InputDStream<T> implements StreamingEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubDirectDStream.class);
  private static final String CDAP_PIPELINE = "cdap_pipeline";

  private final Credentials credentials;
  private final PubSubSubscriberConfig config;
  private final long readDuration;
  private final io.cdap.cdap.etl.api.streaming.StreamingContext context;
  private final boolean autoAcknowledge;
  private final SerializableFunction<PubSubMessage, T> mappingFn;
  private final StreamingContext streamingContext;
  private final String pipeline;

  private SubscriptionAdminClient subscriptionAdminClient;
  private ProjectSnapshotName currentSnapshotName;
  private boolean takeSnapshot;

  public PubSubDirectDStream(io.cdap.cdap.etl.api.streaming.StreamingContext context, PubSubSubscriberConfig config,
                             long readDuration, boolean autoAcknowledge,
                             SerializableFunction<PubSubMessage, T> mappingFn) {
    super(context.getSparkStreamingContext().ssc(), scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    this.streamingContext = context.getSparkStreamingContext().ssc();
    this.config = config;
    this.readDuration = readDuration;
    this.context = context;
    this.autoAcknowledge = autoAcknowledge;
    this.mappingFn = mappingFn;
    this.pipeline = context.getPipelineName();
    this.credentials = createCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
  }

  @Override
  public Option<RDD<T>> compute(Time validTime) {
    LOG.debug("Computing RDD for time {}.", validTime);
    PubSubRDD pubSubRDD = new PubSubRDD(streamingContext.sparkContext(), validTime, readDuration, config,
                                        autoAcknowledge, credentials);
    RDD<T> mapped = pubSubRDD.map(mappingFn, scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    return Option.apply(mapped);
  }

  @Override
  public void start() {
    if (config.getTopic() != null) {
      try {
        createSubscriptionIfNotPresent();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Subscription creation failed.", e);
      }
    }

    try {
      subscriptionAdminClient = buildSubscriptionAdminClient(credentials);
    } catch (IOException e) {
      throw new RuntimeException("SubscriptionAdminClient creation failed.", e);
    }

    // If state with snapshot is present, seek to it.
    this.currentSnapshotName = fetchSnapShot(config.getSubscription(), context);
    if (this.currentSnapshotName != null) {
      seekSnapshot(currentSnapshotName, ProjectSubscriptionName.of(config.getProject(), config.getSubscription()));
      // Can use the same snapshot till end of the batch.
      takeSnapshot = false;
    } else {
      takeSnapshot = true;
    }
  }

  @Override
  public void stop() {
    if (subscriptionAdminClient == null) {
      return;
    }

    subscriptionAdminClient.close();
  }

  @Override
  public void onBatchStarted(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.debug("Starting a batch.");
    if (!takeSnapshot) {
      return;
    }

    String generatedSnapshotName = generateName(config.getSubscription());
    ProjectSnapshotName projectSnapshotName = ProjectSnapshotName.of(config.getProject(), generatedSnapshotName);
    ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(config.getProject(),
                                                                                 config.getSubscription());
    // Create in Pub/Sub
    Snapshot snapshot = createSnapshot(projectSnapshotName, projectSubscriptionName);
    // Save snapshot as state
    try {
      saveSnapshotAsState(snapshot, config.getSubscription(), context);
    } catch (IOException e) {
      // Delete the snapshot from Pub/Sub to avoid abandoned snapshots.
      // Snapshots have a max expiry of 7 days after which it gets auto deleted.
      deleteSnapshot(projectSnapshotName);

      // Retries are already part of the state saving library, so just throw here.
      throw new RuntimeException("Error while saving state.", e);
    }
    this.currentSnapshotName = projectSnapshotName;
  }

  @Override
  public void onBatchRetry(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.debug("Batch is about to be retried. Seeking to snapshot {} for the current batch.", currentSnapshotName);
    seekSnapshot(currentSnapshotName, ProjectSubscriptionName.of(config.getProject(), config.getSubscription()));
  }

  @Override
  public void onBatchCompleted(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.debug("Batch completed called.");
    try {
      streamingContext.deleteState(config.getSubscription());
    } catch (IOException e) {
      throw new RuntimeException("Deleting state failed. ", e);
    }
    // delete the snapshot from Pub/Sub .
    deleteSnapshot(currentSnapshotName);

    // For next batch
    takeSnapshot = true;
    LOG.debug("Batch completed successfully. Deleted snapshot {} for the current batch.", currentSnapshotName);
  }

  private String generateName(String subscription) {
    // Rules for name
    // Not begin with the string goog
    // Start with a letter
    // Contain between 3 and 255 characters
    // Contain only the following characters:
    // Letters [A-Za-z], numbers [0-9], dashes -, underscores _, periods ., tildes ~, plus signs +, and percent signs %

    // Starting with subscription name should take care of the beginning reqs.
    String uuidString = UUID.randomUUID().toString();
    int maxLenForSubscriptionPrefix = 255 - uuidString.length() - 1;
    String subscriptionPrefix = subscription.length() > maxLenForSubscriptionPrefix ?
      subscription.substring(0, maxLenForSubscriptionPrefix) : subscription;
    return String.format("%s-%s", subscriptionPrefix, uuidString);
  }

  private void createSubscriptionIfNotPresent() throws IOException, InterruptedException {
    PubSubSubscriberUtil.createSubscription(() -> true, BackoffConfig.defaultInstance(),
                                            ProjectSubscriptionName.format(config.getProject(),
                                                                           config.getSubscription()),
                                            TopicName.format(config.getProject(), config.getTopic()),
                                            () -> subscriptionAdminClient,
                                            PubSubSubscriberUtil::isApiExceptionRetryable);
  }

  private void seekSnapshot(ProjectSnapshotName projectSnapshotName,
                            ProjectSubscriptionName projectSubscriptionName) {
    try {
      subscriptionAdminClient.seek(SeekRequest.newBuilder().setSnapshot(projectSnapshotName.toString())
                                     .setSubscription(projectSubscriptionName.toString())
                                     .build());
    } catch (NotFoundException e) {
      throw new RuntimeException(String.format(
        "Saved snapshot %s not found. Please clear the application state to proceed. " +
          "REST api for state deletion is namespaces/{namespace-id}/apps/{app-id}/state .",
        projectSnapshotName.toString()), e);
    }
  }

  @Nullable
  private ProjectSnapshotName fetchSnapShot(String subscriptionId,
                                            io.cdap.cdap.etl.api.streaming.StreamingContext context) {
    Optional<byte[]> state = null;
    try {
      state = context.getState(subscriptionId);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error fetching saved state for subscription %s.", subscriptionId), e);
    }

    if (!state.isPresent()) {
      LOG.debug("No saved state for {}.", subscriptionId);
      return null;
    }

    try {
      Snapshot snapshot = Snapshot.parseFrom(state.get());
      LOG.debug("Found existing snapshot {} .", snapshot.getName());
      return ProjectSnapshotName.parse(snapshot.getName());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Error parsing saved state for subscription %s.", subscriptionId), e);
    }
  }

  private Snapshot createSnapshot(ProjectSnapshotName projectSnapshotName,
                                  ProjectSubscriptionName projectSubscriptionName) {
    // Creation takes around 3.5 s .
    LOG.debug("Creating snapshot {} for subscription {} in Pub/Sub .", projectSnapshotName.toString(),
              projectSubscriptionName.toString());
    CreateSnapshotRequest request = CreateSnapshotRequest.newBuilder()
      .setName(projectSnapshotName.toString())
      .setSubscription(projectSubscriptionName.toString())
      .putAllLabels(Collections.singletonMap(CDAP_PIPELINE, pipeline))
      .build();
    return subscriptionAdminClient.createSnapshot(request);
  }

  private void deleteSnapshot(ProjectSnapshotName projectSnapshotName) {
    // Deletion takes around 2.5 s .
    // TODO - Consider making this asynchronous
    subscriptionAdminClient.deleteSnapshot(projectSnapshotName);
  }

  private void saveSnapshotAsState(Snapshot snapshot, String subscription,
                                   io.cdap.cdap.etl.api.streaming.StreamingContext context) throws IOException {
    LOG.debug("Saving snapshot {} in state .", snapshot.getName());
    context.saveState(subscription, snapshot.toByteArray());
  }

  private SubscriptionAdminClient buildSubscriptionAdminClient(Credentials credentials) throws IOException {
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();

    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }

    RetrySettings retrySettings = PubSubSubscriberUtil.getRetrySettings();
    builder.seekSettings().setRetrySettings(retrySettings);
    builder.createSnapshotSettings().setRetrySettings(retrySettings);
    builder.deleteSnapshotSettings().setRetrySettings(retrySettings);

    return SubscriptionAdminClient.create(builder.build());
  }

  private Credentials createCredentials(String serviceAccount, boolean serviceAccountFilePath) {
    try {
      return serviceAccount == null ? null : GCPUtils.loadServiceAccountCredentials(serviceAccount,
                                                                                    serviceAccountFilePath);
    } catch (IOException e) {
      throw new RuntimeException("error creating credentials from service account.", e);
    }
  }
}
