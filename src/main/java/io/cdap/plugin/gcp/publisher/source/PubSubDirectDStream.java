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
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * PubSubDirectDStream implementation.
 *
 * @param <T> Type of object returned by RDD
 */
public class PubSubDirectDStream<T> extends InputDStream<T> implements StreamingEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubDirectDStream.class);

  private static final int MAX_ATTEMPTS = 3;
  private SubscriptionAdminClient subscriptionAdminClient;

  private final Credentials credentials;
  private PubSubSubscriberConfig config;
  private long readDuration;
  private io.cdap.cdap.etl.api.streaming.StreamingContext context;
  private final boolean autoAcknowledge;
  private final transient SerializableFunction<PubSubMessage, T> mappingFn;
  private StreamingContext streamingContext;

  private String currentSnapshotName;

  private boolean takeSnapshot;
  private String existingSnapshot;

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
    this.credentials = createCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
  }

  @Override
  public Option<RDD<T>> compute(Time validTime) {
    LOG.info("In PubSubDirectDStream compute");
    PubSubRDD pubSubRDD = new PubSubRDD(streamingContext.sparkContext(), readDuration, config, autoAcknowledge,
                                        credentials);
    final SerializableFunction<PubSubMessage, T> localMappingFn = mappingFn;
    SerializableFunction1<PubSubMessage, T> pubSubMessageTFunction1 = v1 -> localMappingFn.apply(v1);
    RDD<T> mapped = pubSubRDD.map(pubSubMessageTFunction1,
                                  scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    return Option.apply(mapped);
  }

  @Override
  public void start() {
    if (config.getTopic() != null) {
      createSubscription();
    }

    try {
      subscriptionAdminClient = buildSubscriptionAdminClient(credentials);
    } catch (IOException e) {
      throw new RuntimeException("SubscriptionAdminClient creation failed.", e);
    }

    // If state with snapshot is present, seek to it.
    this.existingSnapshot = fetchSnapShot(config.getSubscription(), context);
    if (this.existingSnapshot != null) {
      seekSnapshot(config.getProject(), config.getSubscription(), existingSnapshot);
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
    LOG.info("In PubSubDirectDStream onBatchStarted");
    this.currentSnapshotName = setSnapshotForBatch(config.getSubscription(), config.getProject());
  }

  @Override
  public void onBatchRetry(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.info("Batch is about to be retried. Seek to snapshot {} for the current batch.", currentSnapshotName);
    seekSnapshot(config.getProject(), config.getSubscription(), currentSnapshotName);
  }

  @Override
  public void onBatchCompleted(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.info("In PubSubDirectDStream onBatchCompleted");
    try {
      streamingContext.deleteSate(config.getSubscription());
    } catch (IOException e) {
      throw new RuntimeException("Deleting state failed. ", e);
    }
    // delete the snapshot from Pub/Sub
    deleteSnapshot(currentSnapshotName);

    // For next batch
    takeSnapshot = true;
    LOG.info("Batch completed successfully. Deleted snapshot {} for the current batch.", currentSnapshotName);
  }

  private void createSubscription() {
    try {
      PubSubSubscriberUtil.createSubscription(() -> true, BackoffConfig.defaultInstance(),
                                              ProjectSubscriptionName.format(config.getProject(),
                                                                             config.getSubscription()),
                                              TopicName.format(config.getProject(), config.getTopic()),
                                              () -> subscriptionAdminClient,
                                              PubSubSubscriberUtil::isApiExceptionRetryable);
    } catch (Exception e) {
      throw new RuntimeException("Subscription creation failed.", e);
    }
  }

  private SubscriptionAdminClient buildSubscriptionAdminClient() throws IOException {
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
    return SubscriptionAdminClient.create(builder.build());

  }

  private String setSnapshotForBatch(String subscription, String project) {
    LOG.info("In PubSubDirectDStream setSnapshotForBatch.");
    if (takeSnapshot) {
      return createAndSave(project, subscription, subscription + "-snapshot-" + UUID.randomUUID(), context);
    }
    return existingSnapshot;
  }

  private String seekSnapshot(String projectId, String subscriptionId, String snapshotName) {
    try {
      ProjectSnapshotName projectSnapshotName = ProjectSnapshotName.parse(snapshotName);
      subscriptionAdminClient.seek(
        SeekRequest.newBuilder().setSnapshot(projectSnapshotName.toString())
          .setSubscription(ProjectSubscriptionName.of(projectId, subscriptionId).toString())
          .build());
      return snapshotName;
    } catch (NotFoundException e) {
      //Saved snapshot is not found. Create one and save it.
      throw new RuntimeException(String.format(
        "Saved snapshot %s not found. Please clear the application state to proceed. " +
          "REST api for state deletion is namespaces/{namespace-id}/apps/{app-id}/state", snapshotName), e);
    }
  }

  @Nullable
  private String fetchSnapShot(String subscriptionId, io.cdap.cdap.etl.api.streaming.StreamingContext context) {
    Optional<byte[]> state = null;
    try {
      state = context.getState(subscriptionId);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error fetching saved state for subscription %s.", subscriptionId), e);
    }

    if (!state.isPresent()) {
      LOG.info("No saved state for {}.", subscriptionId);
      return null;
    }

    try {
      Snapshot snapshot = Snapshot.parseFrom(state.get());
      LOG.info("Found existing snapshot {} .", snapshot.getName());
      return snapshot.getName();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Error parsing saved state for subscription %s.", subscriptionId), e);
    }
  }

  private String createAndSave(String projectId, String subscriptionId, String snapshotName,
                               io.cdap.cdap.etl.api.streaming.StreamingContext context) {
    LOG.info("In PubSubDirectDStream createAndSave.");
    Snapshot snapshot = createSnapshot(projectId, subscriptionId, snapshotName);
    saveSnapshot(snapshot, subscriptionId, context);
    return snapshot.getName();
  }

  private Snapshot createSnapshot(String projectId, String subscriptionId,
                                  String snapshotName) {
    CreateSnapshotRequest request =
      CreateSnapshotRequest.newBuilder()
        .setName(ProjectSnapshotName.of(projectId, snapshotName).toString())
        .setSubscription(
          ProjectSubscriptionName.of(projectId, subscriptionId).toString())
        .putAllLabels(new HashMap<>())
        .build();
    return subscriptionAdminClient.createSnapshot(request);
  }

  private void deleteSnapshot(String snapshotName) {
    ProjectSnapshotName projectSnapshotName = ProjectSnapshotName.parse(snapshotName);
    subscriptionAdminClient.deleteSnapshot(projectSnapshotName);
  }

  private void saveSnapshot(Snapshot snapshot, String subscription,
                            io.cdap.cdap.etl.api.streaming.StreamingContext context) {
    try {
      LOG.info("Saving snapshot {} .", snapshot.getName());
      context.saveState(subscription, snapshot.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SubscriptionAdminClient buildSubscriptionAdminClient(Credentials credentials) throws IOException {
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();

    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }

    builder.seekSettings().setRetrySettings(getRetrySettings());
    builder.createSnapshotSettings().setRetrySettings(getRetrySettings());
    builder.deleteSnapshotSettings().setRetrySettings(getRetrySettings());

    return SubscriptionAdminClient.create(builder.build());
  }

  // TODO - Improve retry settings
  private RetrySettings getRetrySettings() {
    return RetrySettings.newBuilder()
      .setMaxAttempts(MAX_ATTEMPTS)
      .build();
  }

  private Credentials createCredentials(String serviceAccount, boolean serviceAccountFilePath) {
    try {
      return serviceAccount == null ?
        null : GCPUtils.loadServiceAccountCredentials(serviceAccount,
                                                      serviceAccountFilePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
