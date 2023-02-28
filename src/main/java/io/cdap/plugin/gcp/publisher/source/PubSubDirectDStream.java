package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.CreateSnapshotRequest;
import com.google.pubsub.v1.ProjectSnapshotName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Snapshot;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/**
 * PubSubDirectDStream implementation
 */
public class PubSubDirectDStream extends InputDStream<PubSubMessage> implements StreamingEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubDirectDStream.class);

  private static final long maxMessages = 10L;
  private static final long maxMessageSize = 20 * 1024 * 1024; // 20 mb
  private PubSubSubscriberConfig config;
  private long readDuration;
  private String snapShotName;
  private io.cdap.cdap.etl.api.streaming.StreamingContext context;
  private StreamingContext streamingContext;

  private String prevSnapshotName;

  public PubSubDirectDStream(io.cdap.cdap.etl.api.streaming.StreamingContext context, PubSubSubscriberConfig config,
                             long readDuration, String snapShotName) {
    super(context.getSparkStreamingContext().ssc(), scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    this.streamingContext = context.getSparkStreamingContext().ssc();
    this.config = config;
    this.readDuration = readDuration;
    this.snapShotName = snapShotName;
    this.context = context;
  }

  @Override
  public Option<RDD<PubSubMessage>> compute(Time validTime) {
    this.prevSnapshotName = seekSnapshot(config.getProject(), config.getSubscription(), snapShotName);

    Option<RDD<PubSubMessage>> rdd = Option.apply(
      new PubSubRDD(streamingContext.sparkContext(), maxMessages, maxMessageSize, readDuration, config));
    return rdd;
  }

  private String seekSnapshot(String projectId, String subscriptionId, String snapshotName) {
    if (snapshotName == null || snapshotName.isEmpty()) {
      return createAndSave(projectId, subscriptionId, subscriptionId + "-snapshot-" + UUID.randomUUID());
    }

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectSnapshotName projectSnapshotName = ProjectSnapshotName.of(projectId, snapshotName);
      subscriptionAdminClient.seek(
        SeekRequest.newBuilder().setSnapshot(projectSnapshotName.toString())
          .setSubscription(ProjectSubscriptionName.of(projectId, subscriptionId).toString())
          .build());
      return snapshotName;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (NotFoundException e) {
      //Saved snapshot is not found. Create one and save it.
      return createAndSave(projectId, subscriptionId, snapshotName);
    }
  }

  private String createAndSave(String projectId, String subscriptionId, String snapshotName) {
    Snapshot snapshot = createSnapshot(projectId, subscriptionId, snapshotName);
    saveSnapshot(snapshot);
    return snapshot.getName();
  }

  private Snapshot createSnapshot(String projectId, String subscriptionId,
                                 String snapshotName) {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      CreateSnapshotRequest request =
        CreateSnapshotRequest.newBuilder()
          .setName(ProjectSnapshotName.of(projectId, snapshotName).toString())
          .setSubscription(
            ProjectSubscriptionName.of(projectId, subscriptionId).toString())
          .putAllLabels(new HashMap<>())
          .build();
      return subscriptionAdminClient.createSnapshot(request);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteSnapshot(String projectId, String snapshotName) {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectSnapshotName projectSnapshotName = ProjectSnapshotName.of(projectId, snapshotName);
      subscriptionAdminClient.deleteSnapshot(projectSnapshotName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void onBatchCompleted(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    // take snapshot
    String newSnapshotName = snapShotName + "-" + UUID.randomUUID();
    Snapshot snapshot = createSnapshot(config.getProject(), config.getSubscription(), newSnapshotName);
    saveSnapshot(snapshot);
    LOG.info("Saved new snapshot {} for batch.", newSnapshotName);
    deleteSnapshot(config.getProject(), prevSnapshotName);
    LOG.info("Deleted old snapshot {} for batch.", prevSnapshotName);
  }

  private void saveSnapshot(Snapshot snapshot) {
    try {
      // context.getSparkExecutionContext().getSparkExecutionContext().saveState();
      LOG.info("Saving snapshot {} .", snapshot.getName());
      context.saveState(config.getSubscription(), snapshot.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
