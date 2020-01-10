package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test Version.
 */
public class SyncPullBasedInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SyncPullBasedInputDStream.class);
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );

  private final String projectId;
  private final String subscriptionId;
  private final String serviceAccountFilePath;
  private JavaStreamingContext sparkStreamingContext;
  private String subscriptionName;
  private SubscriberStub subscriber;
  private final int numOfThreads = 7;
  private ExecutorService executorService;

  public SyncPullBasedInputDStream(JavaStreamingContext sparkStreamingContext, String projectId,
                                   String subscriptionId, String serviceAccountFilePath) {
    super(sparkStreamingContext.ssc(), scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class));
    this.projectId = projectId;
    this.subscriptionId = subscriptionId;
    this.serviceAccountFilePath = serviceAccountFilePath;
    this.sparkStreamingContext = sparkStreamingContext;
    this.executorService = Executors.newFixedThreadPool(numOfThreads);
  }

  class Process implements Callable<List<StructuredRecord>> {

    private final int id;

    Process(int id) {
      this.id = id;
    }

    @Override
    public List<StructuredRecord> call() throws Exception {
      LOG.info("Thread" + id + ": Started computing");

      PullRequest pullRequest =
        PullRequest.newBuilder()
          .setMaxMessages(1000)
          .setReturnImmediately(true) // return immediately if messages are not available
          .setSubscription(subscriptionName)
          .build();

      PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
      List<ReceivedMessage> receivedMessagesList = pullResponse.getReceivedMessagesList();

      LOG.info("Thread" + id + ": Received message back, size is " + receivedMessagesList.size());

      List<String> ackIds = new ArrayList<>(receivedMessagesList.size());
      List<StructuredRecord> records = new ArrayList<>(receivedMessagesList.size());

      for (ReceivedMessage message : receivedMessagesList) {
        PubsubMessage pubSubMessage = message.getMessage();
        // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
        Map<String, String> hashMap = new HashMap<>();
        if (pubSubMessage.getAttributesMap() != null) {
          hashMap.putAll(pubSubMessage.getAttributesMap());
        }

        records.add(StructuredRecord.builder(DEFAULT_SCHEMA)
                      .set("message", pubSubMessage.getData().toByteArray())
                      .set("id", pubSubMessage.getMessageId())
                      .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime()))
                      .set("attributes", hashMap)
                      .build());
        ackIds.add(message.getAckId());
      }

      if (!records.isEmpty()) {
        // acknowledge received messages
        AcknowledgeRequest acknowledgeRequest =
          AcknowledgeRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAllAckIds(ackIds)
            .build();

        // use acknowledgeCallable().futureCall to asynchronously perform this operation
        subscriber.acknowledgeCallable().call(acknowledgeRequest);

        LOG.info("Thread" + id + ": Messaged are acked");
      } else {
        LOG.info("Thread" + id + ": No returned messages");
      }

      return records;
    }
  }

  @Override
  public void start() {
    LOG.info("Project ID: " + projectId);
    LOG.info("Subscription ID: " + subscriptionId);
    LOG.info("Service account file path: " + serviceAccountFilePath);

    subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
    CredentialsProvider credentialsProvider;

    try {
      if (serviceAccountFilePath == null) {
        credentialsProvider = FixedCredentialsProvider.create(
          GoogleCredentials.getApplicationDefault());
      } else {
        credentialsProvider = FixedCredentialsProvider.create(
          GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
      }
    } catch (Exception e) {
      LOG.error("Failed to load service account credentials", e);
      return;
    }

    LOG.info("Successfully loaded service account credentials");

    SubscriberStubSettings subscriberStubSettings;

    try {
      subscriberStubSettings =
        SubscriberStubSettings.newBuilder()
          .setCredentialsProvider(credentialsProvider)
          .build();
    } catch (Exception e) {
      LOG.error("Failed to init subscriberSetting", e);
      return;
    }

    LOG.info("Successfully init subscriberSetting");

    try {
      subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
    } catch (Exception e) {
      LOG.error("Failed to create subscriber");
      return;
    }

    LOG.info("Successfully created subscriber");
  }

  @Override
  public void stop() {
    if (subscriber != null) {
      subscriber.close();
    }

    if (executorService != null) {
      executorService.shutdown();
    }
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time time) {
    List<Callable<List<StructuredRecord>>> callables = new ArrayList<>();
    for (int i = 0; i < numOfThreads; i++) {
      callables.add(new Process(i));
    }

    try {
      List<Future<List<StructuredRecord>>> results = executorService.invokeAll(callables);
      List<StructuredRecord> records = new ArrayList<>();

      for (Future<List<StructuredRecord>> result : results) {
        records.addAll(result.get());
      }

      LOG.info("Total message per interval: " + records.size());

      RDD<StructuredRecord> structuredRecordRDD =
        JavaRDD.toRDD(sparkStreamingContext.sparkContext().parallelize(records));

      return Option.apply(structuredRecordRDD);
    } catch (Exception e) {
      LOG.error("Error occurred: " + e.getMessage(), e);
      return Option.apply(JavaRDD.toRDD(sparkStreamingContext.sparkContext().parallelize(Collections.emptyList())));
    }
  }

  private ZonedDateTime getTimestamp(Timestamp timestamp) {
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }
}
