package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * PubSubRDD to be used with streaming source
 */
public class PubSubRDD extends RDD<PubSubMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubRDD.class);

  private long maxMessages;

  private long maxMessageSize;
  private String projectId;
  private String subscriptionId;
  private Credentials credentials;
  private long readDuration;

  PubSubRDD(SparkContext sparkContext, long maxMessages, long maxMessageSize, long readDuration,
            PubSubSubscriberConfig config) {
    super(sparkContext, scala.collection.JavaConverters.asScalaBuffer(Collections.emptyList()),
          scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));

    this.maxMessages = maxMessages;
    this.maxMessageSize = maxMessageSize;
    this.readDuration = readDuration;
    this.projectId = config.getProject();
    this.subscriptionId = config.getSubscription();
    this.credentials = createCredentials(config);

  }

  @Override
  public Iterator<PubSubMessage> compute(Partition split, TaskContext context) {
    try {
      List<PubSubMessage> messages = fetch();
      return scala.collection.JavaConverters.asScalaIterator(messages.iterator());
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to compute RDD for partition {}. ", split.index()), e);
    }
  }

  protected Credentials createCredentials(PubSubSubscriberConfig config) {
    try {
      return config.getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(),
                                                      config.isServiceAccountFilePath());
    } catch (IOException e) {
      throw new RuntimeException("Unable to get credentials for receiver.", e);
    }
  }

  @Override
  public Partition[] getPartitions() {
    Partition partition = () -> 0;
    return new Partition[]{partition};
  }

  private List<PubSubMessage> fetch() throws IOException {
    if (maxMessages <= 0) {
      return Collections.emptyList();
    }

    List<PubSubMessage> messagesRead = new ArrayList<>();
    ProjectSubscriptionName subscriptionName =
      ProjectSubscriptionName.of(projectId, subscriptionId);

    // Instantiate an asynchronous message receiver.
    MessageReceiver receiver =
      (PubsubMessage message, AckReplyConsumer consumer) -> {
        // Handle incoming message, then ack the received message.
        Instant instant = Instant.ofEpochSecond(message.getPublishTime().getSeconds())
          .plusNanos(message.getPublishTime().getNanos());
        PubSubMessage pubSubMessage = new PubSubMessage(message.getMessageId(), message.getOrderingKey(), null,
                                                        message.getData().toByteArray(), message.getAttributesMap(),
                                                        instant);
        messagesRead.add(pubSubMessage);
        //TODO -  dont ack if this is preview
        consumer.ack();
      };


    Subscriber.Builder builder = Subscriber.newBuilder(subscriptionName, receiver);
    FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
      .setLimitExceededBehavior(FlowController.LimitExceededBehavior.ThrowException)
      .setMaxOutstandingElementCount(maxMessages)
      .setMaxOutstandingRequestBytes(maxMessageSize)
      .build();
    builder.setFlowControlSettings(flowControlSettings);
    Subscriber subscriber = builder.build();
    try {
      // Start the subscriber.
      subscriber.startAsync().awaitRunning();
      LOG.info(String.format("Listening for messages in RDD on %s .", subscriptionName));
      // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
      subscriber.awaitTerminated(readDuration, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      // Shut down the subscriber after 30s. Stop receiving messages.
      subscriber.stopAsync();
    }
    return messagesRead;
  }
}
