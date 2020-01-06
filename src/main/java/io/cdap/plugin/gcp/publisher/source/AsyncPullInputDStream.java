package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Test Version.
 */
public class AsyncPullInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncPullInputDStream.class);
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
  private Subscriber subscriber;
  private RDD<StructuredRecord> structuredRecordRDD;
  private JavaStreamingContext sparkStreamingContext;

  public AsyncPullInputDStream(JavaStreamingContext sparkStreamingContext, String projectId,
                               String subscriptionId, String serviceAccountFilePath) {
    super(sparkStreamingContext.ssc(), scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class));
    this.projectId = projectId;
    this.subscriptionId = subscriptionId;
    this.serviceAccountFilePath = serviceAccountFilePath;
    this.sparkStreamingContext = sparkStreamingContext;
  }

  @Override
  public void start() {
    LOG.info("Project ID", projectId);
    LOG.info("Subscription ID", subscriptionId);
    LOG.info("Service account file path", serviceAccountFilePath);

    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
    CredentialsProvider credentialsProvider;

    try {
      credentialsProvider = FixedCredentialsProvider.create(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
    } catch (Exception e) {
      LOG.error("Failed to load service account credentials", e);
      return;
    }

    LOG.info("Successfully loaded service account credentials");

    MessageReceiver receiver =
      new MessageReceiver() {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
          // handle incoming message, then ack/nack the received message
          // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
          Map<String, String> hashMap = new HashMap<>();
          if (message.getAttributesMap() != null) {
            hashMap.putAll(message.getAttributesMap());
          }

          structuredRecordRDD = JavaRDD.<StructuredRecord>toRDD(
            sparkStreamingContext.sparkContext().parallelize(
              Collections.singletonList(StructuredRecord.builder(DEFAULT_SCHEMA)
                                        .set("message", message.getData())
                                        .set("id", message.getMessageId())
                                        .setTimestamp("timestamp", getTimestamp(message.getPublishTime()))
                                        .set("attributes", hashMap)
                                        .build())));

          LOG.info("Id : " + message.getMessageId());
          LOG.info("Data : " + message.getData().toStringUtf8());
          consumer.ack();
        }
      };

    try {
      // Create a subscriber for "my-subscription-id" bound to the message receiver
      subscriber = Subscriber.newBuilder(subscriptionName, receiver)
        .setCredentialsProvider(credentialsProvider)
        .build();
      LOG.info("Successfully created subscriber");
      subscriber.startAsync().awaitRunning();
      LOG.info("Running async pulling...");
      // Allow the subscriber to run indefinitely unless an unrecoverable error occurs
      subscriber.awaitTerminated();
    } catch (Exception e) {
      LOG.error("Caught error", e);
    } finally {
      stop();
    }
  }

  @Override
  public void stop() {
    if (subscriber != null) {
      subscriber.stopAsync();
    }
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time time) {
    return Option.apply(structuredRecordRDD);
  }

  private ZonedDateTime getTimestamp(Timestamp publishTime) {
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(publishTime.getSeconds()).plusNanos(publishTime.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }
}
