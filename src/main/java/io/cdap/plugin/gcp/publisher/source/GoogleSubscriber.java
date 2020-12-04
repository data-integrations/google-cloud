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

import com.google.cloud.Timestamp;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.publisher.PubSubConstants;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.reflect.ClassTag;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read messages from Google PubSub.")
public class GoogleSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSubscriber.class);
  private static final String SCHEMA = "schema";
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                            Schema.of(Schema.Type.STRING)))
    );

  private SubscriberConfig config;

  public GoogleSubscriber(SubscriberConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    Schema schema = context.getOutputSchema();
    // record dataset lineage
    context.registerLineage(config.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from Pub/Sub",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {
    String serviceAccount = config.getServiceAccount();
    SparkGCPCredentials credentials = new GCPCredentialsProvider(serviceAccount, config.isServiceAccountFilePath());

    boolean autoAcknowledge = true;
    if (streamingContext.isPreviewEnabled()) {
      autoAcknowledge = false;
    }
    Option<String> topic = Option.apply(config.topic);
    ReceiverInputDStream<SparkPubsubMessage> stream =
      PubsubUtils.createStream(streamingContext.getSparkStreamingContext().ssc(), config.getProject(),
                               topic, config.subscription, credentials,
                               StorageLevel.MEMORY_ONLY(), autoAcknowledge);
    ClassTag<SparkPubsubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(SparkPubsubMessage.class);
    JavaReceiverInputDStream<SparkPubsubMessage> pubSubMessages = new JavaReceiverInputDStream<>(stream, tag);

    Schema customMessageSchema = getCustomMessageSchema();
    final Schema outputSchema = config.getSchema();
    final String format = config.getFormat().toLowerCase();

    return pubSubMessages.map(pubSubMessage -> {
      // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
      HashMap<String, String> hashMap = new HashMap<>();
      if (pubSubMessage.getAttributes() != null) {
        hashMap.putAll(pubSubMessage.getAttributes());
      }
      StructuredRecord payload = getStructuredRecord(customMessageSchema, format, pubSubMessage);

      return StructuredRecord.builder(outputSchema)
        .set("message", (format.equalsIgnoreCase(PubSubConstants.TEXT) ||
          format.equalsIgnoreCase(PubSubConstants.BLOB)) ?
          pubSubMessage.getData() : payload)
        .set("id", pubSubMessage.getMessageId())
        .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime()))
        .set("attributes", hashMap)
        .build();
    });
  }

  private StructuredRecord getStructuredRecord(Schema customMessageSchema, String format,
                                               SparkPubsubMessage pubSubMessage) throws IOException {
    StructuredRecord payload = null;
    final String data = new String(pubSubMessage.getData());

    switch (format) {
      case PubSubConstants.AVRO:
      case PubSubConstants.PARQUET: {
        final byte[] payloadData = pubSubMessage.getData();
        final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
          parse(String.valueOf(customMessageSchema));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        ByteArrayInputStream in = new ByteArrayInputStream(payloadData);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        GenericRecord record = datumReader.read(null, decoder);
        payload = new AvroToStructuredTransformer().transform(record);
        break;
      }
      case PubSubConstants.CSV: {
        payload = StructuredRecordStringConverter.fromDelimitedString(data, ",", customMessageSchema);
        break;
      }
      case PubSubConstants.DELIMITED: {
        payload = StructuredRecordStringConverter.fromDelimitedString(data, config.delimiter,
                                                                      customMessageSchema);
        break;
      }
      case PubSubConstants.JSON: {
        payload = StructuredRecordStringConverter.fromJsonString(data, customMessageSchema);
        break;
      }
      case PubSubConstants.TSV: {
        payload = StructuredRecordStringConverter.fromDelimitedString(data, "\t", customMessageSchema);
        break;
      }
    }
    return payload;
  }

  private Schema.Field getMessageField() {
    Schema schema = config.getSchema();
    return schema.getField("message");
  }

  private Schema getCustomMessageSchema() {
    Schema.Field messageField = getMessageField();
    if (messageField == null) {
      return null;
    }
    return messageField.getSchema();
  }
  private ZonedDateTime getTimestamp(String publishTime) {
    Timestamp timestamp = Timestamp.parseTimestamp(publishTime);
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  /**
   * Configuration class for the subscriber source.
   */
  public static class SubscriberConfig extends GCPReferenceSourceConfig {

    @Description("Cloud Pub/Sub subscription to read from. If a subscription with the specified name does not " +
      "exist, it will be automatically created if a topic is specified. Messages published before the subscription " +
      "was created will not be read.")
    @Macro
    private String subscription;

    @Description("Cloud Pub/Sub topic to create a subscription on. This is only used when the specified  " +
      "subscription does not already exist and needs to be automatically created. If the specified " +
      "subscription already exists, this value is ignored.")
    @Macro
    @Nullable
    private String topic;

    @Macro
    @Nullable
    @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'tsv', 'csv', 'delimited', 'json', "
      + "'parquet' and 'text'.")
    private String format;

    @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
      + "is anything other than 'delimited'.")
    @Macro
    @Nullable
    private String delimiter;

    @Name(SCHEMA)
    @Macro
    @Nullable
    private String schema;

    public void validate(FailureCollector collector) {
      super.validate(collector);
      final Schema outputSchema = getSchema();
      final ArrayList<String> defaultSchemaFields = getFieldsOfDefaultSchema();
      String regAllowedChars = "[A-Za-z0-9-.%~+_]*$";
      String regStartWithLetter = "[A-Za-z]";

      ArrayList<String> outputSchemaFields = new ArrayList<>();

      if (outputSchema != null) {
        for (Schema.Field field : Objects.requireNonNull(outputSchema.getFields())) {
          outputSchemaFields.add(field.getName());
        }

        for (Schema.Field field : Objects.requireNonNull(DEFAULT_SCHEMA.getFields())) {
          if (!outputSchemaFields.contains(field.getName())) {
            collector.addFailure("Some required fields are missing from the schema.",
                                 String.format("You should use the existing fields of default schema %s.",
                                               defaultSchemaFields))
              .withConfigProperty(schema);
          }
        }

        for (Schema.Field field : Objects.requireNonNull(outputSchema.getFields())) {
          Schema.Field outputField = DEFAULT_SCHEMA.getField(field.getName());
          if (field.getSchema().isNullable()) {
            collector.addFailure(String.format("Null is not allowed in %s.", field.getName()),
                                 "Schema is non-nullable")
              .withConfigProperty(schema);
          }
          if (outputField == null) {
            collector.addFailure(String.format("Field %s is not allowed.", field.getName()),
                                 "You should use the existing fields of default schema.")
              .withConfigProperty(schema);
          } else {
            Schema.Type fieldType = field.getSchema().getType();
            if (field.getName().equals("message")) {
              if (!(fieldType == Schema.Type.RECORD || fieldType == Schema.Type.BYTES)) {
                collector.addFailure(String.format("Type %s is not allowed in %s.",
                                                   fieldType.toString().toLowerCase(), field.getName()),
                                     "Type should be record or byte.")
                  .withConfigProperty(schema);
              }
              continue;
            }
            if (!fieldType.equals(outputField.getSchema().getType())) {
              collector.addFailure(String.format("Type %s is not allowed in %s.",
                                                 fieldType.toString().toLowerCase(), field.getName()),
                                   String.format("You should use the same type [%s] as in default schema.",
                                                 outputField.getSchema().toString()))
                .withConfigProperty(schema);
            }
          }
        }
      }

      if (!getSubscription().matches(regAllowedChars)) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Unexpected Character. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().startsWith("goog")) {
        collector.addFailure("Subscription Name does not match naming convention.",
          " Cannot Start with String goog. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (!getSubscription().substring(0, 1).matches(regStartWithLetter)) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Name must start with a letter. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().length() < 3  || getSubscription().length() > 255) {
        collector.addFailure("Subscription Name does not match naming convention.",
          "Character Length must be between 3 and 255 characters. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }

      if (!containsMacro(PubSubConstants.DELIMITER) && (!containsMacro(PubSubConstants.FORMAT) &&
        getFormat().equalsIgnoreCase(PubSubConstants.DELIMITED) && delimiter == null)) {
        collector.addFailure(String.format("Delimiter is required when format is set to %s.", getFormat()),
                             "Ensure the delimiter is provided.")
          .withConfigProperty(delimiter);
      }

      collector.getOrThrowException();
    }

    public String getSubscription() {
      return subscription;
    }

    public String getFormat() {
      return Strings.isNullOrEmpty(format) ? PubSubConstants.TEXT : format;
    }

    public ArrayList<String> getFieldsOfDefaultSchema() {
      ArrayList<String> outputSchemaAttributes = new ArrayList<>();
      for (Schema.Field field : Objects.requireNonNull(DEFAULT_SCHEMA.getFields())) {
        outputSchemaAttributes.add(field.getName());
      }
      return outputSchemaAttributes;
    }

    public Schema getSchema() {
      try {
        if (containsMacro(SCHEMA)) {
          return null;
        }
        return Strings.isNullOrEmpty(schema) ? DEFAULT_SCHEMA : Schema.parseJson(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema with error %s, %s",
                                                         e.getMessage(), e));
      }
    }
  }
}
