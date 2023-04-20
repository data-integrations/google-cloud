package io.cdap.plugin.gcp.publisher.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.cdap.plugin.gcp.common.MappingException;
import io.cdap.plugin.gcp.publisher.PubSubConstants;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;

/**
 * RecordUtils functions
 */
public class RecordConverter {

  /**
   * Converts a PubSubMessage into a StructuredRecord based on the specified schema.
   * If no schema is specified, the default schema is used.
   */
  public static SerializableFunction<PubSubMessage, StructuredRecord> getFunction(GoogleSubscriberConfig config) {
    return (pubSubMessage) -> {

      Schema customMessageSchema = getCustomMessageSchema(config);
      final Schema outputSchema = config.getSchema();
      final String format = config.getFormat();

      // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
      HashMap<String, String> hashMap = new HashMap<>();
      if (pubSubMessage.getAttributes() != null) {
        hashMap.putAll(pubSubMessage.getAttributes());
      }

      try {
        StructuredRecord payload = getStructuredRecord(config, customMessageSchema, format, pubSubMessage);

        return StructuredRecord.builder(outputSchema)
          .set("message", (format.equalsIgnoreCase(PubSubConstants.TEXT) ||
            format.equalsIgnoreCase(PubSubConstants.BLOB)) ?
            pubSubMessage.getData() : payload)
          .set("id", pubSubMessage.getMessageId())
          .setTimestamp("timestamp", getTimestamp(pubSubMessage.getPublishTime()))
          .set("attributes", hashMap)
          .build();
      } catch (IOException ioe) {
        throw new MappingException(ioe);
      }
    };
  }

  private static ZonedDateTime getTimestamp(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  private static Schema.Field getMessageField(GoogleSubscriberConfig config) {
    Schema schema = config.getSchema();
    return schema.getField("message");
  }

  private static Schema getCustomMessageSchema(GoogleSubscriberConfig config) {
    Schema.Field messageField = getMessageField(config);
    if (messageField == null) {
      return null;
    }
    return messageField.getSchema();
  }

  private static StructuredRecord getStructuredRecord(GoogleSubscriberConfig config, Schema customMessageSchema,
                                                      String format, PubSubMessage pubSubMessage) throws IOException {
    StructuredRecord payload = null;
    final String data = pubSubMessage.getData() != null ? new String(pubSubMessage.getData()) : "";

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
        payload = StructuredRecordStringConverter.fromDelimitedString(data, config.getDelimiter(),
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
}
