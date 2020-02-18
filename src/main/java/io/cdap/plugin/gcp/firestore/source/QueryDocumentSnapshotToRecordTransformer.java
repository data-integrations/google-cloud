/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Transforms {@link QueryDocumentSnapshot} to {@link StructuredRecord}.
 */
public class QueryDocumentSnapshotToRecordTransformer {

  private final Schema schema;
  private final Boolean includeDocumentId;
  private final String idAlias;

  public QueryDocumentSnapshotToRecordTransformer(Schema schema, Boolean includeDocumentId, String idAlias) {
    this.schema = schema;
    this.includeDocumentId = includeDocumentId;
    this.idAlias = idAlias;
  }

  /**
   * Transforms given {@link QueryDocumentSnapshot} to {@link StructuredRecord}.
   *
   * @param queryDocumentSnapshot queryDocumentSnapshot object to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link QueryDocumentSnapshot}.
   */
  public StructuredRecord transform(QueryDocumentSnapshot queryDocumentSnapshot) {
    return convertRecord(null, queryDocumentSnapshot, schema);
  }

  private StructuredRecord convertRecord(@Nullable String fieldName, QueryDocumentSnapshot object, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields(), "Schema fields cannot be empty");
    for (Schema.Field field : fields) {
      if (includeDocumentId && idAlias.equals(field.getName())) {
        builder.set(field.getName(), object.getId());
      } else {
        // Use full field name for nested records to construct meaningful errors messages.
        // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
        String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) :
          field.getName();

        Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
          : field.getSchema();
        Object value = extractValue(fullFieldName, object.get(field.getName()), nonNullableSchema);
        if (!field.getSchema().isNullable() && value == null) {
          builder.set(field.getName(), "");
        } else {
          builder.set(field.getName(), value);
        }
      }
    }
    return builder.build();
  }

  private Object extractValue(String fieldName, Object object, Schema schema) {
    //if (object == null || object instanceof BsonUndefined) {
    if (object == null) {
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          ensureTypeValid(fieldName, object, Date.class);
          Instant instant = ((Date) object).toInstant();
          long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond());
          return Math.addExact(millis, TimeUnit.NANOSECONDS.toMillis(instant.getNano()));
        case TIMESTAMP_MICROS:
          ensureTypeValid(fieldName, object, Date.class);
          Instant dateInstant = ((Date) object).toInstant();
          long micros = TimeUnit.SECONDS.toMicros(dateInstant.getEpochSecond());
          return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(dateInstant.getNano()));
          /*
        case DECIMAL:
          ensureTypeValid(fieldName, object, Decimal128.class);
          return extractDecimal(fieldName, (Decimal128) object, schema);
          */
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'",
            fieldName, fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        ensureTypeValid(fieldName, object, Boolean.class);
        return object;
      case INT:
        ensureTypeValid(fieldName, object, Integer.class);
        return object;
      case DOUBLE:
        ensureTypeValid(fieldName, object, Double.class);
        return object;
      case BYTES:
        /*
        ensureTypeValid(fieldName, object, ObjectId.class, byte[].class);
        if (object instanceof ObjectId) {
          // ObjectId can be read as a bytes. Exactly 12 bytes are required
          ByteBuffer buffer = ByteBuffer.allocate(12);
          ObjectId objectId = (ObjectId) object;
          objectId.putToByteBuffer(buffer);
          buffer.rewind();
          return buffer;
        }
        */
        return object;
      case LONG:
        ensureTypeValid(fieldName, object, Long.class);
        return object;
      case STRING:
        /*
        ensureTypeValid(fieldName, object, ObjectId.class, String.class);
        if (object instanceof ObjectId) {
          // ObjectId can be read as a string
          return object.toString();
        }
        */
        return object;
      /*
      case MAP:
        ensureTypeValid(fieldName, object, BSONObject.class);
        return convertMap(fieldName, (BSONObject) object, schema);
      case ARRAY:
        ensureTypeValid(fieldName, object, BasicBSONList.class);
        return convertArray(fieldName, (BasicBSONList) object, schema);
      case RECORD:
        ensureTypeValid(fieldName, object, BSONObject.class);
        return convertRecord(fieldName, (BSONObject) object, schema);
       */
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
          fieldType.name().toLowerCase()));
    }
  }

  /*
  private byte[] extractDecimal(String fieldName, Decimal128 decimal128, Schema schema) {
    BigDecimal decimal = decimal128.bigDecimalValue();
    int schemaPrecision = schema.getPrecision();
    int schemaScale = schema.getScale();

    if (decimal.precision() > schemaPrecision) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has precision '%s' which is higher than schema precision '%s'.",
                      fieldName, decimal.precision(), schemaPrecision));
    }

    if (decimal.scale() > schemaScale) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has scale '%s' which is not equal to schema scale '%s'.",
                      fieldName, decimal.scale(), schemaScale));
    }

    return decimal.setScale(schemaScale).unscaledValue().toByteArray();
  }

  private Object convertMap(String fieldName, BSONObject bsonObject, Schema schema) {
    Schema valueSchema = schema.getMapSchema().getValue().isNullable()
      ? schema.getMapSchema().getValue().getNonNullable() : schema.getMapSchema().getValue();
    Map<String, Object> extracted = new HashMap<>();
    for (String key : bsonObject.keySet()) {
      Object value = bsonObject.get(key);
      extracted.put(key, extractValue(fieldName, value, valueSchema));
    }
    return extracted;
  }

  private Object convertArray(String fieldName, BasicBSONList bsonList, Schema schema) {
    List<Object> values = Lists.newArrayListWithCapacity(bsonList.size());
    Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
      : schema.getComponentSchema();
    for (Object obj : bsonList) {
      values.add(extractValue(fieldName, obj, componentSchema));
    }
    return values;
  }
  */

  private void ensureTypeValid(String fieldName, Object value, Class... expectedTypes) {
    for (Class expectedType : expectedTypes) {
      if (expectedType.isInstance(value)) {
        return;
      }
    }

    String expectedTypeNames = Stream.of(expectedTypes)
      .map(Class::getName)
      .collect(Collectors.joining(", "));
    throw new UnexpectedFormatException(
      String.format("Document field '%s' is expected to be of type '%s', but found a '%s'.", fieldName,
        expectedTypeNames, value.getClass().getSimpleName()));
  }
}
