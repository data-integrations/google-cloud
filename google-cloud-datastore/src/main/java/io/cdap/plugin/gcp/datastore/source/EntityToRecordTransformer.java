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
package io.cdap.plugin.gcp.datastore.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Transforms Google Cloud Datastore {@link Entity} to {@link StructuredRecord}.
 */
public class EntityToRecordTransformer {

  private final Schema schema;
  private final SourceKeyType keyType;
  private final String keyAlias;

  public EntityToRecordTransformer(Schema schema, SourceKeyType keyType, String keyAlias) {
    this.schema = schema;
    this.keyType = keyType;
    this.keyAlias = keyAlias;
  }

  public StructuredRecord transformEntity(Entity entity) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields());
    for (Schema.Field field : fields) {
      String fieldName = field.getName();

      if (SourceKeyType.NONE != keyType && fieldName.equals(keyAlias)) {
        builder.set(fieldName, transformKeyToKeyString(entity.getKey()));
        continue;
      }

      populateRecordBuilder(builder, entity, field.getName(), field.getSchema());

    }
    return builder.build();
  }

  /**
   * Transforms Datastore key to its string representation based on given key type.
   * <p/>
   * Key literal will be represented in key literal format: key(<kind>, <identifier>, <kind>, <identifier>, [...]).
   * For example: `key(kind_1, 'stringId', kind_2, 100)`
   * <p/>
   * Url-safe key will represented in the encoded form that can be used as part of a URL.
   * For example: partition_id+%7B%0A++project_id%3A+%22test-project%22%0A++namespace_id%3A+% ...
   *
   * @param key     Datastore key instance
   * @return key string representation
   */
  @VisibleForTesting
  String transformKeyToKeyString(Key key) {
    switch (keyType) {
      case KEY_LITERAL:
        StringBuilder builder = new StringBuilder("key(");
        boolean firstIteration = true;
        for (Key.PathElement pathElement: key.getPathList()) {
          if (firstIteration) {
            firstIteration = false;
          } else {
            builder.append(", ");
          }
          builder.append(pathElement.getKind()).append(", ");
          switch(pathElement.getIdTypeCase()) {
            case ID:
              builder.append(pathElement.getId());
              break;
            case NAME:
              builder.append("'").append(pathElement.getName()).append("'");
              break;
            default:
              throw new IllegalStateException(
                String.format("Unexpected path element type %s", pathElement.getIdTypeCase().toString()));
          }
        }
        return builder.append(")").toString();

      case URL_SAFE_KEY:
        try {
          return URLEncoder.encode(key.toString(), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
          throw new IllegalStateException("Unexpected encoding exception", e);
        }
      default:
        throw new IllegalStateException(
          String.format("Unable to transform key '%s' to type '%s' string representation", key, keyType.getValue()));
    }
  }

  private void populateRecordBuilder(StructuredRecord.Builder builder, Entity entity,
                                     String fieldName, Schema fieldSchema) {
    Value defaultValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    Value value = entity.getPropertiesOrDefault(fieldName, defaultValue);
    builder.set(fieldName, getValue(value, fieldName, fieldSchema));
  }

  private Object getValue(Value value, String fieldName, Schema fieldSchema) {
    if (value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE) {
      return null;
    }
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      // GC timestamp supports nano second level precision, CDAP only micro second level precision
      switch (logicalType) {
        case TIMESTAMP_MICROS:
          Timestamp timestamp = (Timestamp) castValue(value, Value.ValueTypeCase.TIMESTAMP_VALUE, fieldName);
          Instant zonedInstant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
          long micros = TimeUnit.SECONDS.toMicros(zonedInstant.getEpochSecond());
          return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(zonedInstant.getNano()));
        case DATETIME:
          try {
            LocalDateTime.parse(value.getStringValue());
          } catch (DateTimeParseException exception) {
            throw new UnexpectedFormatException(
              String.format("Datetime field '%s' with value '%s' is not in ISO-8601 format.", fieldName,
                            value.getStringValue()), exception);
          }
          return value.getStringValue();
        default:
          throw new IllegalStateException(
            String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
      }
    }
    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        return castValue(value, Value.ValueTypeCase.STRING_VALUE, fieldName);
      case DOUBLE:
        return castValue(value, Value.ValueTypeCase.DOUBLE_VALUE, fieldName);
      case BOOLEAN:
        return castValue(value, Value.ValueTypeCase.BOOLEAN_VALUE, fieldName);
      case LONG:
        return castValue(value, Value.ValueTypeCase.INTEGER_VALUE, fieldName);
      case BYTES:
        return castValue(value, Value.ValueTypeCase.BLOB_VALUE, fieldName);
      case RECORD:
        Entity nestedEntity = (Entity) castValue(
          value, Value.ValueTypeCase.ENTITY_VALUE, fieldName);
        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(fieldSchema);
        Objects.requireNonNull(fieldSchema.getFields()).forEach(
          nestedField -> populateRecordBuilder(nestedBuilder, nestedEntity,
                                               nestedField.getName(), nestedField.getSchema()));
        return nestedBuilder.build();
      case ARRAY:
        Schema componentSchema = fieldSchema.getComponentSchema();
        ArrayValue arrayValue = (ArrayValue) castValue(
          value, Value.ValueTypeCase.ARRAY_VALUE, fieldName);
        return arrayValue.getValuesList().stream().map(v -> getValue(v, fieldName, componentSchema))
          .collect(Collectors.toList());
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (fieldSchema.isNullable()) {
          return getValue(value, fieldName, fieldSchema.getNonNullable());
        }

        List<Schema> unionSchemas = fieldSchema.getUnionSchemas();
        for (Schema unionSchema : unionSchemas) {
          try {
            return getValue(value, fieldName, unionSchema);
          } catch (UnexpectedFormatException | IllegalStateException e) {
            // if we couldn't convert, move to the next possibility
          }
        }
        throw new IllegalStateException(
          String.format("Field '%s' is of unexpected type '%s'. Declared 'complex UNION' types: %s",
                        fieldName, value.getValueTypeCase(), unionSchemas));
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, fieldType));
    }
  }

  private Object castValue(Value value, Value.ValueTypeCase valueTypeCase, String fieldName) {
    if (value.getValueTypeCase() != valueTypeCase) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' is not of expected type '%s'", fieldName, valueTypeCase.toString()));
    }
    switch(valueTypeCase) {
      case STRING_VALUE: return value.getStringValue();
      case DOUBLE_VALUE: return value.getDoubleValue();
      case BOOLEAN_VALUE: return value.getBooleanValue();
      case INTEGER_VALUE: return value.getIntegerValue();
      case BLOB_VALUE: return value.getBlobValue().toByteArray();
      case ARRAY_VALUE: return value.getArrayValue();
      case ENTITY_VALUE: return value.getEntityValue();
      case TIMESTAMP_VALUE: return value.getTimestampValue();
      default: throw new UnexpectedFormatException(
        String.format("Unexpected value type '%s'", valueTypeCase.toString()));
    }
  }

}
