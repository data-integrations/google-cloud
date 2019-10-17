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

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;

import java.time.Instant;
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

        key.getAncestors()
          .forEach(a -> appendKindWithNameOrId(builder, a.getKind(), a.getNameOrId()).append(", "));

        appendKindWithNameOrId(builder, key.getKind(), key.getNameOrId());
        return builder.append(")").toString();

      case URL_SAFE_KEY:
        return key.toUrlSafe();

      default:
        throw new IllegalStateException(
          String.format("Unable to transform key '%s' to type '%s' string representation", key, keyType.getValue()));
    }
  }

  /**
   * Appends to the string builder name or id based on given instance type.
   * Name will be enclosed into the single quotes.
   *
   * @param builder  string builder
   * @param kind     kind name
   * @param nameOrId object representing name or id
   * @return updated string builder
   */
  private StringBuilder appendKindWithNameOrId(StringBuilder builder, String kind, Object nameOrId) {
    builder.append(kind).append(", ");
    if (nameOrId instanceof Long) {
      builder.append(nameOrId);
    } else {
      builder.append("'").append(nameOrId).append("'");
    }
    return builder;
  }

  private void populateRecordBuilder(StructuredRecord.Builder builder, FullEntity<?> entity,
                                     String fieldName, Schema fieldSchema) {
    Object value = entity.contains(fieldName)
      ? getValue(entity.getValue(fieldName), fieldName, fieldSchema)
      : null;
    builder.set(fieldName, value);
  }

  private Object getValue(Value<?> value, String fieldName, Schema fieldSchema) {
    if (value instanceof NullValue) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      // GC timestamp supports nano second level precision, CDAP only micro second level precision
      if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
        Timestamp timestamp = castValue(value, fieldName, logicalType.getToken(), TimestampValue.class).get();
        Instant zonedInstant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
        long micros = TimeUnit.SECONDS.toMicros(zonedInstant.getEpochSecond());
        return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(zonedInstant.getNano()));
      }
      throw new IllegalStateException(
        String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        return castValue(value, fieldName, fieldType.toString(), StringValue.class).get();
      case DOUBLE:
        return castValue(value, fieldName, fieldType.toString(), DoubleValue.class).get();
      case BOOLEAN:
        return castValue(value, fieldName, fieldType.toString(), BooleanValue.class).get();
      case LONG:
        return castValue(value, fieldName, fieldType.toString(), LongValue.class).get();
      case BYTES:
        return castValue(value, fieldName, fieldType.toString(), BlobValue.class).get().toByteArray();
      case RECORD:
        FullEntity<?> nestedEntity = castValue(value, fieldName, fieldType.toString(), EntityValue.class).get();
        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(fieldSchema);
        Objects.requireNonNull(fieldSchema.getFields()).forEach(
          nestedField -> populateRecordBuilder(nestedBuilder, nestedEntity,
                                               nestedField.getName(), nestedField.getSchema()));
        return nestedBuilder.build();
      case ARRAY:
        Schema componentSchema = fieldSchema.getComponentSchema();
        List<? extends Value<?>> arrayValues = castValue(value, fieldName, fieldType.toString(), ListValue.class).get();
        return arrayValues.stream()
          .map(v -> getValue(v, fieldName, componentSchema))
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
                        fieldName, value.getType(), unionSchemas));
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, fieldType));
    }
  }

  private <T> T castValue(Value<?> value, String fieldName, String fieldType, Class<T> clazz) {
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, fieldType));
  }

}
