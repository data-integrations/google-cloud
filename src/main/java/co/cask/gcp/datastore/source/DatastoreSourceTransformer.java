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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.source.util.SourceKeyType;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.common.annotations.VisibleForTesting;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Transforms Google Cloud Datastore {@link Entity} to {@link StructuredRecord} using {@link DatastoreSourceConfig}.
 */
public class DatastoreSourceTransformer {

  private final DatastoreSourceConfig config;
  private final Schema schema;

  public DatastoreSourceTransformer(DatastoreSourceConfig config) {
    this.config = config;
    this.schema = config.getSchema();
  }

  public StructuredRecord transformEntity(Entity entity) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields());
    for (Schema.Field field : fields) {
      String fieldName = field.getName();

      if (config.isIncludeKey() && fieldName.equals(config.getKeyAlias())) {
        builder.set(fieldName, transformKeyToKeyString(entity.getKey(), config.getKeyType()));
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
   * @param keyType key type
   * @return key string representation
   */
  @VisibleForTesting
  String transformKeyToKeyString(Key key, SourceKeyType keyType) {
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
    if (!entity.contains(fieldName) || entity.isNull(fieldName)) {
      if (!fieldSchema.isNullable()) {
        throw new IllegalArgumentException("Can not set null value to a non-nullable field: " + fieldName);
      }
      builder.set(fieldName, null);
      return;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case TIMESTAMP_MICROS:
          // GC timestamp supports nano second level precision, CDAP only micro second level precision
          Timestamp timestamp = entity.getTimestamp(fieldName);
          Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
          builder.setTimestamp(fieldName,
                               ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
          return;
        default:
          throw new IllegalStateException(
            String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
      }
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        builder.set(fieldName, entity.getString(fieldName));
        break;
      case DOUBLE:
        builder.set(fieldName, entity.getDouble(fieldName));
        break;
      case BOOLEAN:
        builder.set(fieldName, entity.getBoolean(fieldName));
        break;
      case LONG:
        builder.set(fieldName, entity.getLong(fieldName));
        break;
      case BYTES:
        builder.set(fieldName, entity.getBlob(fieldName).toByteArray());
        break;
      case RECORD:
        FullEntity<?> nestedEntity = entity.getEntity(fieldName);
        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(fieldSchema);
        Objects.requireNonNull(fieldSchema.getFields()).forEach(
          nestedField -> populateRecordBuilder(nestedBuilder, nestedEntity,
                                               nestedField.getName(), nestedField.getSchema()));
        builder.set(fieldName, nestedBuilder.build());
        break;
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (fieldSchema.isNullable()) {
          populateRecordBuilder(builder, entity, fieldName, fieldSchema.getNonNullable());
          break;
        }
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type 'complex UNION'", fieldName));
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, fieldType));
    }
  }
}
