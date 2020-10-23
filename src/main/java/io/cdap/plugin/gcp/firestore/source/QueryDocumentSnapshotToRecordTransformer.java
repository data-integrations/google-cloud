/*
 * Copyright © 2020 Cask Data, Inc.
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

import java.nio.ByteBuffer;
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

  /**
   * Constructor for QueryDocumentSnapshotToRecordTransformer object.
   * @param schema the schema
   * @param includeDocumentId the include document id
   * @param idAlias the id alias
   */
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
        case DECIMAL:
          ByteBuffer value = (ByteBuffer) object;
          byte[] bytes = new byte[value.remaining()];
          int pos = value.position();
          value.get(bytes);
          value.position(pos);
          return bytes;
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
        ensureTypeValid(fieldName, object, byte[].class);
        return object;
      case LONG:
        ensureTypeValid(fieldName, object, Long.class);
        return object;
      case STRING:
        ensureTypeValid(fieldName, object, String.class);
        return object;
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
          fieldType.name().toLowerCase()));
    }
  }

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
