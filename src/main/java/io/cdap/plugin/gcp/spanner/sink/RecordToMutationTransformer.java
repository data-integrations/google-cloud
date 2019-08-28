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

package io.cdap.plugin.gcp.spanner.sink;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transforms CDAP {@link StructuredRecord} to Google Spanner {@link Mutation}.
 */
public class RecordToMutationTransformer {
  private final String tableName;
  private final Schema schema;

  public RecordToMutationTransformer(String tableName, Schema schema) {
    this.tableName = tableName;
    this.schema = schema;
  }

  public Mutation transform(StructuredRecord record) {
    Mutation.WriteBuilder builder = com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder(tableName);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Schema fieldSchema = field.getSchema();
      builder.set(name).to(convertToValue(name, fieldSchema, record));
    }

    return builder.build();
  }

  Value convertToValue(String fieldName, Schema fieldSchema, StructuredRecord record) {
    Schema schema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      if (record.get(fieldName) != null) {
        switch (logicalType) {
          case DATE:
            LocalDate date = record.getDate(fieldName);
            Date spannerDate = Date.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            return Value.date(spannerDate);
          case TIMESTAMP_MILLIS:
          case TIMESTAMP_MICROS:
            ZonedDateTime ts = record.getTimestamp(fieldName);
            Timestamp spannerTs = Timestamp.ofTimeSecondsAndNanos(ts.toEpochSecond(), ts.getNano());
            return Value.timestamp(spannerTs);
          default:
            throw new IllegalStateException(
              String.format("Field '%s' is of unsupported logical type '%s'", fieldName, logicalType.getToken()));
        }
      }
    }

    Schema.Type type = schema.getType();
    switch (type) {
      case BOOLEAN:
        return Value.bool(record.<Boolean>get(fieldName));
      case STRING:
        return Value.string(record.get(fieldName));
      case INT:
        Integer intValue = record.<Integer>get(fieldName);
        if (intValue == null) {
          return Value.int64(null);
        } else {
          return Value.int64(intValue.longValue());
        }
      case LONG:
        return Value.int64(record.<Long>get(fieldName));
      case FLOAT:
        Float floatValue = record.<Float>get(fieldName);
        if (floatValue == null) {
          return Value.float64(null);
        } else {
          return Value.float64(floatValue.doubleValue());
        }
      case DOUBLE:
        return Value.float64(record.<Double>get(fieldName));
      case BYTES:
        Object byteObject = record.get(fieldName);

        if (byteObject == null) {
          return Value.bytes(null);
        }

        if (byteObject instanceof ByteBuffer) {
          return Value.bytes(ByteArray.copyFrom((ByteBuffer) byteObject));
        } else {
          return Value.bytes(ByteArray.copyFrom((byte[]) byteObject));
        }
      case ARRAY:
        Schema componentSchema = schema.getComponentSchema();
        if (componentSchema == null) {
          throw new IllegalStateException(
            String.format("Component schema of array field '%s' is null", fieldName));
        }
        return transformCollectionToValue(fieldName, componentSchema, record.get(fieldName));
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, type));
    }
  }

  private Value transformCollectionToValue(String name, Schema schema, Object object) {
    Schema componentSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema elementSchema = Schema.recordOf("arrayElementSchema",
                                           Schema.Field.of(name, Schema.nullableOf(componentSchema)));

    Collection<Object> arrayValues = toCollection(name, componentSchema.getType().toString(), object);
    List<Value> values = arrayValues.stream()
      .map(value -> {
        // TODO CDAP-15727 - remove wrap into record
        // Wrap array element into record to be able to re-use convertToValue method type conversion logic
        // because StructuredRecord has different methods to extract value based on schema type.
        // For example: record.get(fieldName) is used for most of the values,
        // record.getTimestamp(fieldName) is used for timestamp values.
        StructuredRecord structuredRecord = StructuredRecord.builder(elementSchema)
          .set(name, value)
          .build();
        return convertToValue(name, elementSchema.getField(name).getSchema(), structuredRecord);
      })
      .collect(Collectors.toList());

    Schema.LogicalType logicalType = componentSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return Value.dateArray(
            values.stream()
              .map(value -> value.isNull() ? null : value.getDate())
              .collect(Collectors.toList())
          );
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return Value.timestampArray(
            values.stream()
              .map(value -> value.isNull() ? null : value.getTimestamp())
              .collect(Collectors.toList())
          );
        default:
          throw new IllegalStateException(
            String.format("Field '%s' is an array of unsupported logical type '%s'", name, logicalType.getToken()));
      }
    }

    Schema.Type type = componentSchema.getType();
    switch (type) {
      case BOOLEAN:
        return Value.boolArray(
          values.stream()
            .map(value -> value.isNull() ? null : value.getBool())
            .collect(Collectors.toList())
        );
      case STRING:
        return Value.stringArray(
          values.stream()
            .map(value -> value.isNull() ? null : value.getString())
            .collect(Collectors.toList())
        );
      case INT:
      case LONG:
        return Value.int64Array(
          values.stream()
            .map(value -> value.isNull() ? null : value.getInt64())
            .collect(Collectors.toList())
        );
      case FLOAT:
      case DOUBLE:
        return Value.float64Array(
          values.stream()
            .map(value -> value.isNull() ? null : value.getFloat64())
            .collect(Collectors.toList())
        );
      case BYTES:
        return Value.bytesArray(
          values.stream()
            .map(value -> value.isNull() ? null : value.getBytes())
            .collect(Collectors.toList())
        );
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is an array of '%s', which is not supported.", name, type));
    }

  }

  static Collection<Object> toCollection(String fieldName, String fieldType, Object value) {
    if (value instanceof Collection) {
      return (Collection<Object>) value;
    } else if (value.getClass().isArray()) {
      return convertToObjectCollection(value);
    } else {
      throw new UnexpectedFormatException(
        String.format("Field '%s' of type '%s' has unexpected value '%s'", fieldName, fieldType, value));
    }
  }

  private static Collection<Object> convertToObjectCollection(Object array) {
    Class ofArray = array.getClass().getComponentType();
    if (ofArray.isPrimitive()) {
      List<Object> list = new ArrayList<>();
      int length = Array.getLength(array);
      for (int i = 0; i < length; i++) {
        list.add(Array.get(array, i));
      }
      return list;
    } else {
      return Arrays.asList((Object[]) array);
    }
  }
}
