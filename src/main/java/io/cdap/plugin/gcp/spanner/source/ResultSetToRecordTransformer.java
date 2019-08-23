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

package io.cdap.plugin.gcp.spanner.source;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Transforms Google Spanner {@link com.google.cloud.spanner.ResultSet} to CDAP {@link StructuredRecord}.
 */
public class ResultSetToRecordTransformer {
  private final Schema schema;

  public ResultSetToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  public StructuredRecord transform(ResultSet resultSet) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      Type columnType = resultSet.getColumnType(fieldName);
      Type.Code code = columnType.getCode();
      if (columnType == null || (resultSet.isNull(fieldName) && code != Type.Code.ARRAY)) {
        continue;
      }
      switch (columnType.getCode()) {
        case BOOL:
          builder.set(fieldName, resultSet.getBoolean(fieldName));
          break;
        case INT64:
          builder.set(fieldName, resultSet.getLong(fieldName));
          break;
        case FLOAT64:
          builder.set(fieldName, resultSet.getDouble(fieldName));
          break;
        case STRING:
          builder.set(fieldName, resultSet.getString(fieldName));
          break;
        case BYTES:
          ByteArray byteArray = resultSet.getBytes(fieldName);
          builder.set(fieldName, byteArray.toByteArray());
          break;
        case DATE:
          // spanner DATE is a date without time zone. so create LocalDate from spanner DATE
          Date spannerDate = resultSet.getDate(fieldName);
          builder.setDate(fieldName, LocalDate.of(spannerDate.getYear(), spannerDate.getMonth(),
                                                  spannerDate.getDayOfMonth()));
          break;
        case TIMESTAMP:
          Timestamp spannerTs = resultSet.getTimestamp(fieldName);
          // Spanner TIMESTAMP supports nano second level precision, however, cdap schema only supports
          // microsecond level precision.
          Instant instant = Instant.ofEpochSecond(spannerTs.getSeconds()).plusNanos(spannerTs.getNanos());
          builder.setTimestamp(fieldName, ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
          break;
        case ARRAY:
          builder.set(fieldName, transformArrayToList(resultSet, fieldName, field.getSchema(),
                                                      columnType.getArrayElementType()).toArray());
          break;
      }
    }

    return builder.build();
  }

  private List<?> transformArrayToList(ResultSet resultSet, String fieldName, Schema fieldSchema,
                                       Type arrayElementType) {
    if (resultSet.isNull(fieldName)) {
      return Collections.emptyList();
    }

    switch (arrayElementType.getCode()) {
      case BOOL:
        return resultSet.getBooleanList(fieldName);
      case INT64:
        return resultSet.getLongList(fieldName);
      case FLOAT64:
        return resultSet.getDoubleList(fieldName);
      case STRING:
        return resultSet.getStringList(fieldName);
      case BYTES:
        return resultSet.getBytesList(fieldName)
          .stream()
          .map(byteArray -> byteArray == null ? null : byteArray.toByteArray())
          .collect(Collectors.toList());
      case DATE:
        // spanner DATE is a date without time zone. so create LocalDate from spanner DATE
        return resultSet.getDateList(fieldName)
          .stream()
          .map(this::convertDateToLong)
          .collect(Collectors.toList());
      case TIMESTAMP:
        // Spanner TIMESTAMP supports nano second level precision, however, cdap schema only supports
        // microsecond level precision.
        return resultSet.getTimestampList(fieldName)
          .stream()
          .map(timestamp -> convertTimestampToLong(fieldName, timestamp, fieldSchema.getLogicalType()))
          .collect(Collectors.toList());
      default:
        return Collections.emptyList();
    }
  }

  private Integer convertDateToLong(Date date) {
    if (date == null) {
      return null;
    }

    return Math.toIntExact(LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()).toEpochDay());
  }

  private Long convertTimestampToLong(String fieldName, Timestamp timestamp, Schema.LogicalType logicalType) {
    if (timestamp == null) {
      return null;
    }

    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds())
      .plusNanos(timestamp.getNanos())
      .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
      .toInstant();

    try {
      if (logicalType == Schema.LogicalType.TIMESTAMP_MILLIS) {
        long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond());
        return Math.addExact(millis, TimeUnit.NANOSECONDS.toMillis(instant.getNano()));
      }

      long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
      return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
    } catch (ArithmeticException e) {
      throw new UnexpectedFormatException(String.format("Field %s was set to a %s that is too large.", fieldName,
                                                        logicalType.getToken()));
    }
  }
}
