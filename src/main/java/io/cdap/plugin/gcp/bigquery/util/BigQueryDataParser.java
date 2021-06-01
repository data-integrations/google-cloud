/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.util;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * BigQuery Data Parser that parse the BiqQuery query result
 */
public final class BigQueryDataParser {

  private BigQueryDataParser() {
  }

  public static List<StructuredRecord> parse(TableResult result) {
    List<StructuredRecord> samples = new ArrayList<>();

    com.google.cloud.bigquery.Schema schema = result.getSchema();
    Schema cdapSchema = BigQueryUtil.getTableSchema(schema, null);

    FieldList fields = schema.getFields();
    for (FieldValueList fieldValues : result.iterateAll()) {
      StructuredRecord record = getStructuredRecord(cdapSchema, fields, fieldValues);
      samples.add(record);
    }
    return samples;
  }

  public static StructuredRecord getStructuredRecord(Schema schema, FieldList fields, FieldValueList fieldValues) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (Field field : fields) {
      String fieldName = field.getName();
      FieldValue fieldValue = fieldValues.get(fieldName);
      FieldValue.Attribute attribute = fieldValue.getAttribute();
      if (fieldValue.isNull()) {
        recordBuilder.set(fieldName, null);
        continue;
      }

      if (attribute == FieldValue.Attribute.REPEATED) {
        List<Object> list = new ArrayList<>();
        for (FieldValue value : fieldValue.getRepeatedValue()) {
          list.add(convertValue(field, value));
        }
        recordBuilder.set(fieldName, list);
      } else {
        Object value = convertValue(field, fieldValue);
        if (value instanceof ZonedDateTime) {
          recordBuilder.setTimestamp(fieldName, (ZonedDateTime) value);
        } else if (value instanceof LocalTime) {
          recordBuilder.setTime(fieldName, (LocalTime) value);
        } else if (value instanceof LocalDate) {
          recordBuilder.setDate(fieldName, (LocalDate) value);
        } else if (value instanceof LocalDateTime) {
          recordBuilder.setDateTime(fieldName, (LocalDateTime) value);
        } else if (value instanceof BigDecimal) {
          recordBuilder.setDecimal(fieldName, (BigDecimal) value);
        } else {
          recordBuilder.set(fieldName, value);
        }
      }
    }
    StructuredRecord record = recordBuilder.build();
    return record;
  }

  /**
   * Convert BigQuery field value to CDAP field value
   * @param field BigQuery field
   * @param fieldValue BigQUery field value
   * @return the converted CDAP field value
   */
  public static Object convertValue(Field field, FieldValue fieldValue) {
    LegacySQLTypeName type = field.getType();
    StandardSQLTypeName standardType = type.getStandardType();
    switch (standardType) {
      case TIME:
        return LocalTime.parse(fieldValue.getStringValue());
      case DATE:
        return LocalDate.parse(fieldValue.getStringValue());
      case TIMESTAMP:
        long tsMicroValue = fieldValue.getTimestampValue();
        return getZonedDateTime(tsMicroValue);
      case NUMERIC:
        BigDecimal decimal = fieldValue.getNumericValue();
        if (decimal.scale() < 9) {
          // scale up the big decimal. this is because structured record expects scale to be exactly same as schema
          // Big Query supports maximum unscaled value up to 38 digits. so scaling up should still be <= max
          // precision
          decimal = decimal.setScale(9);
        }
        return decimal;

      case DATETIME:
        return LocalDateTime.parse(fieldValue.getStringValue());
      case STRING:
        return fieldValue.getStringValue();
      case BOOL:
        return fieldValue.getBooleanValue();
      case FLOAT64:
        return fieldValue.getDoubleValue();
      case INT64:
        return fieldValue.getLongValue();
      case BYTES:
        return fieldValue.getBytesValue();
      default:
        throw new RuntimeException(String.format("BigQuery type %s is not supported.", standardType));
    }
  }

  /**
   * Convert epoch microseconds to zoned date time
   * @param microTs the epoch microseconds
   * @return the converted zoned datte time
   */
  public static ZonedDateTime getZonedDateTime(long microTs) {
    long tsInSeconds = TimeUnit.MICROSECONDS.toSeconds(microTs);
    long mod = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (microTs % mod);
    Instant instant = Instant.ofEpochSecond(tsInSeconds, TimeUnit.MICROSECONDS.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

}
