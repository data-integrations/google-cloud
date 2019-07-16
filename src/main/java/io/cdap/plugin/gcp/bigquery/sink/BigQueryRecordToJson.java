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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Util class to convert structured record into json.
 */
public final class BigQueryRecordToJson {
  private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

  /**
   * Writes object and writes to json writer.
   * @param writer json writer to write the object to
   * @param name name of the field to be written
   * @param object object to be written
   * @param fieldSchema field schema to be written
   */
  public static void write(JsonWriter writer, String name, Object object, Schema fieldSchema) throws IOException {
    write(writer, name, false, object, fieldSchema);
  }

  /**
   * Writes object and writes to json writer.
   * @param writer json writer to write the object to
   * @param name name of the field to be written
   * @param isArrayItem true if the method is writing array item. This means the name of the array field will not be
   *                    added to the json writer
   * @param object object to be written
   * @param fieldSchema field schema to be written
   */
  private static void write(JsonWriter writer, String name, boolean isArrayItem, Object object,
                            Schema fieldSchema) throws IOException {
    Schema schema = BigQueryUtil.getNonNullableSchema(fieldSchema);
    switch (schema.getType()) {
      case NULL:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
      case BYTES:
        writeSimpleTypes(writer, name, isArrayItem, object, schema);
        break;
      case ARRAY:
        writeArray(writer, name, object, schema);
        break;
      case RECORD:
        writeRecord(writer, name, object, schema);
        break;
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", name, fieldSchema.getType()));
    }
  }

  /**
   * Writes simple types to json writer.
   * @param writer json writer
   * @param name name of the field to be written
   * @param isArrayItem true if the method is writing array item. This means the name of the array field will not be
   *                    added to the json writer
   * @param object object to be written
   * @param schema field schema to be written
   */
  private static void writeSimpleTypes(JsonWriter writer, String name, boolean isArrayItem, Object object,
                                       Schema schema) throws IOException {
    if (!isArrayItem) {
      writer.name(name);
    }

    if (object == null) {
      writer.nullValue();
      return;
    }

    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          writer.value(Objects.requireNonNull(LocalDate.ofEpochDay(((Integer) object).longValue()).toString()));
          break;
        case TIME_MILLIS:
          writer.value(TIME_FORMATTER.format(
            Objects.requireNonNull(LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) object))))));
          break;
        case TIME_MICROS:
          writer.value(TIME_FORMATTER.format(
            Objects.requireNonNull(LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) object)))));
          break;
        case TIMESTAMP_MILLIS:
          //timestamp for json input should be in this format yyyy-MM-dd HH:mm:ss.SSSSSS
          writer.value(DATETIME_FORMATTER.format(
            Objects.requireNonNull(getZonedDateTime((long) object, TimeUnit.MILLISECONDS))));
          break;
        case TIMESTAMP_MICROS:
          writer.value(DATETIME_FORMATTER.format(
            Objects.requireNonNull(getZonedDateTime((long) object, TimeUnit.MICROSECONDS))));
          break;
        case DECIMAL:
          writer.value(Objects.requireNonNull(getDecimal(name, (byte[]) object, schema)).toPlainString());
          break;
        default:
          throw new IllegalStateException(
            String.format("Field '%s' is of unsupported type '%s'", name, logicalType.getToken()));
      }
      return;
    }

    switch (schema.getType()) {
      case NULL:
        writer.nullValue(); // nothing much to do here.
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        writer.value((Number) object);
        break;
      case BOOLEAN:
        writer.value((Boolean) object);
        break;
      case STRING:
        writer.value(object.toString());
        break;
      // TODO CDAP-15342 write byte type to json writer
      default:
        throw new IllegalStateException(String.format("Field '%s' is of unsupported type '%s'",
                                                      name, schema.getType()));
    }
  }

  private static void writeArray(JsonWriter writer,
                                 String name,
                                 @Nullable Object value,
                                 Schema fieldSchema) throws IOException {
    if (value == null) {
      throw new RuntimeException(
        String.format("Field '%s' is of value null, which is not a valid value for BigQuery type array.", name));
    }

    Collection collection;
    if (value instanceof Collection) {
      collection = (Collection) value;
    } else if (value instanceof Object[]) {
      collection = Arrays.asList((Object[]) value);
    } else {
      throw new IllegalArgumentException(String.format(
        "A value for the field '%s' is of type '%s' when it is expected to be a Collection or array.",
        name, value.getClass().getSimpleName()));
    }

    Schema componentSchema = BigQueryUtil.getNonNullableSchema(
      Objects.requireNonNull(fieldSchema.getComponentSchema()));
    if (BigQueryUtil.UNSUPPORTED_ARRAY_TYPES.contains(componentSchema.getType())) {
      throw new IllegalArgumentException(String.format("Field '%s' is an array of '%s', " +
                                                         "which is not a valid BigQuery type.",
                                                       name, componentSchema));
    }

    writer.name(name);
    writer.beginArray();

    for (Object element : collection) {
      // BigQuery does not allow null values in array items
      if (element == null) {
        throw new IllegalArgumentException(String.format("Field '%s' contains null values in its array, " +
                                                           "which is not allowed by BigQuery.", name));
      }
      if (element instanceof StructuredRecord) {
        StructuredRecord record = (StructuredRecord) element;
        processRecord(writer, record, Objects.requireNonNull(record.getSchema().getFields()));
      } else {
        write(writer, name, true, element, componentSchema);
      }
    }
    writer.endArray();
  }

  private static void writeRecord(JsonWriter writer,
                                  String name,
                                  @Nullable Object value,
                                  Schema fieldSchema) throws IOException {
    if (value == null) {
      writer.name(name);
      writer.nullValue();
      return;
    }

    if (!(value instanceof StructuredRecord)) {
      throw new IllegalStateException(
        String.format("Value is of type '%s', expected type is '%s'",
                      value.getClass().getSimpleName(), StructuredRecord.class.getSimpleName()));
    }

    writer.name(name);
    processRecord(writer, (StructuredRecord) value, Objects.requireNonNull(fieldSchema.getFields()));
  }

  private static void processRecord(JsonWriter writer,
                                    StructuredRecord record,
                                    List<Schema.Field> fields) throws IOException {
    writer.beginObject();
    for (Schema.Field field : fields) {
      write(writer, field.getName(), record.get(field.getName()), field.getSchema());
    }
    writer.endObject();
  }

  private static ZonedDateTime getZonedDateTime(long ts, TimeUnit unit) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  private static BigDecimal getDecimal(String name, byte[] value, Schema schema) {
    int scale = schema.getScale();
    BigDecimal decimal = new BigDecimal(new BigInteger(value), scale);
    if (decimal.precision() > 38 || decimal.scale() > 9) {
      throw new IllegalArgumentException(
        String.format("Numeric Field '%s' has invalid precision '%s' and scale '%s'. " +
                        "Precision must be at most 38 and scale must be at most 9.",
                      name, decimal.precision(), decimal.scale()));
    }
    return decimal;
  }

  private BigQueryRecordToJson() {
    //no-op
  }
}
