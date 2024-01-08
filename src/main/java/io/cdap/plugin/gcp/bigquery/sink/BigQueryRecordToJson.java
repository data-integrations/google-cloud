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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
  public static void write(JsonWriter writer, String name, Object object, Schema fieldSchema,
                           Set<String> jsonStringFieldsPaths) throws IOException {
    List<String> path = new ArrayList<>();
    path.add(name);
    write(writer, name, false, object, fieldSchema, path, jsonStringFieldsPaths);
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
                            Schema fieldSchema, List<String> path,
                            Set<String> jsonStringFieldsPaths) throws IOException {
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
        writeSimpleTypes(writer, name, isArrayItem, object, schema, path, jsonStringFieldsPaths);
        break;
      case ARRAY:
        writeArray(writer, name, object, schema, path, jsonStringFieldsPaths);
        break;
      case RECORD:
        writeRecord(writer, name, object, schema, path, jsonStringFieldsPaths);
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
                                       Schema schema, List<String> path,
                                       Set<String> jsonStringFieldsPaths) throws IOException {
    if (!isArrayItem) {
      writer.name(name);
    }

    if (object == null) {
      writer.nullValue();
      return;
    }

    String pathString = String.join(".", path);

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
        case DATETIME:
          //datetime should be already an ISO-8601 string
          writer.value(Objects.requireNonNull(object.toString()));
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
        if (jsonStringFieldsPaths.contains(pathString)) {
          Gson gson = new Gson();
          String jsonString = object.toString();
          if (jsonString.startsWith("{") && jsonString.endsWith("}")) {
            writeJsonObjectToWriter(gson.fromJson(jsonString, JsonObject.class), writer);
          } else if (jsonString.startsWith("[") && jsonString.endsWith("]")) {
            writeJsonArrayToWriter(gson.fromJson(jsonString, JsonArray.class), writer);
          } else {
            throw new IllegalStateException(String.format("Expected value of Field '%s' to be a valid JSON " +
                    "object or array.", name));
          }
          break;
        }
        writer.value(object.toString());
        break;
      case BYTES:
        if (object instanceof byte[]) {
          writer.value(Base64.getEncoder().encodeToString((byte[]) object));
        } else if (object instanceof ByteBuffer) {
          writer.value(Base64.getEncoder().encodeToString(Bytes.toBytes((ByteBuffer) object)));
        } else {
          throw new IllegalStateException(String.format("Expected value of Field '%s' to be bytes but got '%s'",
                                                        name, object.getClass().getSimpleName()));
        }
        break;
      default:
        throw new IllegalStateException(String.format("Field '%s' is of unsupported type '%s'",
                                                      name, schema.getType()));
    }
  }

  private static void writeArray(JsonWriter writer,
                                 String name,
                                 @Nullable Object value,
                                 Schema fieldSchema,
                                 List<String> path, Set<String> jsonStringFieldsPaths) throws IOException {
    writer.name(name);
    writer.beginArray();

    // If it's a null array, handle it as an empty array
    if (value != null) {
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


      for (Object element : collection) {
        // BigQuery does not allow null values in array items
        if (element == null) {
          throw new IllegalArgumentException(String.format("Field '%s' contains null values in its array, " +
                  "which is not allowed by BigQuery.", name));
        }
        if (element instanceof StructuredRecord) {
          StructuredRecord record = (StructuredRecord) element;
          processRecord(writer, record, Objects.requireNonNull(record.getSchema().getFields()),
                  path, jsonStringFieldsPaths);
        } else {
          write(writer, name, true, element, componentSchema, path, jsonStringFieldsPaths);
        }
      }
    }
    writer.endArray();
  }

  private static void writeRecord(JsonWriter writer,
                                  String name,
                                  @Nullable Object value,
                                  Schema fieldSchema,
                                  List<String> path, Set<String> jsonStringFieldsPaths) throws IOException {
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
    processRecord(writer, (StructuredRecord) value, Objects.requireNonNull(fieldSchema.getFields()), path
    , jsonStringFieldsPaths);
  }

  private static void processRecord(JsonWriter writer,
                                    StructuredRecord record,
                                    List<Schema.Field> fields,
                                    List<String> path, Set<String> jsonStringFieldsPaths) throws IOException {
    writer.beginObject();
    for (Schema.Field field : fields) {
      path.add(field.getName());
      write(writer, field.getName(), false, record.get(field.getName()), field.getSchema(), path,
              jsonStringFieldsPaths);
      path.remove(path.size() - 1);
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
    // Checks from https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
    BigDecimal decimal = new BigDecimal(new BigInteger(value), scale);
    if (decimal.precision() > BigQueryTypeSize.BigNumeric.PRECISION ||
      decimal.scale() > BigQueryTypeSize.BigNumeric.SCALE) {
      throw new IllegalArgumentException(
        String.format("Numeric Field '%s' has invalid precision '%s' and scale '%s'. " +
                        "Precision must be at most '%s' and scale must be at most '%s'.",
                      name, decimal.precision(), decimal.scale(),
                      BigQueryTypeSize.BigNumeric.PRECISION, BigQueryTypeSize.BigNumeric.SCALE));
    }
    return decimal;
  }

  private BigQueryRecordToJson() {
    //no-op
  }

  private static void writeJsonObjectToWriter(JsonObject jsonObject, JsonWriter jsonWriter) throws IOException {
    jsonWriter.beginObject();
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = entry.getValue();

      jsonWriter.name(key);
      writeJsonElementToWriter(value, jsonWriter); // Recursively write the value
    }
    jsonWriter.endObject();
  }

  private static void writeJsonElementToWriter(JsonElement jsonElement, JsonWriter jsonWriter) throws IOException {
    if (jsonElement.isJsonObject()) {
      writeJsonObjectToWriter(jsonElement.getAsJsonObject(), jsonWriter);
    } else if (jsonElement.isJsonArray()) {
      writeJsonArrayToWriter(jsonElement.getAsJsonArray(), jsonWriter);
    } else if (jsonElement.isJsonPrimitive()) {
      writeJsonPrimitiveToWriter(jsonElement.getAsJsonPrimitive(), jsonWriter);
    } else if (jsonElement.isJsonNull()) {
      jsonWriter.nullValue();
    }
  }

  private static void writeJsonArrayToWriter(JsonArray jsonArray, JsonWriter jsonWriter) throws IOException {
    jsonWriter.beginArray();
    for (JsonElement element : jsonArray) {
      writeJsonElementToWriter(element, jsonWriter); // Recursively write array elements
    }
    jsonWriter.endArray();
  }

  private static void writeJsonPrimitiveToWriter(JsonPrimitive jsonPrimitive, JsonWriter jsonWriter)
          throws IOException {
    if (jsonPrimitive.isNumber()) {
      jsonWriter.value(jsonPrimitive.getAsNumber());
    } else if (jsonPrimitive.isBoolean()) {
      jsonWriter.value(jsonPrimitive.getAsBoolean());
    } else if (jsonPrimitive.isString()) {
      jsonWriter.value(jsonPrimitive.getAsString());
    }
  }
}
