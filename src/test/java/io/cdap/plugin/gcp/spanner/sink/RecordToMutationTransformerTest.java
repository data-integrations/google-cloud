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
import com.google.cloud.spanner.Value;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class RecordToMutationTransformerTest {

  @Test
  public void testConvertToValueAllTypes() {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("INT_COL", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("STRING_COL", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("BOOL_COL", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("BYTES_COL", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("BYTE_BUFFER_COL", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("FLOAT_COL", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("DOUBLE_COL", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DATE_COL", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("TIMESTAMP_MILLIS_COL", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
      Schema.Field.of("TIMESTAMP_MICROS_COL", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("ARRAY_INT_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT)))),
      Schema.Field.of("ARRAY_LONG_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
      Schema.Field.of("ARRAY_STRING_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("ARRAY_BOOL_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))),
      Schema.Field.of("ARRAY_FLOAT_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.FLOAT)))),
      Schema.Field.of("ARRAY_DOUBLE_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))),
      Schema.Field.of("ARRAY_BYTES_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BYTES)))),
      Schema.Field.of("ARRAY_BYTE_BUFFER_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BYTES)))),
      Schema.Field.of("ARRAY_DATE_COL", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)))),
      Schema.Field.of("ARRAY_TIMESTAMP_MILLIS_COL",
                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)))),
      Schema.Field.of("ARRAY_TIMESTAMP_MICROS_COL",
                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))))
    );

    RecordToMutationTransformer transformer = new RecordToMutationTransformer("test", schema);
    
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("ID", 1L)
      .set("INT_COL", 1)
      .set("STRING_COL", "string")
      .set("BOOL_COL", false)
      .set("BYTES_COL", "bytes".getBytes())
      .set("BYTE_BUFFER_COL", ByteBuffer.wrap("bytes".getBytes()))
      .set("FLOAT_COL", 2.2f)
      .set("DOUBLE_COL", 2.2d)
      .setDate("DATE_COL", LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()))
      .setTimestamp("TIMESTAMP_MILLIS_COL", now)
      .setTimestamp("TIMESTAMP_MICROS_COL", now)
      .set("ARRAY_INT_COL", Arrays.asList(1, 2, null))
      .set("ARRAY_LONG_COL", Arrays.asList(1L, 2L, null))
      .set("ARRAY_STRING_COL", Arrays.asList("val1", "val2", null))
      .set("ARRAY_BOOL_COL", Arrays.asList(true, false, null))
      .set("ARRAY_FLOAT_COL", Arrays.asList(1.1F, 2.2F, null))
      .set("ARRAY_DOUBLE_COL", Arrays.asList(1.1D, 2.2D, null))
      .set("ARRAY_BYTES_COL", Arrays.asList("val1".getBytes(), "val2".getBytes(), null))
      .set("ARRAY_BYTE_BUFFER_COL",
           Arrays.asList(ByteBuffer.wrap("val1".getBytes()), ByteBuffer.wrap("val2".getBytes()), null))
      .set("ARRAY_DATE_COL",
           Arrays.asList(
             Math.toIntExact(LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()).toEpochDay()),
             Math.toIntExact(LocalDate.of(now.getYear() + 1, now.getMonthValue(), now.getDayOfMonth()).toEpochDay()),
             null
           )
      )
      .set("ARRAY_TIMESTAMP_MILLIS_COL", Arrays.asList(
        convertZonedDateTimeToLong("ARRAY_TIMESTAMP_MILLIS_COL", now, Schema.LogicalType.TIMESTAMP_MILLIS),
        convertZonedDateTimeToLong("ARRAY_TIMESTAMP_MILLIS_COL", now.plusDays(1), Schema.LogicalType.TIMESTAMP_MILLIS),
        null
      ))
      .set("ARRAY_TIMESTAMP_MICROS_COL", Arrays.asList(
        convertZonedDateTimeToLong("ARRAY_TIMESTAMP_MICROS_COL", now, Schema.LogicalType.TIMESTAMP_MICROS),
        convertZonedDateTimeToLong("ARRAY_TIMESTAMP_MICROS_COL", now.plusDays(1), Schema.LogicalType.TIMESTAMP_MICROS),
        null
      ))
      .build();

    Assert.assertEquals(Value.int64(1L),
                        transformer.convertToValue("ID",
                                                   schema.getField("ID").getSchema(), record));

    Assert.assertEquals(Value.int64(1),
                        transformer.convertToValue("INT_COL",
                                                   schema.getField("INT_COL").getSchema(), record));

    Assert.assertEquals(Value.string("string"),
                        transformer.convertToValue("STRING_COL",
                                                   schema.getField("STRING_COL").getSchema(), record));
    Assert.assertEquals(Value.bool(false),
                        transformer.convertToValue("BOOL_COL",
                                                   schema.getField("BOOL_COL").getSchema(), record));

    Assert.assertEquals(Value.bytes(ByteArray.copyFrom("bytes".getBytes())),
                        transformer.convertToValue("BYTES_COL",
                                                   schema.getField("BYTES_COL").getSchema(), record));

    Assert.assertEquals(Value.bytes(ByteArray.copyFrom("bytes".getBytes())),
                        transformer.convertToValue("BYTE_BUFFER_COL",
                                                   schema.getField("BYTE_BUFFER_COL").getSchema(), record));

    Assert.assertEquals(Value.float64(new Float(2.2).doubleValue()),
                        transformer.convertToValue("FLOAT_COL",
                                                   schema.getField("FLOAT_COL").getSchema(), record));

    Assert.assertEquals(Value.float64(2.2D),
                        transformer.convertToValue("DOUBLE_COL",
                                                   schema.getField("DOUBLE_COL").getSchema(), record));

    Assert.assertEquals(Value.date(Date.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth())),
                        transformer.convertToValue("DATE_COL",
                                                   schema.getField("DATE_COL").getSchema(), record));

    Assert.assertEquals(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(now.toEpochSecond(), now.getNano())),
                        transformer.convertToValue("TIMESTAMP_MILLIS_COL",
                                                   schema.getField("TIMESTAMP_MILLIS_COL").getSchema(), record));

    Assert.assertEquals(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(now.toEpochSecond(), now.getNano())),
                        transformer.convertToValue("TIMESTAMP_MICROS_COL",
                                                   schema.getField("TIMESTAMP_MICROS_COL").getSchema(), record));

    Assert.assertEquals(Value.int64Array(Arrays.asList(1L, 2L, null)),
                        transformer.convertToValue("ARRAY_INT_COL",
                                                   schema.getField("ARRAY_INT_COL").getSchema(), record));

    Assert.assertEquals(Value.int64Array(Arrays.asList(1L, 2L, null)),
                        transformer.convertToValue("ARRAY_LONG_COL",
                                                   schema.getField("ARRAY_LONG_COL").getSchema(), record));

    Assert.assertEquals(Value.stringArray(Arrays.asList("val1", "val2", null)),
                        transformer.convertToValue("ARRAY_STRING_COL",
                                                   schema.getField("ARRAY_STRING_COL").getSchema(), record));

    Assert.assertEquals(Value.boolArray(Arrays.asList(true, false, null)),
                        transformer.convertToValue("ARRAY_BOOL_COL",
                                                   schema.getField("ARRAY_BOOL_COL").getSchema(), record));

    Assert.assertEquals(Value.float64Array(Arrays.asList(new Float(1.1).doubleValue(),
                                                         new Float(2.2).doubleValue(),
                                                         null)),
                        transformer.convertToValue("ARRAY_FLOAT_COL",
                                                   schema.getField("ARRAY_FLOAT_COL").getSchema(), record));

    Assert.assertEquals(Value.bytesArray(Arrays.asList(ByteArray.copyFrom("val1".getBytes()),
                                                       ByteArray.copyFrom("val2".getBytes()),
                                                       null)),
                        transformer.convertToValue("ARRAY_BYTES_COL",
                                                   schema.getField("ARRAY_BYTES_COL").getSchema(), record));

    Assert.assertEquals(Value.bytesArray(Arrays.asList(ByteArray.copyFrom("val1".getBytes()),
                                                       ByteArray.copyFrom("val2".getBytes()),
                                                       null)),
                        transformer.convertToValue("ARRAY_BYTE_BUFFER_COL",
                                                   schema.getField("ARRAY_BYTE_BUFFER_COL").getSchema(), record));

    Assert.assertEquals(Value.dateArray(Arrays.asList(
      Date.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth()),
      Date.fromYearMonthDay(now.getYear() + 1, now.getMonthValue(), now.getDayOfMonth()),
      null)),
                        transformer.convertToValue("ARRAY_DATE_COL",
                                                   schema.getField("ARRAY_DATE_COL").getSchema(), record));

    Assert.assertEquals(Value.timestampArray(Arrays.asList(
      Timestamp.ofTimeSecondsAndNanos(now.toEpochSecond(), now.getNano()),
      Timestamp.ofTimeSecondsAndNanos(now.plusDays(1).toEpochSecond(), now.plusDays(1).getNano()),
      null)),
                        transformer.convertToValue("ARRAY_TIMESTAMP_MILLIS_COL",
                                                   schema.getField("ARRAY_TIMESTAMP_MILLIS_COL").getSchema(),
                                                   record));

    Assert.assertEquals(Value.timestampArray(Arrays.asList(
      Timestamp.ofTimeSecondsAndNanos(now.toEpochSecond(), now.getNano()),
      Timestamp.ofTimeSecondsAndNanos(now.plusDays(1).toEpochSecond(), now.plusDays(1).getNano()),
      null)),
                        transformer.convertToValue("ARRAY_TIMESTAMP_MICROS_COL",
                                                              schema.getField("ARRAY_TIMESTAMP_MICROS_COL").getSchema(),
                                                              record));
  }

  @Test
  public void testToCollectionFromArrayOfPrimitives() {
    int[] array = {1, 2, 3};
    testToCollection(Arrays.asList(1, 2, 3), array);
  }

  @Test
  public void testToCollectionFromArrayOfObjects() {
    Integer[] array = new Integer[]{1, 2, 3};
    testToCollection(Arrays.asList(1, 2, 3), array);
  }

  @Test
  public void testToCollectionFromListObjects() {
    Collection<Integer> collection = Arrays.asList(1, 2, 3);
    testToCollection(collection, collection);
  }

  public void testToCollection(Collection expected, Object object) {
    Collection<Object> collection = RecordToMutationTransformer
      .toCollection("test", "int", object);
    Assert.assertEquals(expected, collection);
  }

  private static Long convertZonedDateTimeToLong(String fieldName, ZonedDateTime zonedDateTime,
                                                 Schema.LogicalType logicalType) {
    if (zonedDateTime == null) {
      return null;
    }

    Instant instant = zonedDateTime.toInstant();
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
