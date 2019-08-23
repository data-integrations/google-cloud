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
import com.google.common.base.Charsets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

public class ResultSetToRecordTransformerTest {
  private static final Schema TEST_SCHEMA = Schema.recordOf(
    "test",
    Schema.Field.of("bool_filed", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("int_field", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("float_field", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("date_field", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("timestamp_field", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("bool_array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))),
    Schema.Field.of("int_array", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("float_array", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("string_array", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("bytes_array", Schema.arrayOf(Schema.of(Schema.Type.BYTES)))
  );

  @Test
  public void testTransform() {
    ResultSet rsMock = Mockito.mock(ResultSet.class);

    Mockito.when(rsMock.getColumnType("bool_filed")).thenReturn(Type.bool());
    Mockito.when(rsMock.isNull("bool_filed")).thenReturn(false);
    Mockito.when(rsMock.getBoolean("bool_filed")).thenReturn(true);

    Mockito.when(rsMock.getColumnType("int_field")).thenReturn(Type.int64());
    Mockito.when(rsMock.isNull("int_field")).thenReturn(false);
    Mockito.when(rsMock.getLong("int_field")).thenReturn(1234L);

    Mockito.when(rsMock.getColumnType("float_field")).thenReturn(Type.float64());
    Mockito.when(rsMock.isNull("float_field")).thenReturn(false);
    Mockito.when(rsMock.getDouble("float_field")).thenReturn(1.1234);

    Mockito.when(rsMock.getColumnType("string_field")).thenReturn(Type.string());
    Mockito.when(rsMock.isNull("string_field")).thenReturn(false);
    Mockito.when(rsMock.getString("string_field")).thenReturn("test");

    Mockito.when(rsMock.getColumnType("bytes_field")).thenReturn(Type.bytes());
    Mockito.when(rsMock.isNull("bytes_field")).thenReturn(false);
    Mockito.when(rsMock.getBytes("bytes_field")).thenReturn(ByteArray.copyFrom("test"));

    LocalDate expectedDate = LocalDate.now();
    Mockito.when(rsMock.getColumnType("date_field")).thenReturn(Type.date());
    Mockito.when(rsMock.isNull("date_field")).thenReturn(false);
    Mockito.when(rsMock.getDate("date_field")).thenReturn(Date.fromYearMonthDay(
      expectedDate.getYear(),
      expectedDate.getMonthValue(),
      expectedDate.getDayOfMonth()
    ));

    Instant timestampInstant = Instant.now();
    ZonedDateTime expectedTime = ZonedDateTime.ofInstant(timestampInstant, ZoneId.of("UTC"));
    Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(
      timestampInstant.getEpochSecond(),
      timestampInstant.getNano()
    );
    Mockito.when(rsMock.getColumnType("timestamp_field")).thenReturn(Type.timestamp());
    Mockito.when(rsMock.isNull("timestamp_field")).thenReturn(false);
    Mockito.when(rsMock.getTimestamp("timestamp_field")).thenReturn(timestamp);

    List<Boolean> expectedBoolList = Arrays.asList(true, false, null);
    Mockito.when(rsMock.getColumnType("bool_array")).thenReturn(Type.array(Type.bool()));
    Mockito.when(rsMock.isNull("bool_array")).thenReturn(false);
    Mockito.when(rsMock.getBooleanList("bool_array")).thenReturn(expectedBoolList);

    List<Long> expectedIntList = Arrays.asList(1234L, 1234L, null);
    Mockito.when(rsMock.getColumnType("int_array")).thenReturn(Type.array(Type.int64()));
    Mockito.when(rsMock.isNull("int_array")).thenReturn(false);
    Mockito.when(rsMock.getLongList("int_array")).thenReturn(expectedIntList);

    List<Double> expectedFloatList = Arrays.asList(1.1234, 1.1234, null);
    Mockito.when(rsMock.getColumnType("float_array")).thenReturn(Type.array(Type.float64()));
    Mockito.when(rsMock.isNull("float_array")).thenReturn(false);
    Mockito.when(rsMock.getDoubleList("float_array")).thenReturn(expectedFloatList);

    List<String> expectedStringList = Arrays.asList("1", "2", null);
    Mockito.when(rsMock.getColumnType("string_array")).thenReturn(Type.array(Type.string()));
    Mockito.when(rsMock.isNull("string_array")).thenReturn(false);
    Mockito.when(rsMock.getStringList("string_array")).thenReturn(expectedStringList);

    Mockito.when(rsMock.getColumnType("bytes_array")).thenReturn(Type.array(Type.bytes()));
    Mockito.when(rsMock.isNull("bytes_array")).thenReturn(false);
    Mockito.when(rsMock.getBytesList("bytes_array")).thenReturn(Arrays.asList(
      ByteArray.copyFrom("1"),
      ByteArray.copyFrom("2"),
      null)
    );

    ResultSetToRecordTransformer transformer = new ResultSetToRecordTransformer(TEST_SCHEMA);
    StructuredRecord record = transformer.transform(rsMock);

    Assert.assertEquals(1234L, (long) record.get("int_field"));
    Assert.assertTrue(record.get("bool_filed"));
    Assert.assertEquals(1.1234, record.get("float_field"), 0.00001);
    Assert.assertEquals("test", record.get("string_field"));
    Assert.assertEquals("test", new String(record.get("bytes_field"), Charsets.UTF_8));
    Assert.assertEquals(expectedDate, record.getDate("date_field"));
    Assert.assertEquals(expectedTime, record.getTimestamp("timestamp_field"));
    Assert.assertArrayEquals(expectedBoolList.toArray(), record.get("bool_array"));
    Assert.assertArrayEquals(expectedIntList.toArray(), record.get("int_array"));
    Assert.assertArrayEquals(expectedFloatList.toArray(), record.get("float_array"));
    Assert.assertArrayEquals(expectedStringList.toArray(), record.get("string_array"));
    Assert.assertArrayEquals(new byte[][]{"1".getBytes(), "2".getBytes(), null}, record.get("bytes_array"));
  }
}
