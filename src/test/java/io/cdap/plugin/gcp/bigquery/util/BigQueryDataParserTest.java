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
import com.google.common.io.BaseEncoding;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import joptsimple.internal.Strings;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


@RunWith(PowerMockRunner.class)
public class BigQueryDataParserTest {

  @Test
  public void testParse() {
    TableResult tableResult = PowerMockito.mock(TableResult.class);

    List<Field> fieldList = new ArrayList<>();
    Field boolField = Field.newBuilder("bool", StandardSQLTypeName.BOOL).build();
    fieldList.add(boolField);
    Field bytesField = Field.newBuilder("bytes", StandardSQLTypeName.BYTES).build();
    fieldList.add(bytesField);
    Field dateField = Field.newBuilder("date", StandardSQLTypeName.DATE).build();
    fieldList.add(dateField);
    Field datetimeField = Field.newBuilder("datetime", StandardSQLTypeName.DATETIME).build();
    fieldList.add(datetimeField);
    Field numericField = Field.newBuilder("numeric", StandardSQLTypeName.NUMERIC).build();
    fieldList.add(numericField);
    Field float64Field = Field.newBuilder("float64", StandardSQLTypeName.FLOAT64).build();
    fieldList.add(float64Field);
    Field int64Field = Field.newBuilder("int64", StandardSQLTypeName.INT64).build();
    fieldList.add(int64Field);
    Field stringField = Field.newBuilder("string", StandardSQLTypeName.STRING).build();
    fieldList.add(stringField);
    Field timeField = Field.newBuilder("time", StandardSQLTypeName.TIME).build();
    fieldList.add(timeField);
    Field timestampField = Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).build();
    fieldList.add(timestampField);


    com.google.cloud.bigquery.Schema bqSchema = com.google.cloud.bigquery.Schema.of(fieldList);
    PowerMockito.when(tableResult.getSchema()).thenReturn(bqSchema);
    List<FieldValue> valueList = new ArrayList<>();
    FieldValue boolValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true");
    valueList.add(boolValue);
    FieldValue bytesValue =
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, BaseEncoding.base64().encode("bytes".getBytes()));
    valueList.add(bytesValue);
    FieldValue dateValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2021-05-26");
    valueList.add(dateValue);
    FieldValue datetimeValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2021-05-26T10:15:30");
    valueList.add(datetimeValue);
    FieldValue numericValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2021052610.15301");
    valueList.add(numericValue);
    FieldValue float64Value = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "12.12");
    valueList.add(float64Value);
    FieldValue int64Value = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1212");
    valueList.add(int64Value);
    FieldValue stringValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string");
    valueList.add(stringValue);
    FieldValue timeValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10:15:30");
    valueList.add(timeValue);
    ZonedDateTime timestamp =
      ZonedDateTime.of(LocalDate.parse("2021-05-26"), LocalTime.parse("10:15:30"), ZoneId.of("UTC"));
    FieldValue timestampValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE,
      String.valueOf(timestamp.toEpochSecond()) + "." + paddHeaddingZero(String.valueOf(timestamp.getNano()), 9));
    valueList.add(timestampValue);


    List<FieldValueList> rows = Arrays.asList(FieldValueList.of(valueList, FieldList.of(fieldList)));

    PowerMockito.when(tableResult.iterateAll()).thenReturn(rows);
    List<StructuredRecord> result = BigQueryDataParser.parse(tableResult);

    List<Schema.Field> cdapFieldList = new ArrayList<>();
    Schema.Field cdapBoolField = Schema.Field.of("bool", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
    cdapFieldList.add(cdapBoolField);
    Schema.Field cdapBytesField = Schema.Field.of("bytes", Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    cdapFieldList.add(cdapBytesField);
    Schema.Field cdapDateField = Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)));
    cdapFieldList.add(cdapDateField);
    Schema.Field cdapDatetimeField =
      Schema.Field.of("datetime", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME)));
    cdapFieldList.add(cdapDatetimeField);
    Schema.Field cdapNumericField = Schema.Field.of("numeric", Schema.nullableOf(Schema.decimalOf(38, 9)));
    cdapFieldList.add(cdapNumericField);
    Schema.Field cdapFloat64Field = Schema.Field.of("float64", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    cdapFieldList.add(cdapFloat64Field);
    Schema.Field cdapInt64Field = Schema.Field.of("int64", Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    cdapFieldList.add(cdapInt64Field);
    Schema.Field cdapStringField = Schema.Field.of("string", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    cdapFieldList.add(cdapStringField);
    Schema.Field cdapTimeField = Schema.Field.of("time", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS)));
    cdapFieldList.add(cdapTimeField);
    Schema.Field cdapTimestampField =
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    cdapFieldList.add(cdapTimestampField);

    Schema expectedSchema = Schema.recordOf("output", cdapFieldList);


    Assert.assertEquals(rows.size(), result.size());
    StructuredRecord record = result.get(0);
    Schema cdapSchema = record.getSchema();
    Assert.assertEquals(expectedSchema, cdapSchema);
    Assert.assertTrue(record.get("bool"));
    Assert.assertArrayEquals("bytes".getBytes(), record.get("bytes"));
    Assert.assertEquals(Integer.valueOf((int) LocalDate.parse("2021-05-26").toEpochDay()), record.get("date"));
    Assert.assertArrayEquals(new BigDecimal("2021052610.15301").setScale(9).unscaledValue().toByteArray(),
      record.get("numeric"));
    Assert.assertEquals(Double.valueOf(12.12), record.get("float64"));
    Assert.assertEquals(Long.valueOf(1212), record.get("int64"));
    Assert.assertEquals("string", record.get("string"));
    Assert.assertEquals(Long.valueOf(TimeUnit.NANOSECONDS.toMicros(LocalTime.parse("10:15:30").toNanoOfDay())),
      record.get("time"));
    Assert.assertEquals(
      Long.valueOf(timestamp.toEpochSecond() * 1000000 + TimeUnit.NANOSECONDS.toMicros(timestamp.getNano())),
      record.get("timestamp"));
  }

  private String paddHeaddingZero(String value, int length) {
    if (value.length() >= length) {
      return value;
    }
    return Strings.repeat('0', length - value.length()) + value;
  }

  @Test
  public void testJsonFieldConversionToString() {
    Field field = Field.newBuilder("demo", LegacySQLTypeName.valueOf("JSON")).build();
    String jsonValue = "{\"key\":\"value\"}";
    FieldValue fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, jsonValue);
    Object result = BigQueryDataParser.convertValue(field, fieldValue);
    Assert.assertEquals(jsonValue, result);
  }
}
