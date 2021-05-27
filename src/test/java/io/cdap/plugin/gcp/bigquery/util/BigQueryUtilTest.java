/*
 *
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
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class BigQueryUtilTest {
  @Test
  public void testGetTableSchema() {
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

    Schema tableSchema = BigQueryUtil.getTableSchema(com.google.cloud.bigquery.Schema.of(fieldList), null);

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
    assertEquals(expectedSchema, tableSchema);
  }
}
