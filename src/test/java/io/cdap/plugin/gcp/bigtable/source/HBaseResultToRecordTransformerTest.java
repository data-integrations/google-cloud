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

package io.cdap.plugin.gcp.bigtable.source;

import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class HBaseResultToRecordTransformerTest {
  private static final byte[] TEST_FAMILY = Bytes.toBytes("test");
  private static final byte[] TEST_ROW = Bytes.toBytes("r1");
  private static final int TEST_TIMESTAMP = (int) Instant.now().getEpochSecond();

  @Test
  public void testTransformAllTypes() {
    List<Cell> cellList = ImmutableList.of(
      createCell("boolean_column", Bytes.toBytes(true)),
      createCell("bytes_column", Bytes.toBytes("bytes")),
      createCell("double_column", Bytes.toBytes(10.5D)),
      createCell("float_column", Bytes.toBytes(10.5F)),
      createCell("int_column", Bytes.toBytes(1)),
      createCell("long_column", Bytes.toBytes(10L)),
      createCell("string_column", Bytes.toBytes("string"))
    );
    Result result = Result.create(cellList);

    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("boolean_column", Schema.of(Schema.Type.BOOLEAN)),
                      Schema.Field.of("int_column", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("long_column", Schema.of(Schema.Type.LONG)),
                      Schema.Field.of("float_column", Schema.of(Schema.Type.FLOAT)),
                      Schema.Field.of("double_column", Schema.of(Schema.Type.DOUBLE)),
                      Schema.Field.of("bytes_column", Schema.of(Schema.Type.BYTES)),
                      Schema.Field.of("string_column", Schema.of(Schema.Type.STRING))
      );

    Map<String, String> columnMappings = ImmutableMap.<String, String>builder()
      .put("test:boolean_column", "boolean_column")
      .put("test:int_column", "int_column")
      .put("test:long_column", "long_column")
      .put("test:float_column", "float_column")
      .put("test:double_column", "double_column")
      .put("test:bytes_column", "bytes_column")
      .put("test:string_column", "string_column")
      .build();

    HBaseResultToRecordTransformer transformer = new HBaseResultToRecordTransformer(schema, null, columnMappings);
    StructuredRecord record = transformer.transform(result);

    Assert.assertTrue(record.get("boolean_column"));
    Assert.assertEquals(1, (int) record.get("int_column"));
    Assert.assertEquals(10L, (long) record.get("long_column"));
    Assert.assertEquals(10.5F, record.get("float_column"), 0);
    Assert.assertEquals(10.5D, record.get("double_column"), 0);
    Assert.assertEquals("bytes", new String((byte[]) record.get("bytes_column")));
    Assert.assertEquals("string", record.get("string_column"));
  }

  @Test
  public void testTransformInvalidFieldType() {
    List<Cell> cellList = ImmutableList.of(
      createCell("null_column", Bytes.toBytes(true))
    );
    Result result = Result.create(cellList);

    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("null_column", Schema.of(Schema.Type.NULL))
      );

    Map<String, String> columnMappings = ImmutableMap.<String, String>builder()
      .put("test:null_column", "null_column")
      .build();

    HBaseResultToRecordTransformer transformer = new HBaseResultToRecordTransformer(schema, null, columnMappings);
    try {
      transformer.transform(result);
      Assert.fail(String.format("Expected to throw %s", UnexpectedFormatException.class.getName()));
    } catch (UnexpectedFormatException e) {
      String errorMessage = "Expected fail while attempting to transform field 'null_column'";
      Assert.assertEquals(errorMessage, "Failed to transform field 'null_column'. Reason: Field type 'null' " +
        "is not supported", e.getMessage());
    }
  }

  private static Cell createCell(String column, byte[] value) {
    return new RowCell(TEST_ROW, TEST_FAMILY, Bytes.toBytes(column), TEST_TIMESTAMP, value);
  }
}
