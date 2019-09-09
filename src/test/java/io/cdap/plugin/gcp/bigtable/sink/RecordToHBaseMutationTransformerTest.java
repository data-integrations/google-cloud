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

package io.cdap.plugin.gcp.bigtable.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class RecordToHBaseMutationTransformerTest {
  private static final String TEST_FAMILY_STRING = "test-family";
  private static final byte[] TEST_FAMILY = Bytes.toBytes(TEST_FAMILY_STRING);

  @Test
  public void testTransformAllTypes() {
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("string_column", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes_column", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("int_column", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_column", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_column", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_column", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("boolean_column", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", 1)
      .set("string_column", "string_value")
      .set("bytes_column", "test_blob".getBytes())
      .set("int_column", 15)
      .set("long_column", 10L)
      .set("float_column", 15.5F)
      .set("double_column", 10.5D)
      .set("boolean_column", true)
      .build();

    Map<String, HBaseColumn> columnMappings = ImmutableMap.<String, HBaseColumn>builder()
      .put("id", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "id"))
      .put("boolean_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "boolean_column"))
      .put("int_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "int_column"))
      .put("long_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "long_column"))
      .put("float_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "float_column"))
      .put("double_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "double_column"))
      .put("bytes_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "bytes_column"))
      .put("string_column", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "string_column"))
      .build();

    RecordToHBaseMutationTransformer transformer =
      new RecordToHBaseMutationTransformer("id", columnMappings);

    Put put = (Put) transformer.transform(inputRecord);

    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("string_column"), Bytes.toBytes("string_value")));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("bytes_column"), "test_blob".getBytes()));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("int_column"), Bytes.toBytes(15)));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("long_column"), Bytes.toBytes(10L)));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("float_column"), Bytes.toBytes(15.5F)));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("double_column"), Bytes.toBytes(10.5D)));
    Assert.assertTrue(put.has(TEST_FAMILY, Bytes.toBytes("boolean_column"), Bytes.toBytes(true)));
  }

  @Test
  public void testTransformInvalidFieldValue() {
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("null_field", Schema.of(Schema.Type.NULL))
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("null_field", null)
      .build();

    Map<String, HBaseColumn> columnMappings = ImmutableMap.<String, HBaseColumn>builder()
      .put("null_field", HBaseColumn.fromFamilyAndQualifier(TEST_FAMILY_STRING, "null_field"))
      .build();

    RecordToHBaseMutationTransformer transformer =
      new RecordToHBaseMutationTransformer("null_field", columnMappings);

    try {
      transformer.transform(inputRecord);
      Assert.fail(String.format("Expected to throw %s", UnexpectedFormatException.class.getName()));
    } catch (UnexpectedFormatException e) {
      String errorMessage = "Expected fail while attempting to transform value of type 'null'";
      Assert.assertEquals(errorMessage, e.getMessage(), "Failed to transform value for field 'null_field'. " +
        "Reason: Field type 'null' is not supported");
    }
  }
}
