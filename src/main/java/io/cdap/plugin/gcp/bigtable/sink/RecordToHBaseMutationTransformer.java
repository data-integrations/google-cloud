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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Transforms Google Cloud Bigtable {@link Result} to {@link StructuredRecord}.
 */
public class RecordToHBaseMutationTransformer {

  private final String keyAlias;
  private final Map<String, HBaseColumn> columnMappings;

  public RecordToHBaseMutationTransformer(String keyAlias, Map<String, HBaseColumn> columnMappings) {
    this.keyAlias = keyAlias;
    this.columnMappings = new HashMap<>(columnMappings);
  }

  public Mutation transform(StructuredRecord record) {
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(), "Schema fields cannot be empty");
    Schema.Field keyField = record.getSchema().getField(keyAlias);
    if (keyField == null) {
      throw new UnexpectedFormatException(String.format("Record does not contain key field '%s'. Keys are required",
                                                        keyAlias));
    }
    byte[] rowKeyBytes = convertFieldValueToBytes(record.get(keyAlias), keyField);
    if (rowKeyBytes == null) {
      throw new UnexpectedFormatException(String.format("Key field '%s' contained a null value. Keys cannot be null",
                                                        keyAlias));
    }
    Put put = new Put(rowKeyBytes);
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      if (fieldName.equals(keyAlias)) {
        continue;
      }
      HBaseColumn column = columnMappings.get(fieldName);
      byte[] valueBytes = convertFieldValueToBytes(record.get(fieldName), field);
      put.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()), valueBytes);
    }
    return put;
  }

  @Nullable
  private static byte[] convertFieldValueToBytes(@Nullable Object value, Schema.Field field) {
    try {
      return convertFieldValueToBytes(value, field.getSchema());
    } catch (Exception e) {
      throw new UnexpectedFormatException(String.format("Failed to transform value for field '%s'. Reason: %s",
                                                        field.getName(), e.getMessage()));
    }
  }

  @Nullable
  private static byte[] convertFieldValueToBytes(@Nullable Object value, Schema fieldSchema) {
    if (fieldSchema.isNullable()) {
      if (value == null) {
        return null;
      }
      return convertFieldValueToBytes(value, fieldSchema.getNonNullable());
    }
    switch (fieldSchema.getType()) {
      case STRING:
        return Bytes.toBytes((String) value);
      case BYTES:
        return (byte[]) value;
      case INT:
        return Bytes.toBytes((int) value);
      case LONG:
        return Bytes.toBytes((long) value);
      case FLOAT:
        return Bytes.toBytes((float) value);
      case DOUBLE:
        return Bytes.toBytes((double) value);
      case BOOLEAN:
        return Bytes.toBytes((boolean) value);
      default:
        throw new UnexpectedFormatException("Field type '" + fieldSchema.getDisplayName() + "' is not supported");
    }
  }
}
