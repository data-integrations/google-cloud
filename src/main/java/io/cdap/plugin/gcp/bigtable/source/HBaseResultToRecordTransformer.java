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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms Google Cloud Bigtable {@link Result} to {@link StructuredRecord}.
 */
public class HBaseResultToRecordTransformer {

  private final Schema schema;
  private final String keyAlias;
  private final Map<String, String> columnMappings;

  public HBaseResultToRecordTransformer(Schema schema, String keyAlias, Map<String, String> columnMappings) {
    this.schema = schema;
    this.keyAlias = keyAlias;
    this.columnMappings = new HashMap<>(columnMappings);
  }

  public StructuredRecord transform(Result result) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    if (keyAlias != null) {
      Object value = convertBytesToFieldValue(result.getRow(), keyAlias);
      recordBuilder.set(keyAlias, value);
    }
    for (Cell cell : result.rawCells()) {
      String columnName = Bytes.toString(cell.getFamilyArray()) + ':' + Bytes.toString(cell.getQualifierArray());
      String fieldName = columnMappings.get(columnName);
      if (fieldName != null) {
        Object value = convertBytesToFieldValue(cell.getValueArray(), fieldName);
        recordBuilder.set(fieldName, value);
      }
    }
    return recordBuilder.build();
  }

  private Object convertBytesToFieldValue(byte[] valueArray, String fieldName) {
    try {
      Schema.Field field = schema.getField(fieldName);
      return convertBytesToFieldValue(valueArray, field.getSchema());
    } catch (Exception e) {
      throw new UnexpectedFormatException(String.format("Failed to transform field '%s'. Reason: %s",
                                                        fieldName, e.getMessage()));
    }
  }

  private static Object convertBytesToFieldValue(byte[] bytes, Schema fieldSchema) {
    if (fieldSchema.isNullable()) {
      if (bytes == null) {
        return null;
      }
      return convertBytesToFieldValue(bytes, fieldSchema.getNonNullable());
    }
    switch (fieldSchema.getType()) {
      case STRING:
        return Bytes.toString(bytes);
      case BYTES:
        return bytes;
      case INT:
        return Bytes.toInt(bytes);
      case LONG:
        return Bytes.toLong(bytes);
      case FLOAT:
        return Bytes.toFloat(bytes);
      case DOUBLE:
        return Bytes.toDouble(bytes);
      case BOOLEAN:
        return Bytes.toBoolean(bytes);
      default:
        throw new UnexpectedFormatException("Field type '" + fieldSchema.getDisplayName() + "' is not supported");
    }
  }
}
