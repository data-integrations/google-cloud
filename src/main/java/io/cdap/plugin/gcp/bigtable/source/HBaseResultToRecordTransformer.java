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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Objects;

/**
 * Transforms Google Cloud Bigtable {@link Result} to {@link StructuredRecord}.
 */
public class HBaseResultToRecordTransformer {

  private final Schema schema;
  private final String keyAlias;

  public HBaseResultToRecordTransformer(Schema schema, String keyAlias) {
    this.schema = schema;
    this.keyAlias = keyAlias;
  }

  public StructuredRecord transform(Result result) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields());
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      try {
        if (fieldName.equals(keyAlias)) {
          Object value = convertBytesToField(result.getRow(), fieldSchema);
          recordBuilder.set(fieldName, value);
        } else {
          HBaseColumn hBaseColumn = HBaseColumn.fromFullName(fieldName);
          byte[] valueBytes =
            result.getValue(Bytes.toBytes(hBaseColumn.getFamily()), Bytes.toBytes(hBaseColumn.getQualifier()));
          Object value = convertBytesToField(valueBytes, fieldSchema);
          recordBuilder.set(fieldName, value);
        }
      } catch (Exception e) {
        throw new UnexpectedFormatException(String.format("Failed to transform field '%s'. Reason: %s",
                                                          fieldName, e.getMessage()));
      }
    }
    return recordBuilder.build();
  }

  private Object convertBytesToField(byte[] bytes, Schema fieldSchema) {
    if (fieldSchema.isNullable()) {
      if (null == bytes) {
        return null;
      }
      return convertBytesToField(bytes, fieldSchema.getNonNullable());
    }
    switch (fieldSchema.getType()) {
      case NULL:
        return null;
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
        throw new UnexpectedFormatException("Field type '" + fieldSchema.getType() + "' is not supported");
    }
  }

}
