/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.gcp.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.hydrator.common.HiveSchemaConverter;
import org.apache.hadoop.io.*;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates ORCStruct records from StructuredRecords
 */
public class StructuredToOrcTransformer extends AbstractStructuredRecordTransformer<OrcStruct> {

  private static final Logger LOG = LoggerFactory.getLogger(StructuredToOrcTransformer.class);
  private final Map<Schema, TypeDescription> schemaCache = new HashMap<>();

  public StructuredToOrcTransformer(@Nullable Schema outputSchema) {
    super(outputSchema);
  }

  @Override
  public OrcStruct transform(StructuredRecord input, Schema schema) {
    List<Schema.Field> fields = input.getSchema().getFields();
    OrcStruct orcRecord = parseOrcSchema(input.getSchema());
    // populate ORC struct orcRecord object
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      try {
        WritableComparable writable = convertToWritable(field, input);
        orcRecord.setFieldValue(field.getName(), writable);
      } catch (UnsupportedTypeException e) {
        LOG.debug("{} is not a supported type", field.getName(), e);
        throw new IllegalArgumentException(String.format("%s is not a supported type", field.getName()), e);
      }
    }
    return orcRecord;
  }

  private OrcStruct parseOrcSchema(Schema inputSchema) {
    OrcStruct orcRecord;
    if (schemaCache.containsKey(inputSchema)) {
      TypeDescription schema = schemaCache.get(inputSchema);
      orcRecord = (OrcStruct) OrcStruct.createValue(schema);
    } else {
      StringBuilder builder = new StringBuilder();
      try {
        HiveSchemaConverter.appendType(builder, inputSchema);
      } catch (UnsupportedTypeException e) {
        throw new IllegalArgumentException(String.format("Not a valid Schema %s", inputSchema.toString()), e);
      }
      TypeDescription schema = TypeDescription.fromString(builder.toString());
      orcRecord = (OrcStruct) OrcStruct.createValue(schema);
      schemaCache.put(inputSchema, schema);
    }
    return orcRecord;
  }

  private WritableComparable convertToWritable(Schema.Field field, StructuredRecord input)
    throws UnsupportedTypeException {
    Object fieldVal = input.get(field.getName());
    Schema fieldSchema = field.getSchema();
    Schema.Type fieldType = fieldSchema.getType();
    if (fieldSchema.isNullable()) {
      if (fieldVal == null) {
        return null;
      }
      fieldType = fieldSchema.getNonNullable().getType();
    }
    switch (fieldType) {
      case NULL:
        return null;
      case STRING:
        return new Text((String) fieldVal);
      case ENUM:
        return new Text(fieldVal.toString());
      case BOOLEAN:
        return new BooleanWritable((Boolean) fieldVal);
      case INT:
        return new IntWritable((Integer) fieldVal);
      case LONG:
        return new LongWritable((Long) fieldVal);
      case FLOAT:
        return new FloatWritable((Float) fieldVal);
      case DOUBLE:
        return new DoubleWritable((Double) fieldVal);
      case BYTES:
        if (fieldVal instanceof byte[]) {
          return new BytesWritable((byte[]) fieldVal);
        } else {
          return new BytesWritable(Bytes.getBytes((ByteBuffer) fieldVal));
        }
      default:
        throw new UnsupportedTypeException(String.format("%s type is currently not supported in ORC",
                                                         field.getSchema().getType().name()));
    }
  }
}
