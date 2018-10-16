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
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StructuredToTextTransformer extends AbstractStructuredRecordTransformer<Text> {
  private final Joiner joiner;

  public StructuredToTextTransformer(String delimiter, @Nullable co.cask.cdap.api.data.schema.Schema outputSchema) {
    super(outputSchema);
    this.joiner = Joiner.on(delimiter);
  }

  @Override
  public Text transform(StructuredRecord structuredRecord, Schema schema) {
    Schema structuredRecordSchema = structuredRecord.getSchema();
    List<String> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema.Field schemaField = structuredRecordSchema.getField(fieldName);
      if (schemaField == null) {
        throw new IllegalArgumentException("Input record does not contain the " + fieldName + " field.");
      }
      fields.add(convertField(structuredRecord.get(fieldName), schemaField.getSchema()));
    }
    return new Text(joiner.join(fields));
  }

  protected String convertField(Object field, Schema fieldSchema) {
    if (field == null) {
      return "";
    }
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    switch (fieldType) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case ENUM:
        return field.toString();
      case BYTES:
        if (field instanceof ByteBuffer) {
          return Bytes.toStringBinary(((ByteBuffer) field).array());
        }
        return Bytes.toStringBinary((byte[]) field);
      case NULL:
        return "";
      default:
        throw new UnexpectedFormatException("Cannot convert fields of type " + fieldType + " to text");
    }
  }
}
