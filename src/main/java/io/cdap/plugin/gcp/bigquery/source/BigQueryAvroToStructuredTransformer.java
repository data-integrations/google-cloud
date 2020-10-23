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

package io.cdap.plugin.gcp.bigquery.source;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.RecordConverter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Create StructuredRecords from GenericRecords. Contains custom logic for BigQuery date and time types.
 */
public class BigQueryAvroToStructuredTransformer extends RecordConverter<GenericRecord, StructuredRecord> {

  private Schema genericRecordSchema;

  public StructuredRecord transform(GenericRecord genericRecord) throws IOException {
    if (genericRecordSchema == null) {
      genericRecordSchema = Schema.parseJson(genericRecord.getSchema().toString());
    }
    return transform(genericRecord, genericRecordSchema);
  }

  @Override
  public StructuredRecord transform(GenericRecord genericRecord, Schema structuredSchema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : structuredSchema.getFields()) {
      String fieldName = field.getName();
      Object value = convertField(genericRecord.get(fieldName), field.getSchema());
      builder.set(fieldName, value);
    }
    return builder.build();
  }

  @Override
  @Nullable
  protected Object convertField(Object field, Schema fieldSchema) throws IOException {
    if (field == null) {
      return null;
    }

    // Union schema expected to be nullable schema. Underlying non-nullable type should always be a supported type
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    try {
      if (logicalType != null) {
        switch (logicalType) {
          case DATE:
            // date will be in yyyy-mm-dd format
            return Math.toIntExact(LocalDate.parse(field.toString()).toEpochDay());
          case TIME_MILLIS:
            // time will be in hh:mm:ss format
            return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(field.toString()).toNanoOfDay()));
          case TIME_MICROS:
            // time will be in hh:mm:ss format
            return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(field.toString()).toNanoOfDay());
          case TIMESTAMP_MILLIS:
          case TIMESTAMP_MICROS:
            return field;
          case DECIMAL:
            ByteBuffer value = (ByteBuffer) field;
            byte[] bytes = new byte[value.remaining()];
            int pos = value.position();
            value.get(bytes);
            value.position(pos);
            return bytes;
          default:
            throw new UnexpectedFormatException("Field type '" + fieldSchema.getDisplayName() + "' is not supported.");
        }
      }
    } catch (ArithmeticException e) {
      throw new IOException("Field type %s has value that is too large." + fieldType);
    }

    // Complex types like maps and unions are not supported in BigQuery plugins.
    if (!BigQuerySourceConfig.SUPPORTED_TYPES.contains(fieldType)) {
      throw new UnexpectedFormatException("Field type " + fieldType + " is not supported.");
    }

    if (fieldSchema.getType() == Schema.Type.RECORD && field instanceof List) {
      List<Object> valuesList = (List<Object>) field;
      List<Object> resultList = new ArrayList<>(valuesList.size());
      for (Object value : valuesList) {
        resultList.add(super.convertField(value, fieldSchema));
      }
      return resultList;
    }
    return super.convertField(field, fieldSchema);
  }

  @Override
  protected Object convertBytes(Object field) {
    return field instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) field) : field;
  }

}
