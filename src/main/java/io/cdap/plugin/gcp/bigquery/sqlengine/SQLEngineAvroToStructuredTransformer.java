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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.plugin.gcp.bigquery.source.BigQueryAvroToStructuredTransformer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * Create StructuredRecords from GenericRecords when pulling records from BigQuery.
 *
 * The following transformations are supported:
 * BigQuery INT64 -> Integer (with Overflow check)
 * BigQuery FLOAT64 -> Float (with Overflow check)
 *
 * Otherwise, field conversion gets delegated to the {@link BigQueryAvroToStructuredTransformer}
 */
public class SQLEngineAvroToStructuredTransformer extends BigQueryAvroToStructuredTransformer
  implements Serializable {

  private static final String LONG_TO_INTEGER_CONVERSION_ERROR =
    "Could not convert Long value '%d' into Integer when reading from BigQuery";
  private static final String INTEGER_CONVERSION_ERROR =
    "Could not convert Object '%s' into Integer when reading from BigQuery";
  private static final String DOUBLE_TO_FLOAT_CONVERSION_ERROR =
    "Could not convert Double '%f' into Float when reading from BigQuery";
  private static final String FLOAT_CONVERSION_ERROR =
    "Could not convert Object '%s' into Float when reading from BigQuery";

  @Override
  @Nullable
  protected Object convertField(Object field, Schema fieldSchema) throws IOException {
    if (field == null) {
      return null;
    }

    // Handle Int types
    if (fieldSchema.getType() == Schema.Type.INT) {
      return mapInteger(field);
    }

    // Handle float types
    if (fieldSchema.getType() == Schema.Type.FLOAT) {
      return mapFloat(field);
    }

    // Handle Strings that are stored as a Byte Buffer.
    if (fieldSchema.getType() == Schema.Type.STRING && field instanceof ByteBuffer) {
      return StandardCharsets.UTF_8.decode((ByteBuffer) field).toString();
    }

    return super.convertField(field, fieldSchema);
  }

  @VisibleForTesting
  protected static Integer mapInteger(Object field) throws SQLEngineException {
    if (field instanceof Long) {
      Long value = (Long) field;
      Integer result = value.intValue();
      if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE || (long) result != value) {
        throw new SQLEngineException(String.format(LONG_TO_INTEGER_CONVERSION_ERROR, value));
      }
      return value.intValue();
    } else {
      throw new SQLEngineException(String.format(INTEGER_CONVERSION_ERROR, field));
    }
  }

  @VisibleForTesting
  protected static Float mapFloat(Object field) {
    if (field instanceof Double) {
      Double value = (Double) field;
      Float result = value.floatValue();
      // Validate precision loss is minimal
      if (Math.abs(value - result) > 1e-15) {
        throw new SQLEngineException(String.format(LONG_TO_INTEGER_CONVERSION_ERROR, value));
      }

      return result;
    } else {
      throw new SQLEngineException(String.format(FLOAT_CONVERSION_ERROR, field));
    }
  }
}
