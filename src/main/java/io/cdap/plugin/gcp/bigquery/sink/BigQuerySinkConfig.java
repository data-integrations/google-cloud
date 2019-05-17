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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * This class <code>BigQuerySinkConfig</code> provides all the configuration required for
 * configuring the <code>BigQuerySink</code> plugin.
 */
public final class BigQuerySinkConfig extends AbstractBigQuerySinkConfig {

  @Macro
  @Description("The table to write to. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Macro
  @Nullable
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private String schema;

  public BigQuerySinkConfig(String referenceName, String dataset, String table,
                            @Nullable String bucket, @Nullable String schema) {
    this.referenceName = referenceName;
    this.dataset = dataset;
    this.table = table;
    this.bucket = bucket;
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema() {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  /**
   * Verifies if output schema only contains simple types. It also verifies if all the output schema fields are
   * present in input schema.
   *
   * @param inputSchema input schema to BigQuery sink
   */
  public void validate(@Nullable Schema inputSchema) {
    super.validate();
    if (!containsMacro("schema")) {
      Schema outputSchema = getSchema();
      if (outputSchema == null) {
        return;
      }
      for (Schema.Field field : outputSchema.getFields()) {
        // check if the required fields are present in the input schema.
        if (!field.getSchema().isNullable() && inputSchema != null && inputSchema.getField(field.getName()) == null) {
          throw new IllegalArgumentException(String.format("Required output field '%s' is not present in input schema.",
                                                           field.getName()));
        }

        Schema fieldSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());
        Schema.Type type = fieldSchema.getType();
        String name = field.getName();

        if (!BigQueryUtil.SUPPORTED_TYPES.contains(type)) {
          throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'.", name, type));
        }

        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        // BigQuery schema precision must be at most 38 and scale at most 9
        if (logicalType == Schema.LogicalType.DECIMAL) {
          if (fieldSchema.getPrecision() > 38 || fieldSchema.getScale() > 9) {
            throw new IllegalArgumentException(
              String.format("Numeric Field '%s' has invalid precision '%s' and scale '%s'. " +
                              "Precision must be at most 38 and scale must be at most 9.",
                            field.getName(), fieldSchema.getPrecision(), fieldSchema.getScale()));
          }
        }

        if (type == Schema.Type.ARRAY) {
          BigQueryUtil.validateArraySchema(field.getSchema(), name);
        }
      }
    }
  }
}
