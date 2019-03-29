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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.common.GCPReferenceSinkConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * This class <code>BigQuerySinkConfig</code> provides all the configuration required for
 * configuring the <code>BigQuerySink</code> plugin.
 */
public final class BigQuerySinkConfig extends GCPReferenceSinkConfig {
  @Macro
  @Description("The dataset to write to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String dataset;

  @Macro
  @Description("The table to write to. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Macro
  @Nullable
  @Description("The Google Cloud Storage bucket to store temporary data in. "
    + "It will be automatically created if it does not exist, but will not be automatically deleted. "
    + "Cloud Storage data will be deleted after it is loaded into BigQuery. " +
    "If it is not provided, a unique bucket will be created and then deleted after the run finishes.")
  private String bucket;

  @Macro
  @Name("create-table-if-not-exist")
  @Description("Creates table if table specified does not exists.")
  private String createTableIfNotExist;

  @Macro
  @Name("create-dataset-if-not-exist")
  @Description("Creates dataset if dataset specified does not exists.")
  private String createDatasetIfNotExist;

  @Macro
  @Name("truncate-table")
  @Description("Truncate destination table before writing")
  private String truncateTable;

  @Macro
  @Name("kms-key")
  @Description("Cloud KMS key to be used to protect destination table")
  @Nullable
  private String kmsKey;

  @Macro
  @Description("The schema of the data to write. Must be compatible with the table schema.")
  private String schema;

  public BigQuerySinkConfig(String referenceName, String dataset, String table,
                            @Nullable String bucket, String createDatasetIfNotExist,
                            String createTableIfNotExist, String truncateTable,
                            String kmsKey, String schema) {
    this.referenceName = referenceName;
    this.dataset = dataset;
    this.table = table;
    this.bucket = bucket;
    this.schema = schema;
    this.createDatasetIfNotExist = createDatasetIfNotExist;
    this.createTableIfNotExist = createTableIfNotExist;
    this.truncateTable = truncateTable;
    this.kmsKey = kmsKey;
  }

  public String getDataset() {
    return dataset;
  }

  public String getTable() {
    return table;
  }

  @Nullable
  public String getBucket() {
    return bucket;
  }

  public boolean createDataset() {
    return createDatasetIfNotExist.equalsIgnoreCase("yes");
  }

  public boolean createTable() {
    return createDatasetIfNotExist.equalsIgnoreCase("yes");
  }

  public boolean truncateTable() {
    return truncateTable.equalsIgnoreCase("yes");
  }

  public String getKMSKey() {
    return kmsKey;
  }

  /**
   * @return the schema of the dataset
   * @throws IllegalArgumentException if the schema is null or invalid
   */
  public Schema getSchema() {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be specified.");
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
   * @param inputSchema input schema to bigquery sink
   */
  public void validate(@Nullable Schema inputSchema) {
    super.validate();
    if (!containsMacro("schema")) {
      Schema outputSchema = getSchema();
      for (Schema.Field field : outputSchema.getFields()) {
        // check if the required fields are present in the input schema.
        if (!field.getSchema().isNullable() && inputSchema != null && inputSchema.getField(field.getName()) == null) {
          throw new IllegalArgumentException(String.format("Required output field '%s' is not present in input schema.",
                                                           field.getName()));
        }

        Schema fieldSchema = BigQueryUtils.getNonNullableSchema(field.getSchema());

        if (!fieldSchema.getType().isSimpleType()) {
          throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'.",
                                                           field.getName(), fieldSchema.getType()));
        }
      }
    }
  }
}
