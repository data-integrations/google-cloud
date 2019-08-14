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
package io.cdap.plugin.gcp.bigquery.actions;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base class for Big Query Merge action configs.
 */
public class BigQueryMergeActionConfig extends GCPConfig {

  public static final String NAME_TABLE_KEY = "relationTableKey";

  @Macro
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String sourceDataset;

  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String sourceTable;

  @Macro
  @Description("The dataset to write to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String destinationDataset;

  @Macro
  @Description("The table to write to. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String destinationTable;

  @Macro
  @Description("Whether to modify the BigQuery destination table schema if it differs from the source table schema.")
  private boolean allowSchemaRelaxation;

  @Macro
  @Description("List of fields that determines relation between source table and destination table.")
  private String relationTableKey;

  public void validate() {
    validateTableExists(sourceDataset, sourceTable);
    validateTableExists(destinationDataset, destinationTable);
    validateOperationProperties();
  }

  private void validateTableExists(String datasetName, String tableName) {
    Table table = BigQueryUtil.getBigQueryTable(project, datasetName, tableName, serviceFilePath);

    if (table == null) {
      throw new IllegalStateException(String.format("BigQuery table '%s:%s.%s' does not exist", project, sourceDataset,
                                                    sourceTable));
    }
  }

  private void validateOperationProperties() {
    Table table = BigQueryUtil.getBigQueryTable(project, sourceDataset, sourceTable, serviceFilePath);

    FieldList sourceFields = table.getDefinition().getSchema().getFields();
    List<String> fields = sourceFields.stream().map(Field::getName).collect(Collectors.toList());

    List<String> keyFields = Arrays.stream(Objects.requireNonNull(getRelationTableKey()).split(","))
      .map(String::trim).collect(Collectors.toList());

    String result = keyFields.stream().filter(s -> !fields.contains(s)).map(s -> String.format("'%s'", s))
      .collect(Collectors.joining(", "));
    if (!result.isEmpty()) {
      throw new InvalidConfigPropertyException(String.format(
        "Fields %s are in the table key, but not in the input schema.", result), NAME_TABLE_KEY);
    }
  }

  public String getSourceDataset() {
    return sourceDataset;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getDestinationDataset() {
    return destinationDataset;
  }

  public String getDestinationTable() {
    return destinationTable;
  }

  public String getRelationTableKey() {
    return relationTableKey;
  }

  public boolean isAllowSchemaRelaxation() {
    return allowSchemaRelaxation;
  }

}
