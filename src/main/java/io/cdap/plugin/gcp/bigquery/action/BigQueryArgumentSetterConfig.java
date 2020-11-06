/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.action;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Builder;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.ConfigUtil;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class BigQueryArgumentSetterConfig extends AbstractBigQueryActionConfig {

  public static final String NAME_DATASET = "dataset";
  public static final String NAME_TABLE = "table";
  public static final String NAME_DATASET_PROJECT = "datasetProject";
  public static final String NAME_ARGUMENT_SELECTION_CONDITIONS = "argumentSelectionConditions";
  public static final String NAME_ARGUMENTS_COLUMNS = "argumentsColumns";
  private static final String QUERY_TEMPLATE = "Select %s from %s where %s";

  @Name(NAME_DATASET)
  @Macro
  @Description(
    "The dataset the table belongs to. A dataset is contained within a specific project. "
      + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String dataset;

  @Name(NAME_TABLE)
  @Macro
  @Description(
    "The table to read from. A table contains individual records organized in rows. "
      + "Each record is composed of columns (also called fields). "
      + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Name(NAME_ARGUMENT_SELECTION_CONDITIONS)
  @Macro
  @Description(
    "A set of conditions for identifying the arguments to run a pipeline."
      + "A particular use case for this would be feed=marketing;date=20200427.")
  private String argumentSelectionConditions;

  @Name(NAME_ARGUMENTS_COLUMNS)
  @Macro
  @Description(
    "Name of the columns, separated by `,` ,that contains the arguments for this run."
      + "A particular use case for this would be country, device")
  private String argumentsColumns;

  public BigQueryArgumentSetterConfig(
    String dataset,
    String table,
    String argumentSelectionConditions,
    String argumentsColumns) {
    this.dataset = dataset;
    this.table = table;
    this.argumentSelectionConditions = argumentSelectionConditions;
    this.argumentsColumns = argumentsColumns;
  }

  public String getDataset() {
    return dataset;
  }

  public String getTable() {
    return table;
  }

  public String getArgumentSelectionConditions() {
    return argumentSelectionConditions;
  }

  @Nullable
  public String getArgumentsColumns() {
    return argumentsColumns;
  }

  @Override
  public void validate(FailureCollector collector) {
    validateProperties(collector);

    if (canConnect()) {
      try {
        getQueryJobConfiguration();
      } catch (Exception e) {
        collector.addFailure(e.getMessage(), "");
      }
    }
  }

  public void validateProperties(FailureCollector collector) {
    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }

    if (!containsMacro(NAME_TABLE)) {
      BigQueryUtil.validateTable(table, NAME_TABLE, collector);
    }

    validateArgumentsSelectionConditions(getArgumentSelectionConditions(), collector);
    validateArgumentsColumns(argumentsColumns, collector);
    collector.getOrThrowException();
  }

  private boolean canConnect() {
    return !containsMacro(NAME_DATASET)
      && !containsMacro(NAME_TABLE)
      && !containsMacro(NAME_DATASET_PROJECT)
      && !containsMacro(NAME_SERVICE_ACCOUNT_TYPE)
      && !(containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || containsMacro(NAME_SERVICE_ACCOUNT_JSON))
      && !containsMacro(NAME_PROJECT)
      && !containsMacro(NAME_ARGUMENT_SELECTION_CONDITIONS)
      && !containsMacro(argumentsColumns);
  }

  private void validateArgumentsSelectionConditions(
    String argumentSelectionConditions, FailureCollector collector) {
    if (containsMacro(NAME_ARGUMENT_SELECTION_CONDITIONS)) {
      return;
    }

    if (Strings.isNullOrEmpty(argumentSelectionConditions)) {
      collector.addFailure("Arguments Selection Conditions is empty.",
                           "Arguments Selection condition can not be empty.")
        .withConfigProperty(NAME_ARGUMENT_SELECTION_CONDITIONS);
      return;
    }

    try {
      getArgumentSelectionConditionsMap();
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), "Invalid key value pair for Argument Selection Conditions.")
        .withConfigProperty(NAME_ARGUMENT_SELECTION_CONDITIONS);;
    }
  }

  private void validateArgumentsColumns(String argumentsColumns, FailureCollector collector) {
    if (containsMacro(NAME_ARGUMENTS_COLUMNS)) {
      return;
    }
    if (Strings.isNullOrEmpty(argumentsColumns)) {
      collector.addFailure("Arguments Columns is empty.", "Arguments Columns can not be empty.")
        .withConfigProperty(NAME_ARGUMENTS_COLUMNS);
    }
  }

  public QueryJobConfiguration getQueryJobConfiguration() {
    Table sourceTable = BigQueryUtil.getBigQueryTable(project, dataset, table, getServiceAccount(),
                                                      isServiceAccountFilePath());

    StandardTableDefinition tableDefinition = Objects.requireNonNull(sourceTable).getDefinition();
    FieldList fields = Objects.requireNonNull(tableDefinition.getSchema()).getFields();

    Map<String, String> argumentConditionMap = getArgumentSelectionConditionsMap();
    Map<String, Field> argumentConditionFields = extractArgumentsFields(fields, argumentConditionMap);

    checkIfArgumentsColumnsExitsInSource(argumentConditionMap, argumentConditionFields);

    String selectClause = getSelectClause();
    String whereCondition = getWhereCondition(argumentConditionMap.keySet());
    String tableName = dataset + "." + table;
    String query = String.format(QUERY_TEMPLATE, selectClause, tableName, whereCondition);

    Builder queryJobConfiguration = QueryJobConfiguration.newBuilder(query);
    getParametersValues(argumentConditionMap.entrySet(), argumentConditionFields)
      .forEach(
        stringQueryParameterValueSimpleEntry ->
          queryJobConfiguration.addNamedParameter(
            stringQueryParameterValueSimpleEntry.getKey(),
            stringQueryParameterValueSimpleEntry.getValue()));

    return queryJobConfiguration.build();
  }

  private void checkIfArgumentsColumnsExitsInSource(Map<String, String> argumentConditionMap,
                                                    Map<String, Field> argumentConditionFields) {
    if (argumentConditionMap.size() == argumentConditionFields.size()) {
      return;
    }
    String nonExistingColumnNames = argumentConditionMap.keySet().stream()
      .filter(columnName -> !argumentConditionFields.containsKey(columnName))
      .collect(Collectors.joining(" ,"));
    throw new RuntimeException(String.format(
      "Columns: \" %s \"do not exist in table. Argument selections columns must exist in table.",
      nonExistingColumnNames));
  }

  private Map<String, Field> extractArgumentsFields(
    FieldList fields, Map<String, String> argumentConditionKeyPair) {
    return fields.stream()
      .filter(field -> argumentConditionKeyPair.containsKey(field.getName()))
      .collect(Collectors.toMap(Field::getName, Function.identity()));
  }

  private String getSelectClause() {
    return String.join(" ,", getArgumentsColumnsList());
  }

  private List<SimpleEntry<String, QueryParameterValue>> getParametersValues(
    Set<Entry<String, String>> argumentConditionKeyPair,
    Map<String, Field> argumentConditionFields) {

    return argumentConditionKeyPair.stream()
      .map(
        entry -> {
          Field field = argumentConditionFields.get(entry.getKey());
          String value = entry.getValue();

          QueryParameterValue build =
            QueryParameterValue.newBuilder()
              .setType(field.getType().getStandardType())
              .setValue(value)
              .build();

          return new SimpleEntry<>(entry.getKey(), build);
        })
      .collect(Collectors.toList());
  }

  private String getWhereCondition(Set<String> argumentConditionKey) {
    return argumentConditionKey.stream()
      .map(columnName -> String.format("%s = @%s ", columnName, columnName))
      .collect(Collectors.joining(" AND "));
  }

  private Map<String, String> getArgumentSelectionConditionsMap() {
    return ConfigUtil.parseKeyValueConfig(argumentSelectionConditions, ";", "=");
  }

  private List<String> getArgumentsColumnsList() {
    String[] parts = getArgumentsColumns().split(",");
    return Lists.newArrayList(parts);
  }
}
