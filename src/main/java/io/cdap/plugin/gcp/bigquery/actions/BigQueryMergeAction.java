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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A action plugin to merge two Big Query tables.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryMergeAction.NAME)
@Description("This action merges two BigQuery tables.")
public class BigQueryMergeAction extends BigQueryQueryExecute {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMergeAction.class);
  public static final String NAME = "BigQueryMerge";
  private static final String MERGE_QUERY = "MERGE %s T USING %s S ON %s WHEN MATCHED THEN UPDATE SET %s " +
    "WHEN NOT MATCHED THEN INSERT (%s) VALUES(%s)";

  private BigQueryMergeActionConfig config;

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    LOG.debug("Run merge action.");
    if (config.isAllowSchemaRelaxation()) {
      updateTableSchema();
    }
    String query = generateQuery();
    setQuery(query);
    super.run(context);
    LOG.debug("Complete merge action.");
  }

  private String generateQuery() {
    String criteriaTemplate = "T.%s = S.%s";
    String sourceTable = config.getSourceDataset() + "." + config.getSourceTable();
    String destinationTable = config.getDestinationDataset() + "." + config.getDestinationTable();
    List<String> tableKeyList = Arrays.stream(config.getRelationTableKey().split(",")).collect(Collectors.toList());
    String criteria = tableKeyList.stream().map(s -> String.format(criteriaTemplate, s, s))
      .collect(Collectors.joining(" AND "));
    List<String> tableFieldsList = getTableFieldList();
    String fieldsForUpdate = tableFieldsList.stream().filter(s -> !tableKeyList.contains(s))
      .map(s -> String.format(criteriaTemplate, s, s)).collect(Collectors.joining(", "));
    String insertFields = String.join(", ", tableFieldsList);
    return String.format(MERGE_QUERY, destinationTable, sourceTable, criteria, fieldsForUpdate,
                         insertFields, insertFields);
  }

  private List<String> getTableFieldList() {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    TableId sourceTableId = TableId.of(config.getSourceDataset(), config.getSourceTable());

    com.google.cloud.bigquery.Table sourceTable = bigquery.getTable(sourceTableId);

    FieldList sourceFields = sourceTable.getDefinition().getSchema().getFields();
    return sourceFields.stream().map(Field::getName).collect(Collectors.toList());
  }

  private void updateTableSchema() {
    LOG.debug("Update/Upsert table schema update");
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    TableId sourceTableId = TableId.of(config.getSourceDataset(), config.getSourceTable());
    TableId destinationTableId = TableId.of(config.getDestinationDataset(), config.getDestinationTable());

    com.google.cloud.bigquery.Table sourceTable = bigquery.getTable(sourceTableId);
    com.google.cloud.bigquery.Table destinationTable = bigquery.getTable(destinationTableId);

    FieldList sourceFields = sourceTable.getDefinition().getSchema().getFields();
    FieldList destinationFields = destinationTable.getDefinition().getSchema().getFields();
    Map<String, Field> sourceFieldMap = sourceFields.stream()
      .collect(Collectors.toMap(Field::getName, x -> x));

    List<Field> resultFieldsList = destinationFields.stream()
      .filter(field -> !sourceFieldMap.containsKey(field.getName()))
      .collect(Collectors.toList());
    resultFieldsList.addAll(sourceFields);

    Schema newSchema = Schema.of(resultFieldsList);
    bigquery.update(
      destinationTable.toBuilder().setDefinition(
        destinationTable.getDefinition().toBuilder().setSchema(newSchema).build()
      ).build()
    );
  }

}
