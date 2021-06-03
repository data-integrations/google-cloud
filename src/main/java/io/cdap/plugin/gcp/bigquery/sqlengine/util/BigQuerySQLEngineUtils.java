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

package io.cdap.plugin.gcp.bigquery.sqlengine.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Utility Class for the BigQuery SQL Engine implementation.
 */
public class BigQuerySQLEngineUtils {

  public static final String GCS_PATH_FORMAT = BigQuerySinkUtils.GS_PATH_FORMAT + "/%s";
  public static final String BQ_TABLE_NAME_FORMAT = "%s_%s";

  private BigQuerySQLEngineUtils() {
    // no-op
  }

  /**
   * Build GCS path using a Bucket, Run ID and Table ID
   * @param bucket bucket name
   * @param runId run ID
   * @param tableId table ID
   * @return GCS path with prefix
   */
  public static String getGCSPath(String bucket, String runId, String tableId) {
    return String.format(GCS_PATH_FORMAT, bucket, runId, tableId);
  }

  /**
   * Get new table/run identifier.
   * @return
   */
  public static String newIdentifier() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * Build new BQ Table name.
   * @param runId run ID to create a new table for.
   * @return new table name for BQ Table.
   */
  public static String getNewTableName(String runId) {
    return String.format(BQ_TABLE_NAME_FORMAT, runId, newIdentifier());
  }

  /**
   * Get the number of rows for a BQ table.
   * @param bigQuery BigQuery client
   * @param project Project Name
   * @param dataset Dataset Name
   * @param table Table Name
   * @return number of rows for this table.
   */
  public static Long getNumRows(BigQuery bigQuery, String project, String dataset, String table) {
    TableId tableId = TableId.of(project, dataset, table);
    Table bgTable = bigQuery.getTable(tableId);

    if (bgTable == null) {
      throw new SQLEngineException(String.format("Table '%s' could not be found on dataset '%s' and project `%s`",
                                                 table, dataset, project));
    }

    return bgTable.getNumRows().longValue();
  }

  /**
   * Validate input stage schema. Any errors will be added to the supplied list of validation issues.
   *
   * @param inputStage Input Stage
   * @param validationProblems List of validation problems to use to append messages
   */
  public static void validateInputStage(JoinStage inputStage, List<String> validationProblems) {
    String stageName = inputStage.getStageName();

    if (inputStage.getSchema() == null) {
      // Null schemas are not supported.
      validationProblems.add(String.format("Input schema from stage '%s' is null", stageName));
    } else {
      // Validate schema
      BigQuerySchemaValidation bigQuerySchemaValidation =
        BigQuerySchemaValidation.validateSchema(inputStage.getSchema());
      if (!bigQuerySchemaValidation.isSupported()) {
        validationProblems.add(
          String.format("Input schema from stage '%s' contains unsupported field types for the following fields: %s",
                        stageName,
                        String.join(", ", bigQuerySchemaValidation.getInvalidFields())));
      }
    }

    if (!isValidIdentifier(stageName)) {
      validationProblems.add(
        String.format("Unsupported stage name '%s'. Stage names cannot contain backtick ` or backslash \\ ",
                      stageName));
    }
  }

  /**
   * Validate output stage schema. Any errors will be added to the supplied list of validation issues.
   *
   * @param outputSchema the schema to validate
   * @param validationProblems List of validation problems to use to append messages
   */
  public static void validateOutputSchema(@Nullable Schema outputSchema, List<String> validationProblems) {
    if (outputSchema == null) {
      // Null schemas are not supported.
      validationProblems.add("Output Schema is null");
    } else {
      // Validate schema
      BigQuerySchemaValidation bigQuerySchemaValidation = BigQuerySchemaValidation.validateSchema(outputSchema);
      if (!bigQuerySchemaValidation.isSupported()) {
        validationProblems.add(
          String.format("Output schema contains unsupported field types for the following fields: %s",
                        String.join(", ", bigQuerySchemaValidation.getInvalidFields())));
      }
    }
  }

  /**
   * Validate on expression join condition
   *
   * @param onExpression Join Condition to validate
   * @param validationProblems List of validation problems to use to append messages
   */
  public static void validateOnExpressionJoinCondition(JoinCondition.OnExpression onExpression,
                                                List<String> validationProblems) {
    for (Map.Entry<String, String> alias : onExpression.getDatasetAliases().entrySet()) {
      if (!isValidIdentifier(alias.getValue())) {
        validationProblems.add(
          String.format("Unsupported alias '%s' for stage '%s'", alias.getValue(), alias.getKey()));
      }
    }
  }

  /**
   * Ensure the Stage name is valid for execution in BQ pushdown.
   *
   * Due to differences in character escaping rules in Spark and BigQuery, identifiers that are accepted in Spark
   * might not be valid in BigQuery. Due to this limitation, we don't support stage names or aliases containing
   * backslash \ or backtick ` characters at this time.
   *
   * @param identifier stage name or alias to validate
   * @return whether this stage name is valid for BQ Pushdown.
   */
  public static boolean isValidIdentifier(String identifier) {
    return identifier != null && !identifier.contains("\\") && !identifier.contains("`");
  }
}
