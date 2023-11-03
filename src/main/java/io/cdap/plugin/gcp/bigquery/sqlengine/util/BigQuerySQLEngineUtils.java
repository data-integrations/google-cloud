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
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryStage;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQueryJobType;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLEngineConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utility Class for the BigQuery SQL Engine implementation.
 */
public class BigQuerySQLEngineUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLEngineUtils.class);
  private static final Gson GSON = new Gson();

  public static final String GCS_PATH_FORMAT = BigQuerySinkUtils.GS_PATH_FORMAT + "/%s";
  public static final String BQ_TABLE_NAME_FORMAT = "%s_%s";
  public static final String METRIC_BYTES_PROCESSED = "bytes.processed";
  public static final String METRIC_BYTES_BILLED = "bytes.billed";
  public static final String METRIC_SLOT_MS = "slot.ms";

  private BigQuerySQLEngineUtils() {
    // no-op
  }

  /**
   * Build GCS path using a Bucket, Run ID and Table ID
   *
   * @param bucket  bucket name
   * @param runId   run ID
   * @param tableId table ID
   * @return GCS path with prefix
   */
  public static String getGCSPath(String bucket, String runId, String tableId) {
    return String.format(GCS_PATH_FORMAT, bucket, runId, tableId);
  }

  /**
   * Get new table/run identifier.
   *
   * @return
   */
  public static String newIdentifier() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * Build new BQ Table name.
   *
   * @param runId run ID to create a new table for.
   * @return new table name for BQ Table.
   */
  public static String getNewTableName(String runId) {
    return String.format(BQ_TABLE_NAME_FORMAT, runId, newIdentifier());
  }

  /**
   * Get the number of rows for a BQ table.
   *
   * @param bigQuery BigQuery client
   * @param dataset  Dataset Id
   * @param table    Table Name
   * @return number of rows for this table.
   */
  public static Long getNumRows(BigQuery bigQuery, DatasetId dataset, String table) {
    LOG.debug("Getting number of records stored in table {}", table);
    TableId tableId = TableId.of(dataset.getProject(), dataset.getDataset(), table);
    Table bgTable = bigQuery.getTable(tableId);

    if (bgTable == null) {
      throw new SQLEngineException(String.format("Table '%s' could not be found on dataset '%s' and project `%s`",
                                                 table, dataset.getDataset(), dataset.getProject()));
    }

    long numRows = bgTable.getNumRows().longValue();

    LOG.debug("Table {} contains {} records", table, numRows);
    return numRows;
  }

  /**
   * Creates an empty table with an empty schema to store records.
   * <p>
   * If the Engine Configuration specifies a TTL for tables, the table is created with the specified TTL.
   *
   * @param config   BigQuery SQL Engine Config instance
   * @param bigQuery BigQuery client
   * @param project  Project Name
   * @param dataset  Dataset Name
   * @param table    Table Name
   */
  public static void createEmptyTable(BigQuerySQLEngineConfig config,
                                      BigQuery bigQuery,
                                      String project,
                                      String dataset,
                                      String table) {

    LOG.debug("Creating empty table {} in dataset {} and project {}", table, dataset, project);

    // Define table name and create builder.
    TableId tableId = TableId.of(project, dataset, table);
    TableDefinition tableDefinition = StandardTableDefinition.of(com.google.cloud.bigquery.Schema.of());
    TableInfo.Builder tableInfoBuilder = TableInfo.newBuilder(tableId, tableDefinition);

    // Set TTL for table if needed.
    if (!config.shouldRetainTables() && config.getTempTableTTLHours() > 0) {
      long ttlMillis = TimeUnit.MILLISECONDS.convert(config.getTempTableTTLHours(), TimeUnit.HOURS);
      long expirationTime = Instant.now().toEpochMilli() + ttlMillis;
      tableInfoBuilder.setExpirationTime(expirationTime);
    }

    bigQuery.create(tableInfoBuilder.build());

    LOG.debug("Created empty table {} in dataset {} and project {}", table, dataset, project);
  }

  /**
   * Creates an empty table with schema, partitioning, clustering same as sourceTable.
   * <p>
   * If the Engine Configuration specifies a TTL for tables, the table is created with the specified TTL.
   *
   * @param bigQuery BigQuery client
   * @param project  Project Name
   * @param dataset  Dataset Name
   * @param table    Table Name
   * @param sourceTable The source table from which we want to copy records
   * @param tableTTL    Time to live for the destination table in millis
   */
  public static void createEmptyTableWithSourceConfig(BigQuery bigQuery,
                                                      String project,
                                                      String dataset,
                                                      String table,
                                                      Table sourceTable,
                                                      Long tableTTL) {

    LOG.debug("Creating empty table {} in dataset {} and project {} with configurations similar to {}",
              table, dataset, project, sourceTable.getTableId());

    StandardTableDefinition tableDefinitionSource = sourceTable.getDefinition();

    // Define table name and create builder.
    TableId tableId = TableId.of(project, dataset, table);
    TableDefinition tableDefinition =
      StandardTableDefinition.newBuilder()
        .setSchema(tableDefinitionSource.getSchema())
        .setTimePartitioning(tableDefinitionSource.getTimePartitioning())
        .setClustering(tableDefinitionSource.getClustering())
        .build();

    TableInfo.Builder tableInfoBuilder = TableInfo.newBuilder(tableId, tableDefinition);

    // Set TTL for table if needed.
    if (tableTTL > 0) {
      tableInfoBuilder.setExpirationTime(tableTTL);
    }

    bigQuery.create(tableInfoBuilder.build());

    LOG.debug("Created empty table {} in dataset {} and project {} with configurations similar to {}",
              table, dataset, project, sourceTable.getTableId());
  }

  /**
   *
   * @param bigQuery the bq client
   * @param tableId  The tableId of the table whose expiration is to be updated
   * @param tableTTL Time to live for the above tableId in millis
   */
  public static void updateTableExpiration(BigQuery bigQuery, TableId tableId, @Nullable Long tableTTL) {
    if (tableTTL == null || tableTTL <= 0) {
      return;
    }

    Table table = bigQuery.getTable(tableId);
    bigQuery.update(table.toBuilder().setExpirationTime(tableTTL).build());
    LOG.debug("Updated {}'s Expiration time to {}", tableId, tableTTL);
  }

  /**
   * Validate input stage schema. Any errors will be added to the supplied list of validation issues.
   *
   * @param inputStage         Input Stage
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
   * @param outputSchema       the schema to validate
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
   * @param onExpression       Join Condition to validate
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
   * Validates stages for a Join on Key operation
   * <p>
   * TODO: Update logic once BQ SQL engine joins support multiple outer join tables
   *
   * @param joinDefinition     Join Definition to validate
   * @param validationProblems List of validation problems to use to append messages
   */
  public static void validateJoinOnKeyStages(JoinDefinition joinDefinition, List<String> validationProblems) {
    // 2 stages are not an issue
    if (joinDefinition.getStages().size() < 3) {
      return;
    }

    // For 3 or more stages, we only support inner joins.
    boolean isInnerJoin = true;

    // If any of the stages is not required, this is an outer join
    for (JoinStage stage : joinDefinition.getStages()) {
      isInnerJoin &= stage.isRequired();
    }

    if (!isInnerJoin) {
      validationProblems.add(
        String.format("Only 2 input stages are supported for outer joins, %d stages supplied.",
                      joinDefinition.getStages().size()));
    }
  }

  /**
   * Check if the supplied schema is supported by the SQL Engine
   *
   * @param schema supplied schema to validate
   * @return whether this schema is supported by the SQL engine.
   */
  public static boolean isSupportedSchema(Schema schema) {
    return BigQuerySchemaValidation.validateSchema(schema).isSupported();
  }

  /**
   * Ensure the Stage name is valid for execution in BQ pushdown.
   * <p>
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

  /**
   * Get tags for BQ Pushdown tags
   *
   * @param operation the current operation that is being executed
   * @return Map containing tags for a job.
   */
  public static Map<String, String> getJobTags(BigQueryJobType operation) {
    return getJobTags(operation.getType());
  }

  /**
   * Get tags for BQ Pushdown tags
   *
   * @param operation the current operation that is being executed
   * @return Map containing tags for a job.
   */
  public static Map<String, String> getJobTags(String operation) {
    Map<String, String> labels = BigQueryUtil.getJobLabels(BigQueryUtil.BQ_JOB_TYPE_PUSHDOWN_TAG);
    labels.put("pushdown_operation", operation);
    return labels;
  }

  /**
   * Logs information about a BigQUery Job execution using a specified Logger instance
   *
   * @param job BigQuery Job
   * @param metrics map used to collect additional metrics for this job.
   */
  public static void logJobMetrics(Job job, Metrics metrics) {
    // Ensure job has statistics information
    if (job.getStatistics() == null) {
      LOG.warn("No statistics were found for BigQuery job {}", job.getJobId());
    }

    String startTimeStr = getISODateTimeString(job.getStatistics().getStartTime());
    String endTimeStr = getISODateTimeString(job.getStatistics().getEndTime());
    String executionTimeStr = getExecutionTimeString(job.getStatistics().getStartTime(),
                                                     job.getStatistics().getEndTime());

    // Print detailed query statistics if available
    if (job.getStatistics() instanceof JobStatistics.QueryStatistics) {
      JobStatistics.QueryStatistics queryStatistics = (JobStatistics.QueryStatistics) job.getStatistics();
      LOG.info("Metrics for job {}:\n" +
                 " Start: {} ,\n" +
                 " End: {} ,\n" +
                 " Execution time: {} ,\n" +
                 " Processed Bytes: {} ,\n" +
                 " Billed Bytes: {} ,\n" +
                 " Total Slot ms: {} ,\n" +
                 " Records per stage (read/write): {}",
               job.getJobId().getJob(),
               startTimeStr,
               endTimeStr,
               executionTimeStr,
               queryStatistics.getTotalBytesProcessed(),
               queryStatistics.getTotalBytesBilled(),
               queryStatistics.getTotalSlotMs(),
               getQueryStageRecordCounts(queryStatistics.getQueryPlan()));

      if (LOG.isTraceEnabled()) {
        LOG.trace("Additional Metrics for job {}:\n" +
                   " Query Plan: {} ,\n" +
                   " Query Timeline: {} \n",
                 job.getJobId().getJob(),
                 GSON.toJson(queryStatistics.getQueryPlan()),
                 GSON.toJson(queryStatistics.getTimeline()));
      }

      // Collect job metrics
      if (queryStatistics.getTotalBytesProcessed() != null) {
        metrics.countLong(METRIC_BYTES_PROCESSED, queryStatistics.getTotalBytesProcessed());
      }
      if (queryStatistics.getTotalBytesBilled() != null) {
        metrics.countLong(METRIC_BYTES_BILLED, queryStatistics.getTotalBytesBilled());
      }
      if (queryStatistics.getTotalSlotMs() != null) {
        metrics.countLong(METRIC_SLOT_MS, queryStatistics.getTotalSlotMs());
      }

      return;
    }

    // Print basic metrics
    LOG.info("Metrics for job: {}\n" +
               " Start: {} ,\n" +
               " End: {} ,\n" +
               " Execution time: {}",
             job.getJobId().getJob(),
             startTimeStr,
             endTimeStr,
             executionTimeStr);
  }

  private static String getISODateTimeString(Long epoch) {
    if (epoch == null) {
      return "N/A";
    }

    return Instant.ofEpochMilli(epoch).toString();
  }
  
  private static String getExecutionTimeString(Long startEpoch, Long endEpoch) {
    if (startEpoch == null || endEpoch == null) {
      return "N/A";
    }

    return (endEpoch - startEpoch) + " ms";
  }

  private static String getQueryStageRecordCounts(List<QueryStage> queryPlan) {
    if (queryPlan == null || queryPlan.isEmpty()) {
      return "N/A";
    }

    return queryPlan.stream()
      .map(qs -> formatRecordCount(qs.getRecordsRead()) + "/" + formatRecordCount(qs.getRecordsWritten()))
      .collect(Collectors.joining(" , ", "[ ", " ]"));
  }

  private static String formatRecordCount(Long val) {
    if (val == null) {
      return "N/A";
    }

    return val.toString();
  }
}
