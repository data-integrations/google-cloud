/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteResult;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkConfig;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sink.Operation;
import io.cdap.plugin.gcp.bigquery.sink.PartitionType;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * SQL Pull Dataset implementation for BigQuery backed datasets.
 */
public class BigQueryWrite {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWrite.class);
  private static final Gson GSON = new Gson();

  public static final String SQL_OUTPUT_JOB_ID = "jobId";
  public static final String SQL_OUTPUT_CONFIG = "config";
  public static final String SQL_OUTPUT_FIELDS = "fields";
  public static final String SQL_OUTPUT_SCHEMA = "schema";
  private static final Type LIST_OF_STRINGS_TYPE = new TypeToken<ArrayList<String>>() { }.getType();
  private static final String BQ_PUSHDOWN_OPERATION_TAG = "write";

  private final BigQuerySQLEngineConfig sqlEngineConfig;
  private final BigQuery bigQuery;
  private final String datasetName;
  private final SQLWriteRequest writeRequest;
  private final TableId sourceTableId;
  private final Metrics metrics;

  private BigQueryWrite(String datasetName,
                        BigQuerySQLEngineConfig sqlEngineConfig,
                        BigQuery bigQuery,
                        SQLWriteRequest writeRequest,
                        TableId sourceTableId,
                        Metrics metrics) {
    this.datasetName = datasetName;
    this.sqlEngineConfig = sqlEngineConfig;
    this.bigQuery = bigQuery;
    this.writeRequest = writeRequest;
    this.sourceTableId = sourceTableId;
    this.metrics = metrics;
  }

  public static BigQueryWrite getInstance(String datasetName,
                                          BigQuerySQLEngineConfig sqlEngineConfig,
                                          BigQuery bigQuery,
                                          SQLWriteRequest writeRequest,
                                          TableId sourceTableId,
                                          Metrics metrics) {
    return new BigQueryWrite(datasetName,
                             sqlEngineConfig,
                             bigQuery,
                             writeRequest,
                             sourceTableId,
                             metrics);
  }

  public SQLWriteResult write() {
    // We use this atomic reference to delete a new table if it was created for this execution.
    AtomicReference<TableId> newDestinationTable = new AtomicReference<>(null);
    try {
      return writeInternal(writeRequest, newDestinationTable);
    } catch (InterruptedException e) {
      LOG.error("Interrupted exception during BigQuery write operation.", e);
    } catch (BigQueryException bqe) {
      LOG.error("BigQuery exception during BigQuery write operation", bqe);
    } catch (Exception e) {
      LOG.error("Exception during BigQuery write operation", e);
    }

    // If a new table was created for this execution, but the execution failed for any reason,
    // delete the created table so the standard sink workflow can succeed.
    if (newDestinationTable.get() != null) {
      tryDeleteTable(newDestinationTable.get());
    }

    // Return as a failure if the operation threw an exception.
    return SQLWriteResult.faiure(writeRequest.getDatasetName());
  }

  private SQLWriteResult writeInternal(SQLWriteRequest writeRequest,
                                       AtomicReference<TableId> newDestinationTable)
    throws BigQueryException, InterruptedException {
    // Check if this output matches the expected engine.
    String datasetName = writeRequest.getDatasetName();
    if (!BigQuerySQLEngine.class.getName().equals(writeRequest.getOutput().getSqlEngineClassName())) {
      LOG.debug("Got output for another SQL engine {}, skipping", writeRequest.getOutput().getSqlEngineClassName());
      return SQLWriteResult.unsupported(datasetName);
    }

    // Get configuration properties from write request arguments
    Map<String, String> arguments = writeRequest.getOutput().getArguments();
    String jobId = arguments.get(SQL_OUTPUT_JOB_ID);
    BigQuerySinkConfig sinkConfig = GSON.fromJson(arguments.get(SQL_OUTPUT_CONFIG), BigQuerySinkConfig.class);
    Schema schema = GSON.fromJson(arguments.get(SQL_OUTPUT_SCHEMA), Schema.class);
    List<String> fields = GSON.fromJson(arguments.get(SQL_OUTPUT_FIELDS), LIST_OF_STRINGS_TYPE);

    // Get destination table information
    String destinationProject = sinkConfig.getDatasetProject();
    String destinationDataset = sinkConfig.getDataset();
    String destinationTableName = sinkConfig.getTable();
    TableId destinationTableId = TableId.of(destinationProject, destinationDataset, destinationTableName);

    // Get relevant sink configuration
    boolean allowSchemaRelaxation = sinkConfig.isAllowSchemaRelaxation();
    Operation operation = sinkConfig.getOperation();

    // Check if both datasets are in the same Location. If not, the direct copy operation cannot be performed.
    DatasetId sourceDatasetId = DatasetId.of(sourceTableId.getProject(), sourceTableId.getDataset());
    DatasetId destinationDatasetId = DatasetId.of(destinationProject, destinationDataset);
    Dataset srcDataset = bigQuery.getDataset(sourceDatasetId);
    Dataset destDataset = bigQuery.getDataset(destinationDatasetId);

    // Ensure datasets exist before proceeding
    if (srcDataset == null || destDataset == null) {
      LOG.warn("Direct table copy is not supported when the datasets are not created.");
      return SQLWriteResult.unsupported(datasetName);
    }

    // Ensore both datasets are in the same location.
    if (!Objects.equals(srcDataset.getLocation(), destDataset.getLocation())) {
      LOG.warn("Direct table copy is only supported if both datasets are in the same location. "
                 + "'{}' is '{}' , '{}' is '{}' .",
               sourceDatasetId.getDataset(), srcDataset.getLocation(),
               destinationDatasetId.getDataset(), destDataset.getLocation());
      return SQLWriteResult.unsupported(datasetName);
    }

    // Inserts with Truncate are not supported by the Direct write operation
    if (sinkConfig.isTruncateTableSet() && operation == Operation.INSERT) {
      LOG.warn("Direct table copy is not supported for the INSERT operation when Truncate Table is enabled.");
      return SQLWriteResult.unsupported(datasetName);
    }

    // Get source table instance
    Table srcTable = bigQuery.getTable(sourceTableId);

    // Get destination table instance
    Table destTable = bigQuery.getTable(destinationTableId);

    // If the operation is an UPSERT and the table doesn't exist, update the operation to become an Insert.
    if (destTable == null && operation == Operation.UPSERT) {
      operation = Operation.INSERT;
    }

    // If the table exists, verify if the schema needs to be related before executing the load operation.
    // Otherwise, create the destination table based on the configured schema.
    if (destTable != null) {
      LOG.info("Destinaton table `{}.{}.{}` already exists.",
               destinationTableId.getProject(), destinationTableId.getDataset(), destinationTableId.getTable());
      // Relax schema if the table exists
      if (allowSchemaRelaxation) {
        relaxTableSchema(schema, destTable);
      }
    } else {
      createTable(schema, destinationTableId, sinkConfig, newDestinationTable);
    }

    // Get query job configuration based on wether the job is an insert or update/upsert
    QueryJobConfiguration.Builder queryConfigBuilder;

    if (operation == Operation.INSERT) {
      queryConfigBuilder = getInsertQueryJobBuilder(sourceTableId, destinationTableId, fields);
    } else {
      queryConfigBuilder = getUpdateUpsertQueryJobBuilder(sourceTableId, destinationTableId, fields, sinkConfig);
    }

    QueryJobConfiguration queryConfig = queryConfigBuilder.build();
    // Create a job ID so that we can safely retry.
    JobId bqJobId = JobId.newBuilder()
      .setJob(jobId)
      .setLocation(srcDataset.getLocation())
      .setProject(sqlEngineConfig.getProject())
      .build();
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(bqJobId).build());
    TableResult result = null;

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();
    JobStatistics.QueryStatistics queryJobStats = queryJob.getStatistics();

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      BigQuerySQLEngineUtils.logJobMetrics(queryJob, metrics);
      LOG.error("Error executing BigQuery Job: '{}' in Project '{}', Dataset '{}': {}",
                jobId, sqlEngineConfig.getProject(), sqlEngineConfig.getDatasetProject(),
                queryJob.getStatus().getError().toString());
      return SQLWriteResult.faiure(datasetName);
    }

    // Number of rows is taken from the job statistics if available.
    // If not, we use the number of source table records.
    long numRows = queryJobStats != null && queryJobStats.getNumDmlAffectedRows() != null ?
      queryJobStats.getNumDmlAffectedRows() : srcTable.getNumRows().longValue();
    LOG.info("Executed copy operation for {} records from {}.{}.{} to {}.{}.{}", numRows,
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             destinationTableId.getProject(), destinationTableId.getDataset(), destinationTableId.getTable());
    BigQuerySQLEngineUtils.logJobMetrics(queryJob, metrics);

    return SQLWriteResult.success(datasetName, numRows);
  }


  /**
   * Relax table fields based on the supplied schema
   *
   * @param schema schema to use when relaxing
   * @param table  the destionation table to relax
   */
  protected void relaxTableSchema(Schema schema, Table table) {
    com.google.cloud.bigquery.Schema bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(schema);
    List<Field> fieldsToCopy = new ArrayList<>(bqSchema.getFields());
    List<Field> destinationTableFields = table.getDefinition().getSchema().getFields();
    BigQuerySinkUtils.relaxTableSchema(bigQuery, table, fieldsToCopy, destinationTableFields);
  }

  /**
   * Create a new BigQuery table based on the supplied schema and table identifier
   *
   * @param schema              schema to use for this table
   * @param tableId             itendifier for the new table
   * @param sinkConfig          Sink configuration used to define this table
   * @param newDestinationTable Atomic reference to this new table. Used to delete this table if the execution fails.
   */
  protected void createTable(Schema schema,
                             TableId tableId,
                             BigQuerySinkConfig sinkConfig,
                             AtomicReference<TableId> newDestinationTable) {
    com.google.cloud.bigquery.Schema bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(schema);

    // Create table definition and set schema
    StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition.newBuilder();
    tableDefinitionBuilder.setSchema(bqSchema);

    // Configure partitioning options
    switch (sinkConfig.getPartitioningType()) {
      case TIME:
        tableDefinitionBuilder.setTimePartitioning(getTimePartitioning(sinkConfig));
        break;
      case INTEGER:
        tableDefinitionBuilder.setRangePartitioning(getRangePartitioning(sinkConfig));
        break;
      case NONE:
        // no-op
        break;
    }

    // Configure clustering. We only configure clustering when the partitioning is not set to NONE.
    List<String> clusteringFields = getClusteringOrderFields(sinkConfig);
    if (PartitionType.NONE != sinkConfig.getPartitioningType() && !clusteringFields.isEmpty()) {
      tableDefinitionBuilder.setClustering(getClustering(clusteringFields));
    }

    TableInfo.Builder tableInfo = TableInfo.newBuilder(tableId, tableDefinitionBuilder.build());

    // Configure CMEK key if needed for the new table.
    if (sinkConfig.getCmekKey() != null) {
      tableInfo.setEncryptionConfiguration(getEncyptionConfiguration(sinkConfig));
    }

    if (sinkConfig.isPartitionFilterRequired()) {
      tableInfo.setRequirePartitionFilter(true);
    }

    Table table = bigQuery.create(tableInfo.build());

    if (table != null) {
      newDestinationTable.set(tableId);
    }
  }

  /**
   * Try to delete this table while handling exception
   *
   * @param table the table identified for the table we want to delete.
   */
  protected void tryDeleteTable(TableId table) {
    try {
      bigQuery.delete(table);
    } catch (BigQueryException bqe) {
      LOG.error("Unable to delete table {}.{}.{}. This may cause the pipeline to fail",
                table.getProject(), table.getDataset(), table.getTable(), bqe);
    }
  }

  protected QueryJobConfiguration.Builder getInsertQueryJobBuilder(TableId sourceTableId,
                                                                   TableId destinationTableId,
                                                                   List<String> fields) {
    String query = String.format("SELECT %s FROM `%s.%s.%s`",
                                 String.join(",", fields),
                                 sourceTableId.getProject(),
                                 sourceTableId.getDataset(),
                                 sourceTableId.getTable());
    LOG.info("Copying data from `{}.{}.{}` to `{}.{}.{}` using SQL statement: {} ",
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             destinationTableId.getProject(), destinationTableId.getDataset(), destinationTableId.getTable(),
             query);

    return QueryJobConfiguration.newBuilder(query)
      .setDestinationTable(destinationTableId)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
      .setPriority(sqlEngineConfig.getJobPriority())
      .setLabels(BigQuerySQLEngineUtils.getJobTags(BQ_PUSHDOWN_OPERATION_TAG));
  }

  protected QueryJobConfiguration.Builder getUpdateUpsertQueryJobBuilder(TableId sourceTableId,
                                                                         TableId destinationTableId,
                                                                         List<String> fields,
                                                                         BigQuerySinkConfig sinkConfig) {
    // Get table keys used for insert/upsert
    String relationTableKey = sinkConfig.getRelationTableKey();
    List<String> tableKeys = Arrays
      .stream(relationTableKey != null ? relationTableKey.split(",") : new String[0])
      .map(String::trim)
      .collect(Collectors.toList());

    // Get ordering fields used for deduplication
    String dedupeBy = sinkConfig.getDedupeBy();
    List<String> dedupeByKeys = Arrays
      .stream(dedupeBy != null ? dedupeBy.split(",") : new String[0])
      .map(String::trim)
      .collect(Collectors.toList());

    // Get partition filter
    String partitionFilter = sinkConfig.getPartitionFilter();

    String query = BigQuerySinkUtils.generateUpdateUpsertQuery(sinkConfig.getOperation(),
                                                               sourceTableId,
                                                               destinationTableId,
                                                               fields,
                                                               tableKeys,
                                                               dedupeByKeys,
                                                               partitionFilter);


    LOG.info("Copying data from `{}.{}.{}` to `{}.{}.{}` using SQL statement: {} ",
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             destinationTableId.getProject(), destinationTableId.getDataset(), destinationTableId.getTable(),
             query);

    return QueryJobConfiguration.newBuilder(query)
      .setPriority(sqlEngineConfig.getJobPriority())
      .setLabels(BigQuerySQLEngineUtils.getJobTags(BQ_PUSHDOWN_OPERATION_TAG));
  }

  /**
   * Build time partitioning configuration based on the BigQuery Sink configuration.
   *
   * @param config sink configuration to use
   * @return Time Partitioning configuration
   */
  protected TimePartitioning getTimePartitioning(BigQuerySinkConfig config) {

    // Default partitioning type is DAY if not specified
    TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(config.getTimePartitioningType());

    // Set partition field if specified
    if (config.getPartitionByField() != null) {
      timePartitioningBuilder.setField(config.getPartitionByField());
    }

    return timePartitioningBuilder.build();
  }

  /**
   * Build range partitioning configuration based on the BigQuery Sink configuration.
   *
   * @param config sink configuration to use
   * @return Range Partitioning configuration
   */
  protected RangePartitioning getRangePartitioning(BigQuerySinkConfig config) {
    // Create range partitioning instance and set range
    RangePartitioning.Builder rangePartitioningBuilder = RangePartitioning.newBuilder();
    rangePartitioningBuilder.setRange(getRangePartitioningRange(config));

    // Set partition field if specified
    if (config.getPartitionByField() != null) {
      rangePartitioningBuilder.setField(config.getPartitionByField());
    }

    return rangePartitioningBuilder.build();
  }

  /**
   * Build range used for partitioning configuration
   *
   * @param config sink configuration to use
   * @return Range configuration
   */
  protected RangePartitioning.Range getRangePartitioningRange(BigQuerySinkConfig config) {
    // Create and configure the range used for partitioning.
    RangePartitioning.Range.Builder rangeBuilder = RangePartitioning.Range.newBuilder();

    // 0 is used as a default in the BigQueryOutputFormat if this is set tu null;
    rangeBuilder.setStart(config.getRangeStart() != null ? config.getRangeStart() : 0);
    rangeBuilder.setEnd(config.getRangeEnd() != null ? config.getRangeEnd() : 0);
    rangeBuilder.setInterval(config.getRangeInterval() != null ? config.getRangeInterval() : 0);

    return rangeBuilder.build();
  }

  /**
   * Get the list of fields to use for clustering based on the supplied sink configuration
   *
   * @param config sink configuration to use
   * @return List containing all clustering order fields.
   */
  List<String> getClusteringOrderFields(BigQuerySinkConfig config) {
    String clusteringOrder = config.getClusteringOrder() != null ? config.getClusteringOrder() : "";
    return Arrays.stream(clusteringOrder.split(","))
      .map(String::trim)
      .filter(f -> !f.isEmpty())
      .collect(Collectors.toList());
  }

  /**
   * Get the clustering information for a list of clustering fields
   *
   * @param clusteringFields list of clustering fields to use
   * @return Clustering configuration
   */
  protected Clustering getClustering(List<String> clusteringFields) {
    Clustering.Builder clustering = Clustering.newBuilder();
    clustering.setFields(clusteringFields);
    return clustering.build();
  }

  /**
   * Get encryption configuration for the supplied sink configuration
   *
   * @param config sink configuration to use
   * @return Encryption configuration
   */
  protected EncryptionConfiguration getEncyptionConfiguration(BigQuerySinkConfig config) {
    return EncryptionConfiguration.newBuilder().setKmsKeyName(config.getCmekKey()).build();
  }
}
