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

/*
 * Readright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a read of
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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLReadRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLReadResult;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * SQL Pull Dataset implementation for BigQuery backed datasets.
 */
public class BigQueryReadDataset implements SQLDataset, BigQuerySQLDataset {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryReadDataset.class);
  private static final Gson GSON = new Gson();

  public static final String SQL_INPUT_CONFIG = "config";
  public static final String SQL_INPUT_FIELDS = "fields";
  public static final String SQL_INPUT_SCHEMA = "schema";
  private static final Type LIST_OF_STRINGS_TYPE = new TypeToken<ArrayList<String>>() {
  }.getType();

  private final BigQuerySQLEngineConfig sqlEngineConfig;
  private final BigQuery bigQuery;
  private final String datasetName;
  private final SQLReadRequest readRequest;
  private final TableId destinationTableId;
  private final String jobId;
  private Schema schema;
  private Long numRows;

  private BigQueryReadDataset(String datasetName,
                              BigQuerySQLEngineConfig sqlEngineConfig,
                              BigQuery bigQuery,
                              SQLReadRequest readRequest,
                              TableId destinationTableId,
                              String jobId) {
    this.datasetName = datasetName;
    this.sqlEngineConfig = sqlEngineConfig;
    this.bigQuery = bigQuery;
    this.readRequest = readRequest;
    this.destinationTableId = destinationTableId;
    this.jobId = jobId;
  }

  public static BigQueryReadDataset getInstance(String datasetName,
                                                BigQuerySQLEngineConfig sqlEngineConfig,
                                                BigQuery bigQuery,
                                                SQLReadRequest readRequest,
                                                TableId destinationTableId) {
    // Get new Job ID for this push operation
    String jobId = BigQuerySQLEngineUtils.newIdentifier();

    return new BigQueryReadDataset(datasetName,
                                   sqlEngineConfig,
                                   bigQuery,
                                   readRequest,
                                   destinationTableId,
                                   jobId);
  }

  public SQLReadResult read() {
    SQLReadResult result = null;
    // We use this atomic reference to delete a new table if it was created for this execution.
    AtomicReference<TableId> newSourceTable = new AtomicReference<>(null);
    try {
      return readInternal(readRequest, newSourceTable);
    } catch (InterruptedException e) {
      LOG.error("Interrupted exception during BigQuery read operation.", e);
    } catch (BigQueryException bqe) {
      LOG.error("BigQuery exception during BigQuery read operation", bqe);
    } catch (Exception e) {
      LOG.error("Exception during BigQuery read operation", e);
    }

    // If a new table was created for this execution, but the execution failed for any reason,
    // delete the created table so the standard sink workflow can succeed.
    if (result == null || !result.isSuccessful()) {
      tryDeleteTable(destinationTableId);
    }

    // Return as a failure if the operation threw an exception.
    return SQLReadResult.faiure(readRequest.getDatasetName());
  }

  private SQLReadResult readInternal(SQLReadRequest readRequest,
                                     AtomicReference<TableId> newSourceTable)
    throws BigQueryException, InterruptedException {
    // Check if this output matches the expected engine.
    String datasetName = readRequest.getDatasetName();
    if (!BigQuerySQLEngine.class.getName().equals(readRequest.getInput().getSqlEngineClassName())) {
      LOG.debug("Got output for another SQL engine {}, skipping", readRequest.getInput().getSqlEngineClassName());
      return SQLReadResult.unsupported(datasetName);
    }

    // Get configuration properties from read request arguments
    Map<String, String> arguments = readRequest.getInput().getArguments();
    BigQuerySourceConfig sourceConfig = GSON.fromJson(arguments.get(SQL_INPUT_CONFIG), BigQuerySourceConfig.class);
    schema = GSON.fromJson(arguments.get(SQL_INPUT_SCHEMA), Schema.class);
    List<String> fields = GSON.fromJson(arguments.get(SQL_INPUT_FIELDS), LIST_OF_STRINGS_TYPE);

    // Get source table information
    String sourceProject = sourceConfig.getDatasetProject();
    String sourceDataset = sourceConfig.getDataset();
    String sourceTableName = sourceConfig.getTable();
    TableId sourceTableId = TableId.of(sourceProject, sourceDataset, sourceTableName);

    // Check if both datasets are in the same Location. If not, the direct read operation cannot be performed.
    DatasetId sourceDatasetId = DatasetId.of(sourceTableId.getProject(), sourceTableId.getDataset());
    DatasetId destinationDatasetId = DatasetId.of(destinationTableId.getProject(), destinationTableId.getDataset());
    Dataset srcDataset = bigQuery.getDataset(sourceDatasetId);
    Dataset destDataset = bigQuery.getDataset(destinationDatasetId);

    // Ensure datasets exist before proceeding
    if (srcDataset == null || destDataset == null) {
      LOG.warn("Direct table read is not supported when the datasets are not created.");
      return SQLReadResult.unsupported(datasetName);
    }

    // Ensore both datasets are in the same location.
    if (!Objects.equals(srcDataset.getLocation(), destDataset.getLocation())) {
      LOG.warn("Direct table read is only supported if both datasets are in the same location. "
                 + "'{}' is '{}' , '{}' is '{}' .",
               sourceDatasetId.getDataset(), srcDataset.getLocation(),
               sourceDatasetId.getDataset(), destDataset.getLocation());
      return SQLReadResult.unsupported(datasetName);
    }

    // Inserts with Truncate are not supported by the Direct read operation
    if (sourceConfig.getFilter() != null ||
      sourceConfig.getPartitionFrom() != null ||
      sourceConfig.getPartitionTo() != null) {
      LOG.warn("Direct table read is not supported for filtered or partitioned tables.");
      return SQLReadResult.unsupported(datasetName);
    }

    // Get source table instance
    Table srcTable = bigQuery.getTable(sourceTableId);

    // Get source table instance
    Table destTable = bigQuery.getTable(sourceTableId);

    // Get query job configuration based on wether the job is an insert or update/upsert
    QueryJobConfiguration.Builder queryConfigBuilder = getQueryBuilder(sourceTableId, destinationTableId, fields);

    QueryJobConfiguration queryConfig = queryConfigBuilder.build();
    // Create a job ID so that we can safely retry.
    JobId bqJobId = JobId.newBuilder()
      .setJob(jobId)
      .setLocation(sqlEngineConfig.getLocation())
      .setProject(sqlEngineConfig.getProject())
      .build();
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(bqJobId).build());
    TableResult result = null;

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();
    JobStatistics.QueryStatistics queryJobStats = queryJob.getStatistics();

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      BigQuerySQLEngineUtils.logJobMetrics(queryJob);
      LOG.error("Error executing BigQuery Job: '{}' in Project '{}', Dataset '{}': {}",
                jobId, sqlEngineConfig.getProject(), sqlEngineConfig.getDatasetProject(),
                queryJob.getStatus().getError().toString());
      return SQLReadResult.faiure(datasetName);
    }

    // Number of rows is taken from the job statistics if available.
    // If not, we use the number of source table records.
    long numRows = queryJobStats != null && queryJobStats.getNumDmlAffectedRows() != null ?
      queryJobStats.getNumDmlAffectedRows() : srcTable.getNumRows().longValue();
    LOG.info("Executed read operation for {} records from {}.{}.{} into {}.{}.{}", numRows,
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable());
    BigQuerySQLEngineUtils.logJobMetrics(queryJob);

    return SQLReadResult.success(datasetName, this);
  }

  protected QueryJobConfiguration.Builder getQueryBuilder(TableId sourceTableId,
                                                          TableId destinationTableId,
                                                          List<String> fields) {
    String query = String.format("SELECT %s FROM `%s.%s.%s`",
                                 String.join(",", fields),
                                 sourceTableId.getProject(),
                                 sourceTableId.getDataset(),
                                 sourceTableId.getTable());
    LOG.info("Reading data from `{}.{}.{}` to `{}.{}.{}` using SQL statement: {} ",
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             sourceTableId.getProject(), sourceTableId.getDataset(), sourceTableId.getTable(),
             query);

    return QueryJobConfiguration.newBuilder(query)
      .setDestinationTable(destinationTableId)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
      .setPriority(sqlEngineConfig.getJobPriority())
      .setLabels(BigQuerySQLEngineUtils.getJobTags("read"));
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

  @Override
  public String getBigQueryProject() {
    return destinationTableId.getProject();
  }

  @Override
  public String getBigQueryDataset() {
    return destinationTableId.getDataset();
  }

  @Override
  public String getBigQueryTable() {
    return destinationTableId.getTable();
  }

  @Nullable
  @Override
  public String getJobId() {
    return jobId;
  }

  @Nullable
  @Override
  public String getGCSPath() {
    return null;
  }

  @Override
  public long getNumRows() {
    // Get the number of rows from BQ if not known at this time.
    if (numRows == null) {
      numRows = BigQuerySQLEngineUtils.getNumRows(bigQuery,
                                                  DatasetId.of(destinationTableId.getProject(),
                                                               destinationTableId.getDataset()),
                                                  destinationTableId.getTable());
    }

    return numRows;
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
