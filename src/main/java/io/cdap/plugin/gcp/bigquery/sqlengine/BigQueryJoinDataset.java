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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQuerySQLBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * SQL Dataset that represents the result of a Join operation that is executed in BigQuery.
 */
public class BigQueryJoinDataset implements SQLDataset, BigQuerySQLDataset {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryJoinDataset.class);

  private final String datasetName;
  private final JoinDefinition joinDefinition;
  private final BigQuery bigQuery;
  private final String location;
  private final String project;
  private final String bqDataset;
  private final String bqTable;
  private final String jobId;
  private final BigQuerySQLBuilder helper;
  private Long numRows;

  private BigQueryJoinDataset(String datasetName,
                              JoinDefinition joinDefinition,
                              Map<String, String> stageToTableNameMap,
                              BigQuery bigQuery,
                              String location,
                              String project,
                              String bqDataset,
                              String bqTable,
                              String jobId) {
    this.datasetName = datasetName;
    this.joinDefinition = joinDefinition;
    this.bigQuery = bigQuery;
    this.location = location;
    this.project = project;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
    this.jobId = jobId;
    this.helper = new BigQuerySQLBuilder(this.project, this.bqDataset, stageToTableNameMap);
  }

  public static BigQueryJoinDataset getInstance(SQLJoinRequest joinRequest,
                                                Map<String, String> bqTableNamesMap,
                                                BigQuery bigQuery,
                                                String location,
                                                String project,
                                                String dataset,
                                                String runId) {

    // Get new Job ID for this push operation
    String jobId = BigQuerySQLEngineUtils.getIdentifier();

    // Build new table name for this dataset
    String table = BigQuerySQLEngineUtils.getNewTableName(runId);

    BigQueryJoinDataset instance = new BigQueryJoinDataset(joinRequest.getDatasetName(),
                                                           joinRequest.getJoinDefinition(),
                                                           bqTableNamesMap,
                                                           bigQuery,
                                                           location,
                                                           project,
                                                           dataset,
                                                           table,
                                                           jobId);
    instance.executeJoin();
    return instance;
  }

  private void executeJoin() {
    TableId destinationTable = TableId.of(project, bqDataset, bqTable);

    String query = getQuery(joinDefinition);
    LOG.info("Creating table `{}` using job: {} with SQL statement: {}", bqTable, jobId, query);

    // Run BigQuery job with generated SQL statement, store results in a new table, and set priority to BATCH
    // TODO: Make priority configurable
    QueryJobConfiguration queryConfig =
      QueryJobConfiguration.newBuilder(query)
        .setDestinationTable(destinationTable)
        .setPriority(QueryJobConfiguration.Priority.BATCH)
        .build();

    // Create a job ID so that we can safely retry.
    JobId bqJobId = JobId.newBuilder().setJob(jobId).setLocation(location).setProject(project).build();
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(bqJobId).build());

    // Wait for the query to complete.
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException ie) {
      throw new SQLEngineException("Interrupted exception when executing Join operation", ie);
    }

    // Check for errors
    if (queryJob == null) {
      throw new SQLEngineException("BigQuery job not found: " + jobId);
    } else if (queryJob.getStatus().getError() != null) {
      throw new SQLEngineException(String.format(
        "Error executing BigQuery Job: '%s' in Project '%s', Dataset '%s', Location'%s' : %s",
        jobId, project, bqDataset, location, queryJob.getStatus().getError().toString()));
    }

    LOG.info("Created BigQuery table `{}` using Job: {}", bqTable, jobId);
  }

  protected String getQuery(JoinDefinition joinDefinition) {
    if (joinDefinition.getCondition().getOp() == JoinCondition.Op.KEY_EQUALITY) {
      return helper.getFieldEqualityQuery(joinDefinition);
    } else {
      //TODO: implement
      throw new SQLEngineException("Advanced joins not currently supported");
    }
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public Schema getSchema() {
    return joinDefinition.getOutputSchema();
  }

  @Override
  public long getNumRows() {
    // Get the number of rows from BQ if not known at this time.
    if (numRows == null) {
      numRows = BigQuerySQLEngineUtils.getNumRows(bigQuery, project, bqDataset, bqTable);
    }

    return numRows;
  }

  @Override
  public String getBigQueryTableName() {
    return bqTable;
  }

  @Override
  @Nullable
  public String getGCSPath() {
    return null;
  }

  @Override
  public String getJobId() {
    return jobId;
  }
}
