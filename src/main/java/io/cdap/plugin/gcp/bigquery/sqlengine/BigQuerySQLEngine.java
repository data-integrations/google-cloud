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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.capability.DefaultPullCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.PullCapability;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLRelationDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteResult;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnector;
import io.cdap.plugin.gcp.bigquery.relational.BigQueryRelation;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpressionFactory;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceUtils;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryJoinSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * SQL Engine implementation using BigQuery as the execution engine.
 */
@Plugin(type = BatchSQLEngine.PLUGIN_TYPE)
@Name(BigQuerySQLEngine.NAME)
@Description("BigQuery SQLEngine implementation, used to push down certain pipeline steps into BigQuery. "
  + "A GCS bucket is used as staging for the read/write operations performed by this engine. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = BigQueryConnector.NAME)})
public class BigQuerySQLEngine
  extends BatchSQLEngine<LongWritable, GenericData.Record, StructuredRecord, NullWritable>
  implements Engine {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLEngine.class);

  public static final String NAME = "BigQueryPushdownEngine";

  private final BigQuerySQLEngineConfig sqlEngineConfig;
  private BigQuery bigQuery;
  private Storage storage;
  private Configuration configuration;
  private String project;
  private String datasetProject;
  private String dataset;
  private String bucket;
  private String runId;
  private Map<String, String> tableNames;
  private Map<String, BigQuerySQLDataset> datasets;

  @SuppressWarnings("unused")
  public BigQuerySQLEngine(BigQuerySQLEngineConfig sqlEngineConfig) {
    this.sqlEngineConfig = sqlEngineConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    // Validate configuration and throw exception if the supplied configuration is invalid.
    sqlEngineConfig.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void prepareRun(RuntimeContext context) throws Exception {
    super.prepareRun(context);

    // Validate configuration and throw exception if the supplied configuration is invalid.
    sqlEngineConfig.validate();

    runId = BigQuerySQLEngineUtils.newIdentifier();
    tableNames = new HashMap<>();
    datasets = new HashMap<>();

    String serviceAccount = sqlEngineConfig.getServiceAccount();
    Credentials credentials = serviceAccount == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccount, sqlEngineConfig.isServiceAccountFilePath());
    project = sqlEngineConfig.getProject();
    datasetProject = sqlEngineConfig.getDatasetProject();
    dataset = sqlEngineConfig.getDataset();
    bucket = sqlEngineConfig.getBucket() != null ? sqlEngineConfig.getBucket() : "bqpushdown-" + runId;

    // Initialize BQ and GCS clients.
    bigQuery = GCPUtils.getBigQuery(project, credentials);
    storage = GCPUtils.getStorage(project, credentials);

    String cmekKey = !Strings.isNullOrEmpty(sqlEngineConfig.cmekKey) ? sqlEngineConfig.cmekKey :
      context.getRuntimeArguments().get(CmekUtils.CMEK_KEY);
    CryptoKeyName cmekKeyName = null;
    if (!Strings.isNullOrEmpty(cmekKey)) {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    }
    configuration = BigQueryUtil.getBigQueryConfig(sqlEngineConfig.getServiceAccount(), sqlEngineConfig.getProject(),
                                                   cmekKeyName, sqlEngineConfig.getServiceAccountType());
    // Create resources needed for this execution
    BigQuerySinkUtils.createResources(bigQuery, storage, DatasetId.of(datasetProject, dataset), bucket,
                                      sqlEngineConfig.getLocation(), cmekKeyName);
    // Configure GCS bucket that is used to stage temporary files.
    // If the bucket is created for this run, mar it for deletion after executon is completed
    BigQuerySinkUtils.configureBucket(configuration, bucket, runId, sqlEngineConfig.getBucket() == null);

    // Configure credentials for the source
    BigQuerySourceUtils.configureServiceAccount(configuration, sqlEngineConfig.connection);
  }

  @Override
  public void onRunFinish(boolean succeeded, RuntimeContext context) {
    super.onRunFinish(succeeded, context);

    String gcsPath;
    // If the bucket was created for this run, we should delete it.
    // Otherwise, just clean the directory within the provided bucket.
    if (sqlEngineConfig.getBucket() == null) {
      gcsPath = String.format("gs://%s", bucket);
    } else {
      gcsPath = String.format(BigQuerySinkUtils.GS_PATH_FORMAT, bucket, runId);
    }
    try {
      BigQueryUtil.deleteTemporaryDirectory(configuration, gcsPath);
    } catch (IOException e) {
      LOG.warn("Failed to delete temporary directory '{}': {}", gcsPath, e.getMessage());
    }
  }

  @Override
  public SQLPushDataset<StructuredRecord, StructuredRecord, NullWritable> getPushProvider(SQLPushRequest sqlPushRequest)
    throws SQLEngineException {
    try {
      BigQueryPushDataset pushDataset =
        BigQueryPushDataset.getInstance(sqlPushRequest,
                                        sqlEngineConfig,
                                        configuration,
                                        bigQuery,
                                        DatasetId.of(datasetProject, dataset),
                                        bucket,
                                        runId);

      LOG.info("Executing Push operation for dataset {} stored in table {}",
               sqlPushRequest.getDatasetName(),
               pushDataset.getBigQueryTable());

      datasets.put(sqlPushRequest.getDatasetName(), pushDataset);
      return pushDataset;
    } catch (IOException ioe) {
      throw new SQLEngineException(ioe);
    }
  }

  @Override
  public SQLPullDataset<StructuredRecord, LongWritable, GenericData.Record> getPullProvider(
    SQLPullRequest sqlPullRequest) throws SQLEngineException {
    if (!datasets.containsKey(sqlPullRequest.getDatasetName())) {
      throw new SQLEngineException(String.format("Trying to pull non-existing dataset: '%s",
                                                 sqlPullRequest.getDatasetName()));
    }

    String table = datasets.get(sqlPullRequest.getDatasetName()).getBigQueryTable();

    LOG.info("Executing Pull operation for dataset {} stored in table {}", sqlPullRequest.getDatasetName(), table);

    try {
      return BigQueryPullDataset.getInstance(sqlPullRequest,
                                             configuration,
                                             bigQuery,
                                             DatasetId.of(datasetProject, dataset),
                                             table,
                                             bucket,
                                             runId);
    } catch (IOException ioe) {
      throw new SQLEngineException(ioe);
    }
  }

  @Override
  public boolean exists(String datasetName) throws SQLEngineException {
    return datasets.containsKey(datasetName);
  }

  @Override
  public boolean canJoin(SQLJoinDefinition sqlJoinDefinition) {
    boolean canJoin = isValidJoinDefinition(sqlJoinDefinition);
    LOG.info("Validating join for stage '{}' can be executed on BigQuery: {}",
             sqlJoinDefinition.getDatasetName(),
             canJoin);
    return canJoin;
  }

  @VisibleForTesting
  protected static boolean isValidJoinDefinition(SQLJoinDefinition sqlJoinDefinition) {
    List<String> validationProblems = new ArrayList<>();

    JoinDefinition joinDefinition = sqlJoinDefinition.getJoinDefinition();

    // Ensure none of the input schemas contains unsupported types or invalid stage names.
    for (JoinStage inputStage : joinDefinition.getStages()) {
      // Validate input stage schema and identifier
      BigQuerySQLEngineUtils.validateInputStage(inputStage, validationProblems);
    }

    // Ensure the output schema doesn't contain unsupported types
    BigQuerySQLEngineUtils.validateOutputSchema(joinDefinition.getOutputSchema(), validationProblems);

    // Ensure expression joins have valid aliases
    if (joinDefinition.getCondition().getOp() == JoinCondition.Op.EXPRESSION) {
      BigQuerySQLEngineUtils
        .validateOnExpressionJoinCondition((JoinCondition.OnExpression) joinDefinition.getCondition(),
                                           validationProblems);
    }

    // Validate join stages for join on keys
    if (joinDefinition.getCondition().getOp() == JoinCondition.Op.KEY_EQUALITY) {
      BigQuerySQLEngineUtils.validateJoinOnKeyStages(joinDefinition, validationProblems);
    }

    if (!validationProblems.isEmpty()) {
      LOG.warn("Join operation for stage '{}' could not be executed in BigQuery. Issues found: {}.",
               sqlJoinDefinition.getDatasetName(),
               String.join("; ", validationProblems));
    }

    return validationProblems.isEmpty();
  }


  @Override
  public SQLDataset join(SQLJoinRequest sqlJoinRequest) throws SQLEngineException {
    // Get SQL builder for this Join operation
    BigQueryJoinSQLBuilder builder = new BigQueryJoinSQLBuilder(
      sqlJoinRequest.getJoinDefinition(),
      DatasetId.of(datasetProject, dataset),
      getStageNameToBQTableNameMap());

    // Execute Select job with the supplied query.
    return executeSelect(sqlJoinRequest.getDatasetName(),
                         sqlJoinRequest.getJoinDefinition().getOutputSchema(),
                         BigQueryJobType.JOIN,
                         builder.getQuery());
  }

  @Nullable
  @Override
  public SQLDatasetProducer getProducer(SQLPullRequest pullRequest, PullCapability capability) {
    // We only support the Spark RDD pull capability if the Storage Read API is enabled.
    if (!sqlEngineConfig.shouldUseStorageReadAPI() || !(capability == DefaultPullCapability.SPARK_RDD_PULL)) {
      return null;
    }

    String table = datasets.get(pullRequest.getDatasetName()).getBigQueryTable();

    return new BigQuerySparkDatasetProducer(sqlEngineConfig,
                                            datasetProject,
                                            dataset,
                                            table,
                                            pullRequest.getDatasetSchema());
  }

  @Override
  public Set<PullCapability> getPullCapabilities() {
    // If the Storage Read API is not enabled, skip this.
    if (!sqlEngineConfig.shouldUseStorageReadAPI()) {
      return Collections.emptySet();
    }

    return Collections.singleton(DefaultPullCapability.SPARK_RDD_PULL);
  }

  @Override
  public SQLWriteResult write(SQLWriteRequest writeRequest) {
    String datasetName = writeRequest.getDatasetName();

    // Check if direct sink write is enabled. If not, skip.
    if (!sqlEngineConfig.shouldUseDirectSinkWrite()) {
      return SQLWriteResult.unsupported(datasetName);
    }

    // Check if this output matches the expected engine. If it doesn't, skip execution for this write operation.;
    if (!BigQuerySQLEngine.class.getName().equals(writeRequest.getOutput().getSqlEngineClassName())) {
      LOG.debug("Got output for another SQL engine {}, skipping", writeRequest.getOutput().getSqlEngineClassName());
      return SQLWriteResult.unsupported(datasetName);
    }

    // Get source table information (from the stage we are attempting to write into the sink)
    String sourceTable = datasets.get(writeRequest.getDatasetName()).getBigQueryTable();
    TableId sourceTableId = TableId.of(datasetProject, dataset, sourceTable);

    // Build Big Query Write instance and execute write operation.
    BigQueryWrite bigQueryWrite = BigQueryWrite.getInstance(datasetName,
                                                            sqlEngineConfig,
                                                            bigQuery,
                                                            writeRequest,
                                                            sourceTableId);
    return bigQueryWrite.write();
  }

  @Override
  public void cleanup(String datasetName) throws SQLEngineException {

    BigQuerySQLDataset bqDataset = datasets.get(datasetName);
    if (bqDataset == null) {
      return;
    }

    LOG.info("Cleaning up dataset {}", datasetName);

    SQLEngineException ex = null;

    // Cancel BQ job
    try {
      cancelJob(datasetName, bqDataset);
    } catch (BigQueryException e) {
      LOG.error("Exception when cancelling BigQuery job '{}' for stage '{}': {}",
                bqDataset.getJobId(), datasetName, e.getMessage());
      ex = new SQLEngineException(String.format("Exception when executing cleanup for stage '%s'", datasetName), e);
    }

    // Delete BQ Table
    try {
      deleteTable(datasetName, bqDataset);
    } catch (BigQueryException e) {
      LOG.error("Exception when deleting BigQuery table '{}' for stage '{}': {}",
                bqDataset.getBigQueryTable(), datasetName, e.getMessage());
      if (ex == null) {
        ex = new SQLEngineException(String.format("Exception when executing cleanup for stage '%s'", datasetName), e);
      } else {
        ex.addSuppressed(e);
      }
    }

    // Delete temporary folder
    try {
      deleteTempFolder(bqDataset);
    } catch (IOException e) {
      LOG.error("Failed to delete temporary directory '{}' for stage '{}': {}",
                bqDataset.getGCSPath(), datasetName, e.getMessage());
      if (ex == null) {
        ex = new SQLEngineException(String.format("Exception when executing cleanup for stage '%s'", datasetName), e);
      } else {
        ex.addSuppressed(e);
      }
    }

    // Throw all collected exceptions, if any.
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * @return capabilities provided by {@link BigQuerySQLEngine}, which include {@link StringExpressionFactoryType}.SQL.
   */
  @Override
  public Set<Capability> getCapabilities() {
    return Collections.singleton(StringExpressionFactoryType.SQL);
  }

  /**
   * @return the single expression factory provided by {@link BigQuerySQLEngine}, which is {@link SQLExpressionFactory}.
   */
  @Override
  public List<ExpressionFactory<?>> getExpressionFactories() {
    return Collections.singletonList(new SQLExpressionFactory());
  }

  @Override
  public Relation getRelation(SQLRelationDefinition relationDefinition) {
    // Builds set of columns to be used for this relation based on the schema. This set maintains field order
    Set<String> columnSet = new LinkedHashSet<>();
    List<Schema.Field> fields = relationDefinition.getSchema().getFields();
    if (fields != null) {
      for (Schema.Field field : fields) {
        columnSet.add(field.getName());
      }
    }
    String datasetName = relationDefinition.getDatasetName();

    return BigQueryRelation.getInstance(datasetName, columnSet);
  }

  @Override
  public Engine getRelationalEngine() {
    return this;
  }

  @Override
  public boolean supportsRelationalTranform() {
    return true;
  }

  @Override
  public boolean supportsInputSchema(Schema schema) {
    return BigQuerySQLEngineUtils.isSupportedSchema(schema);
  }

  @Override
  public boolean supportsOutputSchema(Schema schema) {
    return BigQuerySQLEngineUtils.isSupportedSchema(schema);
  }

  @Override
  public Set<String> getIncludedStageNames() {
    return sqlEngineConfig.getIncludedStages();
  }

  @Override
  public Set<String> getExcludedStageNames() {
    return sqlEngineConfig.getExcludedStages();
  }

  @Override
  public boolean canTransform(SQLTransformDefinition transformDefinition) {
    Relation relation = transformDefinition.getOutputRelation();
    return relation instanceof BigQueryRelation && relation.isValid();
  }

  @Override
  public SQLDataset transform(SQLTransformRequest context) throws SQLEngineException {
    // Get relation instance
    BigQueryRelation relation = (BigQueryRelation) context.getOutputRelation();

    // Set input datasets
    Map<String, BigQuerySQLDataset> bqDatasets = context.getInputDataSets().entrySet()
      .stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> (BigQuerySQLDataset) e.getValue()));

    // Set input datasets for relation.
    relation.setInputDatasets(bqDatasets);

    // Execute select with the generated expression.
    return executeSelect(context.getOutputDatasetName(),
                         context.getOutputSchema(),
                         BigQueryJobType.TRANSFORM,
                         relation.getSQLStatement());
  }

  private BigQuerySelectDataset executeSelect(String datasetName,
                                              Schema outputSchema,
                                              BigQueryJobType jobType,
                                              String query) {
    LOG.info("Executing {} operation for dataset {}", jobType.getType(), datasetName);

    // Get new Job ID for this push operation
    String jobId = BigQuerySQLEngineUtils.newIdentifier();

    // Build new table name for this dataset
    String table = BigQuerySQLEngineUtils.getNewTableName(runId);

    // Create empty table to store query results.
    BigQuerySQLEngineUtils.createEmptyTable(sqlEngineConfig, bigQuery, project, dataset, table);

    BigQuerySelectDataset selectDataset = BigQuerySelectDataset.getInstance(
      datasetName,
      outputSchema,
      sqlEngineConfig,
      bigQuery,
      project,
      DatasetId.of(datasetProject, dataset),
      table,
      jobId,
      jobType,
      query
    ).execute();

    datasets.put(datasetName, selectDataset);

    LOG.info("Executed {} operation for dataset {}", jobType.getType(), datasetName);
    return selectDataset;
  }

  /**
   * Get a map that contains stage names as keys and BigQuery tables as Values.
   *
   * @return map representing all stages currently pushed to BQ.
   */
  protected Map<String, String> getStageNameToBQTableNameMap() {
    return datasets.entrySet()
      .stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> e.getValue().getBigQueryTable()
      ));
  }

  /**
   * Stops the running job for the supplied dataset
   *
   * @param stageName the name of the stage in CDAP
   * @param bqDataset the BigQuery Dataset Instance
   */
  protected void cancelJob(String stageName, BigQuerySQLDataset bqDataset) throws BigQueryException {
    // Skip cancellation if tables need to be retained.
    if (sqlEngineConfig.shouldRetainTables()) {
      return;
    }

    String jobId = bqDataset.getJobId();

    // If this dataset does not specify a job ID, there's no need to cancel any job
    if (jobId == null) {
      return;
    }

    String tableName = bqDataset.getBigQueryTable();
    Job job = bigQuery.getJob(jobId);

    if (job == null) {
      return;
    }

    if (!job.cancel()) {
      LOG.error("Unable to cancel BigQuery job '{}' for table '{}' and stage '{}'", jobId, tableName, stageName);
    }
  }

  /**
   * Deletes the BigQuery table for the supplied dataset
   *
   * @param stageName the name of the stage in CDAP
   * @param bqDataset the BigQuery Dataset Instance
   */
  protected void deleteTable(String stageName, BigQuerySQLDataset bqDataset) throws BigQueryException {
    // Skip deletion if tables need to be retained.
    if (sqlEngineConfig.shouldRetainTables()) {
      return;
    }

    String tableName = bqDataset.getBigQueryTable();
    TableId tableId = TableId.of(datasetProject, dataset, tableName);

    if (!bigQuery.delete(tableId)) {
      LOG.error("Unable to delete BigQuery table '{}' for stage '{}'", tableName, stageName);
    }
  }

  /**
   * Deletes the temporary folder used by a certain BQ dataset.
   *
   * @param bqDataset the BigQuery Dataset Instance
   */
  protected void deleteTempFolder(BigQuerySQLDataset bqDataset) throws IOException {
    String gcsPath = bqDataset.getGCSPath();

    // If this dataset does not use temporary storage, skip this step
    if (gcsPath == null) {
      return;
    }

    BigQueryUtil.deleteTemporaryDirectory(configuration, gcsPath);
  }

}
