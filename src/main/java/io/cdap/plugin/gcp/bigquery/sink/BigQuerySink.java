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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnector;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLEngine;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQueryWrite;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * This class <code>BigQuerySink</code> is a plugin that would allow users
 * to write <code>StructuredRecords</code> to Google Big Query.
 *
 * The plugin uses native BigQuery Output format to write data.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(BigQuerySink.NAME)
@Description("This sink writes to a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse. "
  + "Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = BigQueryConnector.NAME)})
public final class BigQuerySink extends AbstractBigQuerySink {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
  private static final Gson GSON = new Gson();

  public static final String NAME = "BigQueryTable";

  private final BigQuerySinkConfig config;

  private final String jobId = UUID.randomUUID().toString();

  public BigQuerySink(BigQuerySinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    Schema inputSchema = configurer.getInputSchema();
    Schema configuredSchema = config.getSchema(collector);

    config.validate(inputSchema, configuredSchema, collector, Collections.emptyMap());

    if (config.connection == null || config.tryGetProject() == null || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.connection.autoServiceAccountUnavailable())) {
      return;
    }
    // validate schema with underlying table
    Schema schema = configuredSchema == null ? inputSchema : configuredSchema;
    if (schema != null) {
      validateConfiguredSchema(schema, collector);
    }

    if (config.getJsonStringFields() != null && schema != null) {
      validateJsonStringFields(schema, config.getJsonStringFields() , collector);
    }
  }

  @Override
  protected BigQuerySinkConfig getConfig() {
    return config;
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(context.getInputSchema(), config.getSchema(collector), collector, context.getArguments().asMap());
    collector.getOrThrowException();
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery, String bucket) throws IOException {
    FailureCollector collector = context.getFailureCollector();

    Schema configSchema = config.getSchema(collector);
    Schema outputSchema = configSchema == null ? context.getInputSchema() : configSchema;

    configureTable(outputSchema);
    configureBigQuerySink();
    Table table = BigQueryUtil.getBigQueryTable(config.getDatasetProject(), config.getDataset(), config.getTable(),
            config.getServiceAccount(), config.isServiceAccountFilePath(),
            collector);
    initOutput(context, bigQuery, config.getReferenceName(),
               BigQueryUtil.getFQN(config.getDatasetProject(), config.getDataset(), config.getTable()),
               config.getTable(), outputSchema, bucket, collector, null, table);
    initSQLEngineOutput(context, bigQuery, config.getReferenceName(), context.getStageName(), config.getTable(),
                        outputSchema, collector);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    super.onRunFinish(succeeded, context);

    try {
      recordMetric(succeeded, context);
    } catch (Exception exception) {
      LOG.warn("Exception while trying to emit metric. No metric will be emitted for the number of affected rows.",
               exception);
    }
  }

  /**
   * Initialize output for SQL Engine
   * @param context Sink context
   * @param bigQuery BigQuery client
   * @param outputName Name for this output.
   * @param stageName Name for this stage. This is used to collect metrics from the SQL engine execution.
   * @param tableName Destination Table name
   * @param tableSchema Destination Table Schema
   * @param collector Failure Collector
   */
  void initSQLEngineOutput(BatchSinkContext context,
                           BigQuery bigQuery,
                           String outputName,
                           String stageName,
                           String tableName,
                           @Nullable Schema tableSchema,
                           FailureCollector collector) {
    // Sink Pushdown is not supported if the sink schema is not defined.
    if (tableSchema == null) {
      LOG.debug("BigQuery SQL Engine Output was not initialized. Schema was empty.");
      return;
    }

    List<BigQueryTableFieldSchema> fields = BigQuerySinkUtils.getBigQueryTableFields(bigQuery, tableName, tableSchema,
      getConfig().isAllowSchemaRelaxation(),
      config.getDatasetProject(), config.getDataset(), config.isTruncateTableSet(), collector);

    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());

    // Add output for SQL Engine Direct copy
    ImmutableMap.Builder<String, String> arguments = new ImmutableMap.Builder<>();

    arguments
      .put(BigQueryWrite.SQL_OUTPUT_JOB_ID, jobId + "_write")
      .put(BigQueryWrite.SQL_OUTPUT_CONFIG, GSON.toJson(config))
      .put(BigQueryWrite.SQL_OUTPUT_SCHEMA, GSON.toJson(tableSchema))
      .put(BigQueryWrite.SQL_OUTPUT_FIELDS, GSON.toJson(fieldNames));

    context.addOutput(new SQLEngineOutput(outputName,
                                          stageName,
                                          BigQuerySQLEngine.class.getName(),
                                          arguments.build()));
  }

  void recordMetric(boolean succeeded, BatchSinkContext context) {
    if (!succeeded) {
      return;
    }
    JobId bqJobId = getJobId();
    Job queryJob =  bqJobId != null ? bigQuery.getJob(bqJobId) : null;
    if (queryJob == null) {
      LOG.warn("Unable to find BigQuery job. No metric will be emitted for the number of affected rows.");
      return;
    }
    long totalRows = getTotalRows(queryJob);
    LOG.info("Job {} affected {} rows", queryJob.getJobId(), totalRows);
    //work around since StageMetrics count() only takes int as of now
    int cap = 10000; // so the loop will not cause significant delays
    long count = totalRows / Integer.MAX_VALUE;
    if (count > cap) {
      LOG.warn("Total record count is too high! Metric for the number of affected rows may not be updated correctly");
    }
    count = count < cap ? count : cap;
    for (int i = 0; i <= count && totalRows > 0; i++) {
      int rowCount = totalRows < Integer.MAX_VALUE ? (int) totalRows : Integer.MAX_VALUE;
      context.getMetrics().count(RECORDS_UPDATED_METRIC, rowCount);
      totalRows -= rowCount;
    }

    Map<String, String> tags = new ImmutableMap.Builder<String, String>()
        .put(Constants.Metrics.Tag.APP_ENTITY_TYPE, BatchSink.PLUGIN_TYPE)
        .put(Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME, BigQuerySink.NAME)
        .build();
    long totalBytes = getTotalBytes(queryJob);
    context.getMetrics().child(tags).countLong(BigQuerySinkUtils.BYTES_PROCESSED_METRIC, totalBytes);
  }

  @Nullable
  private JobId getJobId() {
    // Check if the dataset exists
    BigQuerySinkConfig config = getConfig();
    DatasetId datasetId = DatasetId.of(config.getDatasetProject(), config.getDataset());
    Dataset dataset = bigQuery.getDataset(datasetId);
    if (dataset == null) {
      LOG.warn("Dataset {} was not found in project {}", config.getDataset(), config.getDatasetProject());
      return null;
    }

    // Get dataset location
    String location = dataset.getLocation();

    // Check if the job exists in the desired location
    JobId id = JobId.newBuilder().setLocation(location).setJob(jobId).build();
    Job job = bigQuery.getJob(id);
    if (job == null) {
      LOG.warn("Job {} was not found in location {}", jobId, location);
      return null;
    }

    // Return job ID
    return id;
  }

  private long getTotalRows(Job queryJob) {
    JobConfiguration.Type type = queryJob.getConfiguration().getType();
    if (type == JobConfiguration.Type.LOAD) {
      return ((JobStatistics.LoadStatistics) queryJob.getStatistics()).getOutputRows();
    } else if (type == JobConfiguration.Type.QUERY) {
      return ((JobStatistics.QueryStatistics) queryJob.getStatistics()).getNumDmlAffectedRows();
    }
    LOG.warn("Unable to identify BigQuery job type. No metric will be emitted for the number of affected rows.");
    return 0;
  }

  private long getTotalBytes(Job queryJob) {
    JobConfiguration.Type type = queryJob.getConfiguration().getType();
    if (type == JobConfiguration.Type.LOAD) {
      long outputBytes = ((JobStatistics.LoadStatistics) queryJob.getStatistics()).getOutputBytes();
      LOG.info("Job {} loaded {} bytes", queryJob.getJobId(), outputBytes);
      return outputBytes;
    } else if (type == JobConfiguration.Type.QUERY) {
      long processedBytes = ((JobStatistics.QueryStatistics) queryJob.getStatistics()).getTotalBytesProcessed();
      LOG.info("Job {} processed {} bytes", queryJob.getJobId(), processedBytes);
      return processedBytes;
    }
    // BQ Sink triggers jobs of type query and load so this code should not be executed.
    LOG.warn("Unable to identify BigQuery job type. No metric will be emitted for the number of affected bytes.");
    return 0;
  }

  @Override
  protected OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                         String tableName,
                                                         Schema tableSchema) {
    return new BigQueryOutputFormatProvider(configuration, tableSchema);
  }

  /**
   * Sets addition configuration for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureBigQuerySink() {
    baseConfiguration.set(BigQueryConstants.CONFIG_JOB_ID, jobId);
    if (config.getPartitionByField() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_PARTITION_BY_FIELD, getConfig().getPartitionByField());
    }
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_REQUIRE_PARTITION_FILTER,
                                 getConfig().isPartitionFilterRequired());
    if (config.getClusteringOrder() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_CLUSTERING_ORDER, getConfig().getClusteringOrder());
    }
    baseConfiguration.set(BigQueryConstants.CONFIG_OPERATION, getConfig().getOperation().name());
    if (config.getRelationTableKey() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_TABLE_KEY, getConfig().getRelationTableKey());
    }
    if (config.getDedupeBy() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_DEDUPE_BY, getConfig().getDedupeBy());
    }
    if (config.getPartitionFilter() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_PARTITION_FILTER, getConfig().getPartitionFilter());
    }

    PartitionType partitioningType = getConfig().getPartitioningType();
    baseConfiguration.setEnum(BigQueryConstants.CONFIG_PARTITION_TYPE, partitioningType);

    TimePartitioning.Type timePartitioningType = getConfig().getTimePartitioningType();
    baseConfiguration.setEnum(BigQueryConstants.CONFIG_TIME_PARTITIONING_TYPE, timePartitioningType);

    if (config.getRangeStart() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_START, config.getRangeStart());
    }
    if (config.getRangeEnd() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_END, config.getRangeEnd());
    }
    if (config.getRangeInterval() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_INTERVAL, config.getRangeInterval());
    }
  }

  /**
   * Sets the output table for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureTable(Schema schema) {
    AbstractBigQuerySinkConfig config = getConfig();
    Table table = BigQueryUtil.getBigQueryTable(config.getDatasetProject(), config.getDataset(),
                                                config.getTable(),
                                                config.getServiceAccount(),
                                                config.isServiceAccountFilePath());
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_DESTINATION_TABLE_EXISTS, table != null);
    List<String> tableFieldsNames = null;
    if (table != null) {
       tableFieldsNames = Objects.requireNonNull(table.getDefinition().getSchema()).getFields().stream()
        .map(Field::getName).collect(Collectors.toList());
    } else if (schema != null) {
      tableFieldsNames = schema.getFields().stream()
        .map(Schema.Field::getName).collect(Collectors.toList());
    }
    if (tableFieldsNames != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_TABLE_FIELDS, String.join(",", tableFieldsNames));
    }
  }

  private void validateConfiguredSchema(Schema schema, FailureCollector collector) {
    if (!config.shouldConnect()) {
      return;
    }

    // Check that nested records do not go pass max depth
    validateRecordDepth(schema, collector);

    String tableName = config.getTable();
    Table table = BigQueryUtil.getBigQueryTable(config.getDatasetProject(), config.getDataset(), tableName,
                                                config.getServiceAccount(), config.isServiceAccountFilePath(),
                                                collector);

    if (table != null && !config.containsMacro(AbstractBigQuerySinkConfig.NAME_UPDATE_SCHEMA)) {
      // if table already exists, validate schema against underlying bigquery table
      com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();

      if (config.getOperation().equals(Operation.INSERT)) {
        BigQuerySinkUtils.validateInsertSchema(table, schema, config.allowSchemaRelaxation,
          config.isTruncateTableSet(), config.getDataset(), collector);
      } else if (config.getOperation().equals(Operation.UPSERT)) {
        BigQuerySinkUtils.validateSchema(tableName, bqSchema, schema, config.allowSchemaRelaxation,
          config.isTruncateTableSet(), config.getDataset(), collector);
      }
    }
  }
}
