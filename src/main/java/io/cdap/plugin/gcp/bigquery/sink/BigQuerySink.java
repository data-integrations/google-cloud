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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

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
public final class BigQuerySink extends AbstractBigQuerySink {

  public static final String NAME = "BigQueryTable";

  private final BigQuerySinkConfig config;

  private final String jobId = UUID.randomUUID().toString();

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

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

    config.validate(inputSchema, configuredSchema, collector);

    if (config.tryGetProject() == null || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable())) {
      return;
    }
    // validate schema with underlying table
    Schema schema = configuredSchema == null ? inputSchema : configuredSchema;
    if (schema != null) {
      validateConfiguredSchema(schema, collector);
    }
  }

  @Override
  protected BigQuerySinkConfig getConfig() {
    return config;
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(context.getInputSchema(), config.getSchema(collector), collector);
    collector.getOrThrowException();
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery, String bucket) throws IOException {
    FailureCollector collector = context.getFailureCollector();

    Schema configSchema = config.getSchema(collector);
    Schema outputSchema = configSchema == null ? context.getInputSchema() : configSchema;

    configureTable(outputSchema);
    configureBigQuerySink();
    initOutput(context, bigQuery, config.getReferenceName(), config.getTable(), outputSchema, bucket, collector);
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

  void recordMetric(boolean succeeded, BatchSinkContext context) {
    if (!succeeded) {
      return;
    }
    Job queryJob = bigQuery.getJob(getJobId());
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
  }

  private JobId getJobId() {
    String location = bigQuery.getDataset(getConfig().getDataset()).getLocation();
    return JobId.newBuilder().setLocation(location).setJob(jobId).build();
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
    Table table = BigQueryUtil.getBigQueryTable(config.getProject(), config.getDatasetProject(), config.getDataset(),
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
        BigQuerySinkUtils
          .validateSchema(tableName, bqSchema, schema, config.allowSchemaRelaxation, config.isTruncateTableSet(),
            config.getDataset(), collector);
      }
    }
  }
}
