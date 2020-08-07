/*
 * Copyright © 2019-2020 Cask Data, Inc.
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
import com.google.cloud.bigquery.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
  public static final String NAME = "BigQueryTable";

  private final BigQuerySinkConfig config;

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

    if (config.tryGetProject() == null || config.autoServiceAccountUnavailable()) {
      return;
    }

    // Try validations which may require connectivity
    try {
      // Validate partition properties
      config.validatePartitionProperties(inputSchema, configuredSchema, collector);
      // validate schema with underlying table
      Schema schema = configuredSchema == null ? inputSchema : configuredSchema;
      validateConfiguredSchema(schema, collector);
    } catch (InvalidStageException e) {
      // Allow deployment even if retrieving schema fails
      LOG.warn("Unable to validate BigQuery schema with error:\n", e);
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
    try {
      config.validatePartitionProperties(context.getInputSchema(), config.getSchema(collector), collector);
    } catch (InvalidConfigPropertyException e) {
      collector.addFailure(String.format("Unable to load credentials from %s.", config.getServiceAccountFilePath()),
                           "Ensure the service account file is available on the local filesystem.")
        .withConfigProperty(GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH);

    } catch (InvalidStageException e) {
      collector.addFailure("Unable to get details about the BigQuery table: " + e.getMessage(), null)
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
    }
    collector.getOrThrowException();
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery, String bucket) throws IOException {
    FailureCollector collector = context.getFailureCollector();
    Schema configSchema = config.getSchema(collector);
    configureTable();
    configureBigQuerySink();
    Schema schema = configSchema == null ? context.getInputSchema() : configSchema;
    initOutput(context, bigQuery, config.getReferenceName(), config.getTable(), schema, bucket, collector);
  }

  @Override
  protected OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                         String tableName,
                                                         Schema tableSchema) {
    return new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return BigQueryOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        return BigQueryUtil.configToMap(configuration);
      }
    };
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws IOException {
    emitter.emit(new KeyValue<>(new AvroKey<>(toAvroRecord(input)), NullWritable.get()));
  }

  /**
   * Sets addition configuration for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureBigQuerySink() {
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_CREATE_PARTITIONED_TABLE,
                                 getConfig().shouldCreatePartitionedTable());
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
  }

  /**
   * Sets the output table for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureTable() {
    AbstractBigQuerySinkConfig config = getConfig();
    Table table = BigQueryUtil.getBigQueryTable(config.getProject(), config.getDataset(),
                                                config.getTable(),
                                                config.getServiceAccountFilePath());
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_DESTINATION_TABLE_EXISTS, table != null);
    if (table != null) {
      List<String> tableFieldsNames = Objects.requireNonNull(table.getDefinition().getSchema()).getFields().stream()
        .map(Field::getName).collect(Collectors.toList());
      baseConfiguration.set(BigQueryConstants.CONFIG_TABLE_FIELDS, String.join(",", tableFieldsNames));
    }
  }

  private void validateConfiguredSchema(Schema schema, FailureCollector collector) {
    if (!config.shouldConnect()) {
      return;
    }

    Table table = BigQueryUtil.getBigQueryTable(config.getProject(), config.getDataset(), config.getTable(),
                                                config.getServiceAccountFilePath());
    if (table != null) {
      // if table already exists, validate schema against underlying bigquery table

      validateSchema(table, schema, config.allowSchemaRelaxation, collector);
    }
  }
}
