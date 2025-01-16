/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineInput;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProviderSpec;
import io.cdap.cdap.etl.api.exception.ErrorPhase;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.bigquery.common.BigQueryErrorDetailsProvider;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnector;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQueryReadDataset;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLEngine;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class description here.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(BigQuerySource.NAME)
@Description("This source reads the entire contents of a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse."
  + "Data is first written to a temporary location on Google Cloud Storage, then read into the pipeline from there.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = BigQueryConnector.NAME)})
public final class BigQuerySource extends BatchSource<LongWritable, GenericData.Record, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  private static final Gson GSON = new Gson();
  public static final String NAME = "BigQueryTable";
  private BigQuerySourceConfig config;
  private Schema outputSchema;
  private Configuration configuration;
  private final BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
  // UUID for the run. Will be used as bucket name if bucket is not provided.
  private String bucketPath;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    Schema configuredSchema = config.getSchema(collector);

    // if any of the require properties have macros or the service account can't be auto-detected
    // or the dataset project isn't set and the project can't be auto-detected
    if (!config.canConnect() || (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
      (config.tryGetProject() == null && config.getDatasetProject() == null)) {
      stageConfigurer.setOutputSchema(configuredSchema);
      return;
    }

    Schema schema = getSchema(collector);
    validatePartitionProperties(collector);

    if (configuredSchema == null) {
      stageConfigurer.setOutputSchema(schema);
      return;
    }

    validateConfiguredSchema(configuredSchema, collector);
    stageConfigurer.setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());

    if (getBQSchema(collector).getFields().isEmpty()) {
      collector.addFailure(String.format("BigQuery table %s.%s does not have a schema.",
                                         config.getDataset(), config.getTable()),
                           "Please edit the table to add a schema.");
      collector.getOrThrowException();
    }

    Schema configuredSchema = getOutputSchema(collector);

    // Create BigQuery client
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = null;
    try {
      credentials = BigQuerySourceUtils.getCredentials(config.getConnection());
    } catch (Exception e) {
      String errorReason = "Unable to load service account credentials.";
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    BigQuery bigQuery;
    Dataset dataset;
    try {
      bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials, null);
      dataset = bigQuery.getDataset(DatasetId.of(config.getDatasetProject(), config.getDataset()));
    } catch (Exception e) {
      ProgramFailureException ex = new BigQueryErrorDetailsProvider().getExceptionDetails(e,
          new ErrorContext(ErrorPhase.READING));
      throw ex == null ? e : ex;
    }

    // Get Configuration for this run
    bucketPath = UUID.randomUUID().toString();
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(), collector);
    collector.getOrThrowException();
    try {
      configuration = BigQueryUtil.getBigQueryConfig(serviceAccount, config.getProject(), cmekKeyName,
        config.getServiceAccountType());
    } catch (Exception e) {
      String errorReason = "Failed to create BigQuery configuration.";
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    String bucketName = BigQueryUtil.getStagingBucketName(context.getArguments().asMap(), null,
                                                          dataset, config.getBucket());

    // Configure GCS Bucket to use
    Storage storage =  GCPUtils.getStorage(config.getProject(), credentials);;
    String bucket = null;
    try {
      bucket = BigQuerySourceUtils.getOrCreateBucket(configuration, storage, bucketName, dataset,
          bucketPath, cmekKeyName);
    } catch (Exception e) {
      String errorReason = "Failed to create bucket.";
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    // Configure Service account credentials
    BigQuerySourceUtils.configureServiceAccount(configuration, config.getConnection());

    // Configure BQ Source
    configureBigQuerySource();

    // Configure BigQuery input format.
    String temporaryGcsPath = BigQuerySourceUtils.getTemporaryGcsPath(bucket, bucketPath, bucketPath);
    try {
      BigQuerySourceUtils.configureBigQueryInput(configuration,
        DatasetId.of(config.getDatasetProject(), config.getDataset()),
        config.getTable(),
        temporaryGcsPath);
    } catch (Exception e) {
      String errorReason = "Failed to configure BigQuery input.";
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema.
    Type sourceTableType = config.getSourceTableType();
    Asset asset = Asset.builder(config.getReferenceName())
      .setFqn(BigQueryUtil.getFQN(config.getDatasetProject(), config.getDataset(), config.getTable()))
      .setLocation(dataset.getLocation())
      .build();

    // set error details provider
    context.setErrorDetailsProvider(new ErrorDetailsProviderSpec(BigQueryErrorDetailsProvider.class.getName()));

    emitLineage(context, configuredSchema, sourceTableType, config.getTable(), asset);
    setInputFormat(context, configuredSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.outputSchema = config.getSchema(context.getFailureCollector());
  }

  /**
   * Converts <code>JsonObject</code> to <code>StructuredRecord</code> for every record
   * retrieved from the BigQuery table.
   *
   * @param input   input record
   * @param emitter emitting the transformed record into downstream nodes.
   */
  @Override
  public void transform(KeyValue<LongWritable, GenericData.Record> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    StructuredRecord transformed = outputSchema == null ?
      transformer.transform(input.getValue()) : transformer.transform(input.getValue(), outputSchema);
    emitter.emit(transformed);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    BigQuerySourceUtils.deleteGcsTemporaryDirectory(configuration, config.getBucket(), bucketPath);
    BigQuerySourceUtils.deleteBigQueryTemporaryTable(configuration, config);
  }

  private void configureBigQuerySource() {
    if (config.getPartitionFrom() != null) {
      configuration.set(BigQueryConstants.CONFIG_PARTITION_FROM_DATE, config.getPartitionFrom());
    }
    if (config.getPartitionTo() != null) {
      configuration.set(BigQueryConstants.CONFIG_PARTITION_TO_DATE, config.getPartitionTo());
    }
    if (config.getFilter() != null) {
      configuration.set(BigQueryConstants.CONFIG_FILTER, config.getFilter());
    }
    if (config.getViewMaterializationProject() != null) {
      configuration.set(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_PROJECT, config.getViewMaterializationProject());
    }
    if (config.getViewMaterializationDataset() != null) {
      configuration.set(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_DATASET, config.getViewMaterializationDataset());
    }
  }

  public Schema getSchema(FailureCollector collector) {
    com.google.cloud.bigquery.Schema bqSchema = getBQSchema(collector);
    return BigQueryUtil.getTableSchema(bqSchema, collector);
  }

  /**
   * Validate output schema. This is needed because its possible that output schema is set without using
   * {@link #getSchema} method.
   */
  private void validateConfiguredSchema(Schema configuredSchema, FailureCollector collector) {
    String dataset = config.getDataset();
    String tableName = config.getTable();
    String project = config.getDatasetProject();
    com.google.cloud.bigquery.Schema bqSchema = getBQSchema(collector);

    FieldList fields = bqSchema.getFields();

    // Match output schema field type with bigquery column type
    for (Schema.Field field : configuredSchema.getFields()) {
      try {
        Field bqField = fields.get(field.getName());
        ValidationFailure failure =
          BigQueryUtil.validateFieldSchemaMatches(bqField, field, dataset, tableName,
                                                  BigQuerySourceConfig.SUPPORTED_TYPES, collector);
        if (failure != null) {
          // For configured source schema field, failure will always map to output field in configured schema.
          failure.withOutputSchemaField(field.getName());
        }
      } catch (IllegalArgumentException e) {
        // this means that the field is not present in BigQuery table.
        collector.addFailure(
          String.format("Field '%s' is not present in table '%s:%s.%s'.", field.getName(), project, dataset, tableName),
          String.format("Remove field '%s' from the output schema.", field.getName()))
          .withOutputSchemaField(field.getName());
      }
    }
    collector.getOrThrowException();
  }

  private com.google.cloud.bigquery.Schema getBQSchema(FailureCollector collector) {
    String serviceAccount = config.getServiceAccount();
    String dataset = config.getDataset();
    String tableName = config.getTable();
    String project = config.getDatasetProject();

    Table table = BigQueryUtil.getBigQueryTable(project, dataset, tableName, serviceAccount,
                                                config.isServiceAccountFilePath(), collector);
    if (table == null) {
      // Table does not exist
      collector.addFailure(String.format("BigQuery table '%s:%s.%s' does not exist.", project, dataset, tableName),
                           "Ensure correct table name is provided.")
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
      throw collector.getOrThrowException();
    }

    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null) {
      collector.addFailure(String.format("Cannot read from table '%s:%s.%s' because it has no schema.",
                                         project, dataset, table), "Alter the table to have a schema.")
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
      throw collector.getOrThrowException();
    }
    return bqSchema;
  }

  @Nullable
  private Schema getOutputSchema(FailureCollector collector) {
    Schema outputSchema = config.getSchema(collector);
    outputSchema = outputSchema == null ? getSchema(collector) : outputSchema;
    validatePartitionProperties(collector);
    validateConfiguredSchema(outputSchema, collector);
    return outputSchema;
  }

  private void validatePartitionProperties(FailureCollector collector) {
    String project = config.getDatasetProject();
    String dataset = config.getDataset();
    String tableName = config.getTable();
    Table sourceTable = BigQueryUtil.getBigQueryTable(project, dataset, tableName, config.getServiceAccount(),
                                                      config.isServiceAccountFilePath(), collector);
    if (sourceTable == null) {
      return;
    }
    if (sourceTable.getDefinition() instanceof StandardTableDefinition) {
      TimePartitioning timePartitioning = ((StandardTableDefinition) sourceTable.getDefinition()).getTimePartitioning();
      if (timePartitioning == null) {
        return;
      }
    }
    String partitionFromDate = config.getPartitionFrom();
    String partitionToDate = config.getPartitionTo();

    if (partitionFromDate == null && partitionToDate == null) {
      return;
    }
    LocalDate fromDate = null;
    if (partitionFromDate != null) {
      try {
        fromDate = LocalDate.parse(partitionFromDate);
      } catch (DateTimeException ex) {
        collector.addFailure("Invalid partition from date format.",
                             "Ensure partition from date is of format 'yyyy-MM-dd'.")
          .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_FROM);
      }
    }
    LocalDate toDate = null;
    if (partitionToDate != null) {
      try {
        toDate = LocalDate.parse(partitionToDate);
      } catch (DateTimeException ex) {
        collector.addFailure("Invalid partition to date format.", "Ensure partition to date is of format 'yyyy-MM-dd'.")
          .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_TO);
      }
    }

    if (fromDate != null && toDate != null && fromDate.isAfter(toDate) && !fromDate.isEqual(toDate)) {
      collector.addFailure("'Partition From Date' must be before or equal 'Partition To Date'.", null)
        .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_FROM)
        .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_TO);
    }
  }

  private void setInputFormat(BatchSourceContext context,
                              Schema configuredSchema) {
    // Set input for Spark
    context.setInput(Input.of(config.getReferenceName(), new BigQueryInputFormatProvider(configuration)));

    // Add output for SQL Engine Direct read
    ImmutableMap.Builder<String, String> arguments = new ImmutableMap.Builder<>();

    if (configuredSchema == null) {
      LOG.debug("BigQuery SQL Engine Input was not initialized. Schema was empty.");
      return;
    }

    List<String> fieldNames = configuredSchema.getFields().stream().map(f -> f.getName()).collect(Collectors.toList());

    arguments
      .put(BigQueryReadDataset.SQL_INPUT_CONFIG, GSON.toJson(config))
      .put(BigQueryReadDataset.SQL_INPUT_SCHEMA, GSON.toJson(configuredSchema))
      .put(BigQueryReadDataset.SQL_INPUT_FIELDS, GSON.toJson(fieldNames));

    Input sqlEngineInput = new SQLEngineInput(config.referenceName,
                                              context.getStageName(),
                                              BigQuerySQLEngine.class.getName(),
                                              arguments.build());
    context.setInput(sqlEngineInput);
  }

  private void emitLineage(BatchSourceContext context, Schema schema, Type sourceTableType,
                           String table, Asset asset) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);

    String type = "table";
    if (Type.VIEW == sourceTableType) {
      type = "view";
    } else if (Type.MATERIALIZED_VIEW == sourceTableType) {
      type = "materialized view";
    }

    if (schema.getFields() != null) {
      lineageRecorder.recordRead("Read", String.format("Read from BigQuery %s '%s'.", type, table),
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
