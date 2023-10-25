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
import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This plugin allows users to write {@link StructuredRecord} entries to multiple Google Big Query tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(BigQueryMultiSink.NAME)
@Description("Writes records to one or more Big Query tables. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse. "
  + "Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = BigQueryConnector.NAME)})
public class BigQueryMultiSink extends AbstractBigQuerySink {
  public static final String NAME = "BigQueryMultiTable";
  private static final String TABLE_PREFIX = "multisink.";
  private static final String OUTPUT_PATTERN = "[A-Za-z0-9_-]+";
  private final BigQueryMultiSinkConfig config;

  public BigQueryMultiSink(BigQueryMultiSinkConfig config) {
    this.config = config;
  }

  @Override
  protected BigQueryMultiSinkConfig getConfig() {
    return config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    super.configurePipeline(pipelineConfigurer);

    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    Schema inputSchema = configurer.getInputSchema();
    String jsonStringFields = config.getJsonStringFields();
    if (jsonStringFields != null && inputSchema != null) {
      validateJsonStringFields(inputSchema, jsonStringFields, collector);
    }
    collector.getOrThrowException();
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());
    collector.getOrThrowException();
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery, String bucket) throws IOException {
    baseConfiguration.set(BigQueryConstants.CONFIG_OPERATION, Operation.INSERT.name());
    Map<String, String> arguments = new HashMap<>(context.getArguments().asMap());
    FailureCollector collector = context.getFailureCollector();

    if (config.getAllowFlexibleSchema()) {
      //Configure MultiSink with support for flexible schemas.
      configureSchemalessOutput(context, bucket);
    } else {
      //Configure MultiSink with fixed schemas based on arguments.
      configureOutputSchemas(context, bigQuery, bucket, arguments, collector);
    }

    collector.getOrThrowException();
  }

  protected void configureOutputSchemas(BatchSinkContext context,
                                        BigQuery bigQuery,
                                        String bucket,
                                        Map<String, String> arguments,
                                        FailureCollector collector) {
    for (Map.Entry<String, String> argument : arguments.entrySet()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String tableName = key.substring(TABLE_PREFIX.length());
      // remove the database prefix, as BigQuery doesn't allow dots
      String[] split = tableName.split("\\.");
      if (split.length == 2) {
        tableName = split[1];
      }

      try {
        Schema configuredSchema = Schema.parseJson(argument.getValue());

        Table table = BigQueryUtil.getBigQueryTable(
          config.getDatasetProject(), config.getDataset(), tableName, config.getServiceAccount(),
          config.isServiceAccountFilePath(), collector);

        Schema tableSchema = configuredSchema;
        if (table != null) {
          // if table already exists, validate schema against underlying bigquery table and
          // override against configured schema as necessary.
          com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();

          BigQuerySinkUtils.validateSchema(tableName, bqSchema, configuredSchema, config.allowSchemaRelaxation,
            config.isTruncateTableSet(), config.getDataset(), collector);

        }
        String outputName = String.format("%s-%s", config.getReferenceName(), tableName);
        outputName = sanitizeOutputName(outputName);
        initOutput(context, bigQuery, outputName,
                   BigQueryUtil.getFQN(config.getDatasetProject(), config.getDataset(), tableName),
                   tableName, tableSchema, bucket, context.getFailureCollector(), tableName, table);
      } catch (IOException e) {
        collector.addFailure("Invalid schema: " + e.getMessage(), null);
      }
    }
  }

  protected void configureSchemalessOutput(BatchSinkContext context,
                                           String bucket) throws IOException {
    Configuration conf = getOutputConfiguration();
    String splitField = config.getSplitField();
    String projectName = config.getDatasetProject();
    String datasetName = config.getDataset();
    context.addOutput(Output.of(
      config.getReferenceName(),
      new DelegatingMultiSinkOutputFormatProvider(conf, splitField, bucket, projectName, datasetName))
    );
  }

  /**
   * This method sanitizes outputName when there is an un-allowed special character in dataset
   * For example: If we have a dataset with outputName testtable$2020, this method will sanitize it to testtable_2020
   */
  @VisibleForTesting
  String sanitizeOutputName(String outputName) {
    // Output name before regex: testtable$2020
    final Pattern compilePattern = Pattern.compile(OUTPUT_PATTERN);
    final boolean validatePattern = compilePattern.matcher(outputName).matches();
    // Output name after regex: testtable_2020
    return (!validatePattern) ? outputName.replaceAll("[^\\p{Alpha}\\p{Digit}-]+", "_") : outputName;
  }

  @Override
  protected OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                         String tableName,
                                                         Schema tableSchema) {
    return new MultiSinkOutputFormatProvider(configuration, tableName, tableSchema, config.getSplitField());
  }
}
