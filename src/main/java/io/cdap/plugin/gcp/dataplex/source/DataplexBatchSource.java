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

package io.cdap.plugin.gcp.dataplex.source;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.gcp.bigquery.source.BigQueryAvroToStructuredTransformer;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.dataplex.common.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.common.connection.impl.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.common.model.Entity;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Dataplex Batch Source Plugin
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DataplexBatchSource.NAME)
@Description("Dataplex Source")
public class DataplexBatchSource extends BatchSource<Object, Object, StructuredRecord> {

  public static final String NAME = "Dataplex";
  public static final String BIGQUERY_DATASET_ENTITY_TYPE = "BIGQUERY";
  public static final String STORAGE_BUCKET_ENTITY_TYPE = "STORAGE_BUCKET";

  private static String dataset;
  private static String tableId;
  private static Entity entityBean;
  private static Schema outputSchema;
  private static String datasetProject;
  private final BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
  private final DataplexBatchSourceConfig config;
  DataplexInterface dataplexInterface = new DataplexInterfaceImpl();

  public DataplexBatchSource(DataplexBatchSourceConfig dataplexBatchSourceConfig) {
    this.config = dataplexBatchSourceConfig;
  }

  @Override
  public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter) throws IOException {
    if (entityBean.getSystem().equalsIgnoreCase(BIGQUERY_DATASET_ENTITY_TYPE)) {
      StructuredRecord transformed = outputSchema == null ?
        transformer.transform((GenericData.Record) input.getValue()) :
        transformer.transform((GenericData.Record) input.getValue(), outputSchema);
      emitter.emit(transformed);
    } else {
      emitter.emit((StructuredRecord) input.getValue());
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    if (!config.getConnection().canConnect() || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
      (config.tryGetProject() == null)) {
      // ValidatingInputFormat plugin setup is mandatory.Otherwise pipeline will fail at runtime in case of GCS entity.
      config.setupValidatingInputFormat(pipelineConfigurer, collector, null);
      return;
    }
    config.validateServiceAccount(collector);
    entityBean = config.getAndValidateEntityConfiguration(collector, dataplexInterface);
    if (entityBean == null) {
      config.setupValidatingInputFormat(pipelineConfigurer, collector, null);
      return;
    }
    if (entityBean.getSystem().equals(BIGQUERY_DATASET_ENTITY_TYPE)) {
      getEntityValuesFromDataPathForBQEntities(entityBean.getDataPath());
      config.validateBigQueryDataset(collector, datasetProject, dataset, tableId);
      config.validateTable(collector, datasetProject, dataset, tableId);
      Schema configuredSchema = getOutputSchemaForBQEntity(collector);
      configurer.setOutputSchema(configuredSchema);
    } else {
      config.checkMetastoreForGCSEntity(dataplexInterface, collector);
      config.setupValidatingInputFormat(pipelineConfigurer, collector, entityBean);
    }
  }

  public void getEntityValuesFromDataPathForBQEntities(String dataPath) {
    String[] entityValues = dataPath.split("/");
    dataset = entityValues[entityValues.length - 3];
    datasetProject = entityValues[1];
    tableId = entityValues[entityValues.length - 1];
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {

  }


  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputSchema = config.getSchema(context.getFailureCollector());
  }


  @Nullable
  private Schema getOutputSchemaForBQEntity(FailureCollector collector) {
    Schema outputSchema = config.getSchema(collector);
    outputSchema = outputSchema == null ? getSchemaForBQEntity(collector) : outputSchema;
    validateConfiguredSchemaForBQEntity(outputSchema, collector);
    return outputSchema;
  }

  public Schema getSchemaForBQEntity(FailureCollector collector) {
    com.google.cloud.bigquery.Schema bqSchema = getBQSchema(collector);
    return BigQueryUtil.getTableSchema(bqSchema, collector);
  }

  private com.google.cloud.bigquery.Schema getBQSchema(FailureCollector collector) {
    String serviceAccount = config.getServiceAccount();
    Table table = BigQueryUtil.getBigQueryTable(datasetProject, dataset, tableId, serviceAccount,
      config.isServiceAccountFilePath(), collector);
    if (table == null) {
      // Table does not exist
      collector.addFailure(
          String.format("BigQuery table '%s:%s.%s' does not exist.", datasetProject, dataset, tableId),
          "Ensure correct table name is provided.")
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
      throw collector.getOrThrowException();
    }
    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null) {
      collector.addFailure(String.format("Cannot read from table '%s:%s.%s' because it has no schema.",
          datasetProject, dataset, table), "Alter the table to have a schema.")
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
      throw collector.getOrThrowException();
    }
    return bqSchema;
  }

  /**
   * Validate output schema. This is needed because its possible that output schema is set without using
   * getSchema method.
   */
  private void validateConfiguredSchemaForBQEntity(Schema configuredSchema, FailureCollector collector) {
    com.google.cloud.bigquery.Schema bqSchema = getBQSchema(collector);
    FieldList fields = bqSchema.getFields();
    // Match output schema field type with bigquery column type
    for (Schema.Field field : configuredSchema.getFields()) {
      try {
        Field bqField = fields.get(field.getName());
        ValidationFailure failure =
          BigQueryUtil.validateFieldSchemaMatches(bqField, field, dataset, tableId,
            BigQuerySourceConfig.SUPPORTED_TYPES, collector);
        if (failure != null) {
          // For configured source schema field, failure will always map to output field in configured schema.
          failure.withOutputSchemaField(field.getName());
        }
      } catch (IllegalArgumentException e) {
        // this means that the field is not present in BigQuery table.
        collector.addFailure(
            String.format("Field '%s' is not present in table '%s:%s.%s'.", field.getName(), datasetProject, dataset,
              tableId),
            String.format("Remove field '%s' from the output schema.", field.getName()))
          .withOutputSchemaField(field.getName());
      }
    }
    collector.getOrThrowException();
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {

  }

}
