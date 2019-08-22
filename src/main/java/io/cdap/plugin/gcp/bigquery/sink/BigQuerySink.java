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
import com.google.cloud.bigquery.Table;
import com.google.gson.JsonObject;
import com.google.gson.internal.bind.JsonTreeWriter;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

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

  public static final String NAME = "BigQueryTable";

  private final BigQuerySinkConfig config;
  private Schema schema;

  public BigQuerySink(BigQuerySinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected BigQuerySinkConfig getConfig() {
    return config;
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    config.validate(context.getInputSchema());
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery, String bucket) throws IOException {
    configureTable();
    Schema configSchema = config.getSchema();
    Schema schema = configSchema == null ? context.getInputSchema() : configSchema;
    initOutput(context, bigQuery, config.getReferenceName(), config.getTable(), schema, bucket);
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
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.schema = config.getSchema();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<JsonObject, NullWritable>> emitter) {
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(input.getSchema().getFields())) {
        // From all the fields in input record, write only those fields that are present in output schema
        if (schema == null || schema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), input.get(recordField.getName()),
                                     recordField.getSchema());
        }
      }
      writer.endObject();
      emitter.emit(new KeyValue<>(writer.get().getAsJsonObject(), NullWritable.get()));
    } catch (IOException e) {
      throw new RuntimeException("Exception while converting structured record to json.", e);
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
}
