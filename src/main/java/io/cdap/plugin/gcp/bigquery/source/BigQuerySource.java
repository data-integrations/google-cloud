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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.Schemas;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Class description here.
 */
@Plugin(type = "batchsource")
@Name(BigQuerySource.NAME)
@Description("This source reads the entire contents of a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse."
  + "Data is first written to a temporary location on Google Cloud Storage, then read into the pipeline from there.")
public final class BigQuerySource extends BatchSource<LongWritable, GenericData.Record, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  public static final String NAME = "BigQueryTable";
  private BigQuerySourceConfig config;
  private Schema outputSchema;
  private Configuration configuration;
  private final BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
  // UUID for the run. Will be used as bucket name if bucket is not provided.
  private UUID uuid;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    config.validate();

    if (config.containsMacro("schema") || config.containsMacro("dataset") || config.containsMacro("table") ||
      config.containsMacro("datasetProject")) {
      configurer.getStageConfigurer().setOutputSchema(null);
      return;
    }

    Schema schema = getSchema();
    Schema configuredSchema = config.getSchema();
    if (configuredSchema == null) {
      configurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }

    try {
      Schemas.validateFieldsMatch(schema, configuredSchema);
      configurer.getStageConfigurer().setOutputSchema(configuredSchema);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigPropertyException(e.getMessage(), e, "schema");
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
    String serviceAccountPath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountPath == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccountPath);
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getDatasetProject(), credentials);
    validateOutputSchema(bigQuery);

    uuid = UUID.randomUUID();
    configuration = BigQueryUtil.getBigQueryConfig(config.getServiceAccountFilePath(), config.getProject());

    String bucket = config.getBucket();
    if (bucket == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket. So enable it only when bucket name
      // is not provided.
      configuration.setBoolean("fs.gs.bucket.delete.enable", true);
    }

    if (!context.isPreviewEnabled()) {
      BigQueryUtil.createResources(bigQuery, GCPUtils.getStorage(config.getDatasetProject(), credentials),
                                   config.getDataset(), bucket);
    }

    configuration.set("fs.gs.system.bucket", bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    String temporaryGcsPath = String.format("gs://%s/hadoop/input/%s", bucket, uuid);
    AvroBigQueryInputFormat.setTemporaryCloudStorageDirectory(configuration, temporaryGcsPath);
    AvroBigQueryInputFormat.setEnableShardedExport(configuration, false);
    BigQueryConfiguration.configureBigQueryInput(configuration, config.getDatasetProject(),
                                                 config.getDataset(), config.getTable());

    Job job = Job.getInstance(configuration);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema which .
    emitLineage(context);
    setInputFormat(context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputSchema = context.getOutputSchema();
  }

  /**
   * Converts <code>JsonObject</code> to <code>StructuredRecord</code> for every record
   * retrieved from the BigQuery table.
   *
   * @param input input record
   * @param emitter emitting the transformed record into downstream nodes.
   */
  @Override
  public void transform(KeyValue<LongWritable, GenericData.Record> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    emitter.emit(transformer.transform(input.getValue(), outputSchema));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    org.apache.hadoop.fs.Path gcsPath = new org.apache.hadoop.fs.Path(String.format("gs://%s", uuid.toString()));
    try {
      if (config.getBucket() == null) {
          FileSystem fs = gcsPath.getFileSystem(configuration);
          if (fs.exists(gcsPath)) {
            fs.delete(gcsPath, true);
          }
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete bucket " + gcsPath.toUri().getPath() + ", " + e.getMessage());
    }
  }

  public Schema getSchema() {
    String dataset = config.getDataset();
    String tableName = config.getTable();
    String project = config.getDatasetProject();
    TableId tableId = TableId.of(project, dataset, tableName);

    String serviceAccountPath = config.getServiceAccountFilePath();
    Credentials credentials = null;
    if (serviceAccountPath != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountPath);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException(
          String.format("Unable to load credentials from %s", serviceAccountPath), "serviceFilePath");
      }
    }
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getDatasetProject(), credentials);

    Table table;
    try {
      table = bigQuery.getTable(tableId);
    } catch (BigQueryException e) {
      throw new InvalidStageException("Unable to get details about the BigQuery table: " + e.getMessage(), e);
    }
    if (table == null) {
      // Table does not exist
      throw new InvalidStageException(String.format("BigQuery table '%s:%s.%s' does not exist",
                                                    project, dataset, tableName));
    }

    com.google.cloud.bigquery.Schema bgSchema = table.getDefinition().getSchema();
    if (bgSchema == null) {
      throw new InvalidStageException(String.format("Cannot read from table '%s:%s.%s' because it has no schema.",
                                                    project, dataset, table));
    }
    List<Schema.Field> fields = getSchemaFields(bgSchema);
    return Schema.recordOf("output", fields);
  }

  /**
   * Validate output schema. This is needed because its possible that output schema is set without using
   * {@link #getSchema()} method.
   */
  private void validateOutputSchema(BigQuery bigQuery) {
    String dataset = config.getDataset();
    String tableName = config.getTable();
    String project = config.getDatasetProject();
    TableId tableId = TableId.of(project, dataset, tableName);
    Table table = bigQuery.getTable(tableId);
    if (table == null) {
      // Table does not exist
      throw new IllegalArgumentException(String.format("BigQuery table '%s:%s.%s' does not exist.",
                                                       project, dataset, tableName));
    }

    com.google.cloud.bigquery.Schema bgSchema = table.getDefinition().getSchema();
    if (bgSchema == null) {
      throw new IllegalArgumentException(String.format("Cannot read from table '%s:%s.%s' because it has no schema.",
                                                       project, dataset, table));
    }

    // Output schema should not have more fields than BigQuery table
    List<String> diff = BigQueryUtil.getSchemaMinusBqFields(config.getSchema().getFields(), bgSchema.getFields());
    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(String.format("Output schema has field(s) '%s' which are not present in table"
                                                         + " '%s:%s.%s' schema.", diff, project, dataset, table));
    }

    FieldList fields = bgSchema.getFields();
    // Match output schema field type with bigquery column type
    for (Schema.Field field : config.getSchema().getFields()) {
      validateSupportedTypes(field);
      BigQueryUtil.validateFieldSchemaMatches(fields.get(field.getName()), field, dataset, tableName);
    }
  }

  private void validateSupportedTypes(Schema.Field field) {
    String name = field.getName();
    Schema fieldSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());
    Schema.Type type = fieldSchema.getType();

    // Complex types like maps and unions are not supported in BigQuery plugins.
    if (!BigQueryUtil.SUPPORTED_TYPES.contains(type)) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'.", name, type));
    }

    // bigquery does not support int types
    if (fieldSchema.getLogicalType() == null && type == Schema.Type.INT) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type 'int'." +
                                                         " It should be 'long'", name));
    }

    // bigquery does not support float types
    if (fieldSchema.getLogicalType() == null && type == Schema.Type.FLOAT) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type 'float'." +
                                                         " It should be 'double'", name));
    }
  }

  private List<Schema.Field> getSchemaFields(com.google.cloud.bigquery.Schema bgSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (Field field : bgSchema.getFields()) {
      LegacySQLTypeName type = field.getType();
      Schema schema;
      StandardSQLTypeName value = type.getStandardType();
      if (value == StandardSQLTypeName.FLOAT64) {
        // float is a float64, so corresponding type becomes double
        schema = Schema.of(Schema.Type.DOUBLE);
      } else if (value == StandardSQLTypeName.BOOL) {
        schema = Schema.of(Schema.Type.BOOLEAN);
      } else if (value == StandardSQLTypeName.INT64) {
        // int is a int64, so corresponding type becomes long
        schema = Schema.of(Schema.Type.LONG);
      } else if (value == StandardSQLTypeName.STRING || value == StandardSQLTypeName.DATETIME) {
        schema = Schema.of(Schema.Type.STRING);
      } else if (value == StandardSQLTypeName.BYTES) {
        schema = Schema.of(Schema.Type.BYTES);
      } else if (value == StandardSQLTypeName.TIME) {
        schema = Schema.of(Schema.LogicalType.TIME_MICROS);
      } else if (value == StandardSQLTypeName.DATE) {
        schema = Schema.of(Schema.LogicalType.DATE);
      } else if (value == StandardSQLTypeName.TIMESTAMP) {
        schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      } else if (value == StandardSQLTypeName.NUMERIC) {
        // bigquery has 38 digits of precision and 9 digits of scale.
        // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#logical_types
        schema = Schema.decimalOf(38, 9);
      } else {
        // this should never happen
        throw new InvalidStageException(String.format("BigQuery column '%s' is of unsupported type '%s'.",
                                                      field.getName(), value));
      }

      if (field.getMode() == null || field.getMode() == Field.Mode.NULLABLE) {
        fields.add(Schema.Field.of(field.getName(), Schema.nullableOf(schema)));
      } else if (field.getMode() == Field.Mode.REQUIRED) {
        fields.add(Schema.Field.of(field.getName(), schema));
      } else if (field.getMode() == Field.Mode.REPEATED) {
        // allow array field types
        fields.add(Schema.Field.of(field.getName(), Schema.arrayOf(schema)));
      }
    }
    return fields;
  }

  private void setInputFormat(BatchSourceContext context) {
    context.setInput(Input.of(config.referenceName, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return AvroBigQueryInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        Map<String, String> config = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
          config.put(entry.getKey(), entry.getValue());
        }
        return config;
      }
    }));
  }

  private void emitLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());

    if (config.getSchema().getFields() != null) {
      lineageRecorder.recordRead("Read", "Read from BigQuery table.",
                                 config.getSchema().getFields().stream()
                                   .map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
