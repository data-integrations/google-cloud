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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
@Name("BigQueryMultiTable")
@Description("Writes to multiple big query tables. The sink will write to the correct table based " +
  "on the value of a split field. For example, if the split field is configured to be 'tablename', any record " +
  "with a 'tablename' field of 'xyz' will be written to file set 'xyz'. This plugin expects that the tables " +
  "to write to will be present in the pipeline arguments. Each table to write to must have an argument where " +
  "the key is 'multisink.[name]' and the value is the schema for that table. Most of the time, " +
  "this plugin will be used with the MultiTableDatabase source, which will set those pipeline arguments.")
public final class BigQueryMultiTableSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMultiTableSink.class);

  private final Conf config;
  // UUID for the run. Will be used as bucket name if bucket is not provided.
  private UUID uuid;

  public BigQueryMultiTableSink(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();

    if (inputSchema != null) {
      Schema.Field tableField = inputSchema.getField(config.tableField);
      if (tableField == null) {
        throw new IllegalArgumentException(String.format("Table field '%s' does not exist in the input schema.",
                                                         config.tableField));
      }
      Schema tableFieldSchema = tableField.getSchema();
      tableFieldSchema = tableFieldSchema.isNullable() ? tableFieldSchema.getNonNullable() : tableFieldSchema;
      if (tableFieldSchema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Table field '%s' must be of type 'string', but found '%s'",
                        config.tableField, tableFieldSchema.getType().name().toLowerCase()));
      }
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    BigQuery bigquery = BigQueryUtils.getBigQuery(config.getServiceAccountFilePath(), config.getProject());
    // create dataset if dataset does not exist
    if (bigquery.getDataset(config.dataset) == null) {
      try {
        bigquery.create(DatasetInfo.newBuilder(config.dataset).build());
      } catch (BigQueryException e) {
        throw new RuntimeException("Exception occurred while creating dataset " + config.dataset + ".", e);
      }
    }

    uuid = UUID.randomUUID();
    String bucket = config.bucket;
    if (config.bucket == null) {
      bucket = uuid.toString();
    }

    for (Map.Entry<String, String> argument : context.getArguments()) {
      String key = argument.getKey();
      String val = argument.getValue();
      if (!key.startsWith("multisink.")) {
        continue;
      }
      String tableName = key.substring("multisink.".length());
      String cleansedName = cleanseTableName(tableName);
      Schema schema = Schema.parseJson(val);
      validateSchema(cleansedName, schema);
      String temporaryGcsPath = String.format("gs://%s/pipelines/bigquery-tmp/%s/%s/%s",
                                              bucket, context.getPipelineName(), uuid, cleansedName);
      addOutput(context, tableName, cleansedName, schema, temporaryGcsPath);
    }
  }

  private String cleanseTableName(String tableName) {
    return tableName.replaceAll("\\.", "_").replaceAll("-", "_");
  }

  private void addOutput(BatchSinkContext context, String tableName, String cleansedName,
                         Schema schema, String temporaryGcsPath) throws IOException {
    Configuration hConf = BigQueryUtils.getBigQueryConfig(config.getServiceAccountFilePath(), config.getProject());
    List<BigQueryTableFieldSchema> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String tableTypeName = BigQueryUtils.getTableDataType(
        BigQueryUtils.getNonNullableSchema(field.getSchema())).name();
      BigQueryTableFieldSchema tableFieldSchema = new BigQueryTableFieldSchema()
        .setName(field.getName())
        .setType(tableTypeName)
        .setMode(Field.Mode.NULLABLE.name());
      fields.add(tableFieldSchema);
    }

    String bucket = config.bucket;
    if (config.bucket == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket. So enable it only when bucket name
      // is not provided.
      hConf.setBoolean("fs.gs.bucket.delete.enable", true);
    }

    hConf.set(GoogleHadoopFileSystemBase.GCS_WORKING_DIRECTORY_KEY, String.format("gs://%s/", bucket));
    hConf.set("fs.gs.system.bucket", bucket);
    hConf.setBoolean("fs.gs.impl.disable.cache", true);
    hConf.setBoolean("fs.gs.metadata.cache.enable", false);

    BigQueryOutputConfiguration.configure(
      hConf,
      String.format("%s.%s", config.dataset, cleansedName),
      new BigQueryTableSchema().setFields(fields),
      temporaryGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      TextOutputFormat.class);
    FilterOutputFormat.configure(hConf, config.tableField, tableName);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema which .
    emitLineage(context, cleansedName, schema, fields);

    context.addOutput(Output.of(cleansedName,
                                new SinkOutputFormatProvider(FilterOutputFormat.class.getName(), hConf)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (config.bucket == null) {
      Path gcsPath = new Path(String.format("gs://%s", uuid.toString()));
      try {
        Configuration hConf = BigQueryUtils.getBigQueryConfig(config.getServiceAccountFilePath(), config.getProject());
        FileSystem fs = gcsPath.getFileSystem(hConf);
        if (fs.exists(gcsPath)) {
          fs.delete(gcsPath, true);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete bucket " + gcsPath.toUri().getPath() + ", " + e.getMessage());
      }
    }
  }

  private void emitLineage(BatchSinkContext context, String tableName, Schema schema,
                           List<BigQueryTableFieldSchema> fields) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, tableName);
    lineageRecorder.createExternalDataset(schema);

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.",
                                  fields.stream().map(BigQueryTableFieldSchema::getName).collect(Collectors.toList()));
    }
  }

  /**
   * Validates output schema against bigquery table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than bigquery table or output schema field types does not match bigquery
   * column types.
   */
  private void validateSchema(String tableName, Schema schema) throws IOException {
    Table table = BigQueryUtils.getBigQueryTable(config.getServiceAccountFilePath(), config.getProject(),
                                                 config.dataset, tableName);
    if (table == null) {
      // Table does not exist, so no further validation is required.
      return;
    }

    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = schema.getFields();

    // Output schema should not have fields that are not present in BigQuery table.
    List<String> diff = BigQueryUtils.getSchemaMinusBqFields(outputSchemaFields, bqFields);
    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("The output schema does not match the BigQuery table schema for '%s.%s' table. " +
                        "The table does not contain the '%s' column(s).", config.dataset, tableName, diff));
    }

    // validate the missing columns in output schema are nullable fields in bigquery
    List<String> remainingBQFields = BigQueryUtils.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
    for (String field : remainingBQFields) {
      if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
        throw new IllegalArgumentException(
          String.format("The output schema does not match the BigQuery table schema for '%s.%s'. " +
                          "The table requires column '%s', which is not in the output schema.",
                        config.dataset, tableName, field));
      }
    }

    // Match output schema field type with bigquery column type
    for (Schema.Field field : outputSchemaFields) {
      validateSimpleTypes(field);
      BigQueryUtils.validateFieldSchemaMatches(bqFields.get(field.getName()), field, config.dataset, tableName);
    }
  }

  private void validateSimpleTypes(Schema.Field field) {
    String name = field.getName();
    Schema fieldSchema = BigQueryUtils.getNonNullableSchema(field.getSchema());
    Schema.Type type = fieldSchema.getType();

    // Complex types like arrays, maps and unions are not supported in BigQuery plugins.
    if (!type.isSimpleType()) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'.", name, type));
    }
  }

  /**
   * Conf for the plugin
   */
  public static class Conf extends GCPConfig {

    @Macro
    @Description("The dataset to write to. A dataset is contained within a specific project. "
      + "Datasets are top-level containers that are used to organize and control access to tables and views.")
    private String dataset;

    @Macro
    @Nullable
    @Description("The Google Cloud Storage bucket to store temporary data in. "
      + "It will be automatically created if it does not exist, but will not be automatically deleted. "
      + "Cloud Storage data will be deleted after it is loaded into BigQuery. " +
      "If it is not provided, a unique bucket will be created and then deleted after the run finishes.")
    private String bucket;

    @Macro
    @Nullable
    @Description("The name of the field that will be used to determine which table to write to. " +
      "Defaults to 'tablename'.")
    private String tableField;

    public Conf() {
      tableField = "tablename";
    }
  }
}
