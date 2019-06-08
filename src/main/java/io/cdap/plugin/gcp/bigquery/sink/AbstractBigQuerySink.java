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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink plugins.
 */
public abstract class AbstractBigQuerySink extends BatchSink<StructuredRecord, Text, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQuerySink.class);

  private static final String gcsPathFormat = "gs://%s";
  private static final String temporaryBucketFormat = gcsPathFormat + "/input/%s-%s";

  // UUID for the run. Will be used as bucket name if bucket is not provided.
  // UUID is used since GCS bucket names must be globally unique.
  private final UUID uuid = UUID.randomUUID();
  private Configuration baseConfiguration;

  /**
   * Executes main prepare run logic. Child classes cannot override this method,
   * instead they should implement two methods {@link #prepareRunValidation(BatchSinkContext)}
   * and {@link #prepareRunInternal(BatchSinkContext, BigQuery, String)} in order to add custom logic.
   *
   * @param context batch sink context
   */
  @Override
  public final void prepareRun(BatchSinkContext context) throws Exception {
    prepareRunValidation(context);

    AbstractBigQuerySinkConfig config = getConfig();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountFilePath == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath);
    String project = config.getProject();
    BigQuery bigQuery = GCPUtils.getBigQuery(project, credentials);
    baseConfiguration = getBaseConfiguration();
    String bucket = configureBucket();
    if (!context.isPreviewEnabled()) {
      BigQueryUtil.createResources(bigQuery, GCPUtils.getStorage(project, credentials), config.getDataset(), bucket);
    }

    prepareRunInternal(context, bigQuery, bucket);
  }

  @Override
  public final void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (getConfig().getBucket() == null) {
      Path gcsPath = new Path(String.format(gcsPathFormat, uuid.toString()));
      try {
        FileSystem fs = gcsPath.getFileSystem(baseConfiguration);
        if (fs.exists(gcsPath)) {
          fs.delete(gcsPath, true);
          LOG.debug("Deleted temporary bucket '{}'", gcsPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete bucket '{}': {}", gcsPath, e.getMessage());
      }
    }
  }

  /**
   * Initializes output along with lineage recording for given table and its schema.
   *
   * @param context batch sink context
   * @param bigQuery big query client for the configured project
   * @param outputName output name
   * @param tableName table name
   * @param tableSchema table schema
   * @param bucket bucket name
   */
  protected final void initOutput(BatchSinkContext context, BigQuery bigQuery, String outputName,
                                  String tableName, @Nullable Schema tableSchema, String bucket) throws IOException {
    LOG.debug("Init output for table '{}' with schema: {}", tableName, tableSchema);
    List<BigQueryTableFieldSchema> fields = getBigQueryTableFields(bigQuery, tableName,
                                                                   tableSchema,
                                                                   getConfig().isAllowSchemaRelaxation());
    Configuration configuration = getOutputConfiguration(bucket, tableName, fields);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exist.
    // We call emitLineage before since it creates the dataset with schema which is used.
    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());
    recordLineage(context, outputName, tableSchema, fieldNames);
    context.addOutput(Output.of(outputName, getOutputFormatProvider(configuration, tableName, tableSchema)));
  }

  /**
   * Child classes must provide configuration based on {@link AbstractBigQuerySinkConfig}.
   *
   * @return config instance
   */
  protected abstract AbstractBigQuerySinkConfig getConfig();

  /**
   * Child classes must override this method to provide specific validation logic to executed before
   * actual {@link #prepareRun(BatchSinkContext)} method execution.
   * For example, Batch Sink plugin can validate schema right away,
   * Batch Multi Sink does not have information at this point to do the validation.
   *
   * @param context batch sink context
   */
  protected abstract void prepareRunValidation(BatchSinkContext context);

  /**
   * Executes main prepare run logic, i.e. prepares output for given table (for Batch Sink plugin)
   * or for a number of tables (for Batch Multi Sink plugin).
   *
   * @param context batch sink context
   * @param bigQuery a big query client for the configured project
   * @param bucket bucket name
   */
  protected abstract void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery,
                                             String bucket) throws IOException;

  /**
   * Returns output format provider instance specific to the child classes that extend this class.
   *
   * @param configuration Hadoop configuration
   * @param tableName table name
   * @param tableSchema table schema
   * @return output format provider
   */
  protected abstract OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                                  String tableName,
                                                                  Schema tableSchema);

  /**
   * Initialized base configuration needed to load data into BigQuery table.
   *
   * @return base configuration
   */
  private Configuration getBaseConfiguration() throws IOException {
    Configuration baseConfiguration = BigQueryUtil.getBigQueryConfig(getConfig().getServiceAccountFilePath(),
                                                                     getConfig().getProject());
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION,
                                 getConfig().isAllowSchemaRelaxation());
    return baseConfiguration;
  }

  /**
   * Generates full path to temporary bucket based on given bucket and table names.
   *
   * @param bucket bucket name
   * @param tableName table name
   * @return full path to temporary bucket
   */
  private String getTemporaryGcsPath(String bucket, String tableName) {
    return String.format(temporaryBucketFormat, bucket, tableName, uuid);
  }

  /**
   * Updates {@link #baseConfiguration} with bucket details.
   * Uses provided bucket, otherwise generates random.
   *
   * @return bucket name
   */
  private String configureBucket() {
    String bucket = getConfig().getBucket();
    if (bucket == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket.
      // So enable it only when bucket name is not provided.
      baseConfiguration.setBoolean("fs.gs.bucket.delete.enable", true);
    }
    baseConfiguration.set("fs.gs.system.bucket", bucket);
    baseConfiguration.setBoolean("fs.gs.impl.disable.cache", true);
    baseConfiguration.setBoolean("fs.gs.metadata.cache.enable", false);
    return bucket;
  }

  /**
   * Validates output schema against Big Query table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than Big Query table or output schema field types does not match
   * Big Query column types unless schema relaxation policy is allowed.
   *
   * @param tableName table name
   * @param tableSchema table schema
   * @param allowSchemaRelaxation allows schema relaxation policy
   */
  private void validateSchema(BigQuery bigQuery, String tableName,
                              Schema tableSchema,
                              boolean allowSchemaRelaxation) {
    TableId tableId = TableId.of(getConfig().getProject(), getConfig().getDataset(), tableName);
    Table table = bigQuery.getTable(tableId);
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
    List<Schema.Field> outputSchemaFields = Objects.requireNonNull(tableSchema.getFields());

    List<String> missingBQFields = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);

    if (allowSchemaRelaxation) {
      List<String> nonNullableFields = missingBQFields.stream()
        .map(tableSchema::getField)
        .filter(Objects::nonNull)
        .filter(field -> !field.getSchema().isNullable())
        .map(Schema.Field::getName)
        .collect(Collectors.toList());

      if (!nonNullableFields.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("The output schema contains non-nullable fields '%s' " +
                          "which are absent in the BigQuery table schema for '%s.%s' table.",
                        nonNullableFields, getConfig().getDataset(), tableName));
      }
    } else {
      // Output schema should not have fields that are not present in BigQuery table,
      if (!missingBQFields.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("The output schema does not match the BigQuery table schema for '%s.%s' table. " +
                          "The table does not contain the '%s' column(s).",
                        getConfig().getDataset(), tableName, missingBQFields));
      }

      // validate the missing columns in output schema are nullable fields in BigQuery
      List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
      for (String field : remainingBQFields) {
        if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
          throw new IllegalArgumentException(
            String.format("The output schema does not match the BigQuery table schema for '%s.%s'. " +
                            "The table requires column '%s', which is not in the output schema.",
                          getConfig().getDataset(), tableName, field));
        }
      }
    }

    // Match output schema field type with BigQuery column type
    for (Schema.Field field : tableSchema.getFields()) {
      String fieldName = field.getName();
      // skip checking schema if field is missing in BigQuery
      if (!missingBQFields.contains(fieldName)) {
        BigQueryUtil.validateFieldSchemaMatches(bqFields.get(field.getName()),
                                                field, getConfig().getDataset(), tableName);
      }
    }

  }

  /**
   * Generates Big Query field instances based on given CDAP table schema after schema validation.
   *
   * @param tableName table name
   * @param tableSchema table schema
   * @param allowSchemaRelaxation if schema relaxation policy is allowed
   * @return list of Big Query fields
   */
  private List<BigQueryTableFieldSchema> getBigQueryTableFields(BigQuery bigQuery, String tableName,
                                                                @Nullable Schema tableSchema,
                                                                boolean allowSchemaRelaxation) {
    if (tableSchema == null) {
      return Collections.emptyList();
    }

    validateSchema(bigQuery, tableName, tableSchema, allowSchemaRelaxation);

    List<Schema.Field> inputFields = Objects.requireNonNull(tableSchema.getFields(), "Schema must have fields");

    return inputFields.stream()
      .map(field -> new BigQueryTableFieldSchema()
        .setName(field.getName())
        .setType(getTableDataType(field.getSchema()).name())
        .setMode(getMode(field.getSchema()).name()))
      .collect(Collectors.toList());
  }

  private Field.Mode getMode(Schema schema) {
    if (schema.getType() == Schema.Type.ARRAY) {
      return Field.Mode.REPEATED;
    } else if (schema.isNullable()) {
      return Field.Mode.NULLABLE;
    }
    return Field.Mode.REQUIRED;
  }

  /**
   * Creates Hadoop configuration for the given table and its fields.
   *
   * @param bucket bucket name
   * @param tableName table name
   * @param fields list of Big Query fields
   * @return Hadoop configuration
   */
  private Configuration getOutputConfiguration(String bucket,
                                               String tableName,
                                               List<BigQueryTableFieldSchema> fields) throws IOException {
    Configuration configuration = new Configuration(baseConfiguration);
    String temporaryGcsPath = getTemporaryGcsPath(bucket, tableName);

    BigQueryTableSchema outputTableSchema = new BigQueryTableSchema();
    if (!fields.isEmpty()) {
      outputTableSchema.setFields(fields);
    }

    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s.%s", getConfig().getDataset(), tableName),
      outputTableSchema,
      temporaryGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      TextOutputFormat.class);

    return configuration;
  }

  private void recordLineage(BatchSinkContext context,
                             String outputName,
                             Schema tableSchema,
                             List<String> fieldNames) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.", fieldNames);
    }
  }

  private LegacySQLTypeName getTableDataType(Schema schema) {
    schema = BigQueryUtil.getNonNullableSchema(schema);
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return LegacySQLTypeName.DATE;
        case TIME_MILLIS:
        case TIME_MICROS:
          return LegacySQLTypeName.TIME;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return LegacySQLTypeName.TIMESTAMP;
        case DECIMAL:
          return LegacySQLTypeName.NUMERIC;
        default:
          throw new IllegalStateException("Unsupported type " + logicalType.getToken());
      }
    }

    Schema.Type type = schema.getType();
    switch (type) {
      case INT:
      case LONG:
        return LegacySQLTypeName.INTEGER;
      case STRING:
        return LegacySQLTypeName.STRING;
      case FLOAT:
      case DOUBLE:
        return LegacySQLTypeName.FLOAT;
      case BOOLEAN:
        return LegacySQLTypeName.BOOLEAN;
      case BYTES:
        return LegacySQLTypeName.BYTES;
      case ARRAY:
        return getTableDataType(schema.getComponentSchema());
      default:
        throw new IllegalStateException("Unsupported type " + type);
    }
  }
}