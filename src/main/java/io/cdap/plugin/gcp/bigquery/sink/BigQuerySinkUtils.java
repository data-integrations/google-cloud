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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.Numeric;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Utility class for the BigQuery DelegatingMultiSink.
 * <p>
 * The logic in this class has been extracted from the {@link AbstractBigQuerySink} in order to make this functionality
 * available to other classes in this package.
 */
public final class BigQuerySinkUtils {

  public static final String GS_PATH_FORMAT = "gs://%s/%s";
  private static final String TEMPORARY_BUCKET_FORMAT = GS_PATH_FORMAT + "/input/%s-%s";
  private static final String DATETIME = "DATETIME";

  /**
   * Creates the given dataset and bucket if they do not already exist. If the dataset already exists but the
   * bucket does not, the bucket will be created in the same location as the dataset. If the bucket already exists
   * but the dataset does not, the dataset will attempt to be created in the same location. This may fail if the bucket
   * is in a location that BigQuery does not yet support.
   *
   * @param bigQuery    the bigquery client for the project
   * @param storage     the storage client for the project
   * @param datasetName the name of the dataset
   * @param bucketName  the name of the bucket
   * @param location    the location of the resources, this is only applied if both the bucket and dataset do not exist
   * @param cmekKey     the name of the cmek key
   * @throws IOException if there was an error creating or fetching any GCP resource
   */
  public static void createResources(BigQuery bigQuery, Storage storage,
                                     String datasetName, String bucketName, @Nullable String location,
                                     @Nullable String cmekKey) throws IOException {
    Dataset dataset = bigQuery.getDataset(datasetName);
    Bucket bucket = storage.get(bucketName);

    if (dataset == null && bucket == null) {
      createBucket(storage, bucketName, location, cmekKey,
        () -> String.format("Unable to create Cloud Storage bucket '%s'", bucketName));
      createDataset(bigQuery, datasetName, location,
        () -> String.format("Unable to create BigQuery dataset '%s'", datasetName));
    } else if (bucket == null) {
      createBucket(
        storage, bucketName, dataset.getLocation(), cmekKey,
        () -> String.format(
          "Unable to create Cloud Storage bucket '%s' in the same location ('%s') as BigQuery dataset '%s'. "
            + "Please use a bucket that is in the same location as the dataset.",
          bucketName, dataset.getLocation(), datasetName));
    } else if (dataset == null) {
      createDataset(
        bigQuery, datasetName, bucket.getLocation(),
        () -> String.format(
          "Unable to create BigQuery dataset '%s' in the same location ('%s') as Cloud Storage bucket '%s'. "
            + "Please use a bucket that is in a supported location.",
          datasetName, bucket.getLocation(), bucketName));
    }
  }

  /**
   * Creates a Dataset in the specified location using the supplied BigQuery client.
   *
   * @param bigQuery     the bigQuery client.
   * @param dataset      the name of the dataset to create.
   * @param location     Location for this dataset.
   * @param errorMessage Supplier for the error message to output if the dataset could not be created.
   * @throws IOException if the dataset could not be created.
   */
  private static void createDataset(BigQuery bigQuery, String dataset, @Nullable String location,
                                    Supplier<String> errorMessage) throws IOException {
    DatasetInfo.Builder builder = DatasetInfo.newBuilder(dataset);
    if (location != null) {
      builder.setLocation(location);
    }
    try {
      bigQuery.create(builder.build());
    } catch (BigQueryException e) {
      if (e.getCode() != 409) {
        // A conflict means the dataset already exists (https://cloud.google.com/bigquery/troubleshooting-errors)
        // This most likely means multiple stages in the same pipeline are trying to create the same dataset.
        // Ignore this and move on, since all that matters is that the dataset exists.
        throw new IOException(errorMessage.get(), e);
      }
    }
  }

  /**
   * Creates the specified GCS bucket using the supplied GCS client.
   *
   * @param storage      GCS Client.
   * @param bucket       Bucket Name.
   * @param location     Location for this bucket.
   * @param cmekKey      CMEK key to use for this bucket.
   * @param errorMessage Supplier for the error message to output if the bucket could not be created.
   * @throws IOException if the bucket could not be created.
   */
  private static void createBucket(Storage storage, String bucket, @Nullable String location,
                                   @Nullable String cmekKey, Supplier<String> errorMessage) throws IOException {
    try {
      GCPUtils.createBucket(storage, bucket, location, cmekKey);
    } catch (StorageException e) {
      if (e.getCode() != 409) {
        // A conflict means the bucket already exists
        // This most likely means multiple stages in the same pipeline are trying to create the same dataset.
        // Ignore this and move on, since all that matters is that the dataset exists.
        throw new IOException(errorMessage.get(), e);
      }
    }
  }

  /**
   * Updates {@link Configuration} with bucket details.
   * Uses provided bucket, otherwise uses provided runId as a bucket name.
   *
   * @return bucket name
   */
  public static String configureBucket(Configuration baseConfiguration, @Nullable String bucket, String runId) {
    boolean deleteBucket = false;
    // If the bucket is null, assign the run ID as the bucket name and mark the bucket for deletion.
    if (bucket == null) {
      bucket = runId;
      deleteBucket = true;
    }
    return configureBucket(baseConfiguration, bucket, runId, deleteBucket);
  }

  /**
   * Updates {@link Configuration} with bucket details.
   * Uses provided bucket, otherwise uses provided runId as a bucket name.
   *
   * @return bucket name
   */
  public static String configureBucket(Configuration baseConfiguration,
                                       String bucket,
                                       String runId,
                                       boolean deleteBucket) {
    if (deleteBucket) {
      // By default, this option is false, meaning the job can not delete the bucket.
      // So enable it only when bucket name is not provided.
      baseConfiguration.setBoolean("fs.gs.bucket.delete.enable", true);
    }
    baseConfiguration.set("fs.default.name", String.format(GS_PATH_FORMAT, bucket, runId));
    baseConfiguration.setBoolean("fs.gs.impl.disable.cache", true);
    baseConfiguration.setBoolean("fs.gs.metadata.cache.enable", false);
    return bucket;
  }

  /**
   * Configures output for Sink
   *
   * @param configuration Hadoop configuration instance
   * @param projectName   name of the project to use
   * @param datasetName   name of the dataset to use
   * @param tableName     name of the table to use
   * @param gcsPath       GCS path to use for output
   * @param fields        list of BigQuery table fields
   * @throws IOException if the output cannot be configured
   */
  public static void configureOutput(Configuration configuration,
                                     String projectName,
                                     String datasetName,
                                     String tableName,
                                     String gcsPath,
                                     List<BigQueryTableFieldSchema> fields) throws IOException {

    // Set up table schema
    BigQueryTableSchema outputTableSchema = new BigQueryTableSchema();
    if (!fields.isEmpty()) {
      outputTableSchema.setFields(fields);
    }

    BigQueryFileFormat fileFormat = getFileFormat(fields);
    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s:%s.%s", projectName, datasetName, tableName),
      outputTableSchema,
      gcsPath,
      fileFormat,
      getOutputFormat(fileFormat));
  }

  /**
   * Configures output for MultiSink
   *
   * @param configuration Hadoop configuration instance
   * @param projectName   name of the project to use
   * @param datasetName   name of the dataset to use
   * @param tableName     name of the table to use
   * @param gcsPath       GCS path to use for output
   * @param fields        list of BigQuery table fields
   * @throws IOException if the output cannot be configured
   */
  public static void configureMultiSinkOutput(Configuration configuration,
                                              String projectName,
                                              String datasetName,
                                              String tableName,
                                              String gcsPath,
                                              List<BigQueryTableFieldSchema> fields) throws IOException {
    configureOutput(configuration,
      projectName,
      datasetName,
      tableName,
      gcsPath,
      fields);

    // Set operation as Insertion. Currently the BQ MultiSink can only support the insertion operation.
    configuration.set(BigQueryConstants.CONFIG_OPERATION, Operation.INSERT.name());
  }

  public static String getTemporaryGcsPath(String bucket, String pathPrefix, String tableName) {
    return String.format(TEMPORARY_BUCKET_FORMAT, bucket, pathPrefix, tableName, pathPrefix);
  }

  public static List<BigQueryTableFieldSchema> getBigQueryTableFieldsFromSchema(Schema tableSchema) {
    List<Schema.Field> inputFields = Objects.requireNonNull(tableSchema.getFields(), "Schema must have fields");
    return inputFields.stream()
      .map(BigQuerySinkUtils::generateTableFieldSchema)
      .collect(Collectors.toList());
  }

  private static BigQueryTableFieldSchema generateTableFieldSchema(Schema.Field field) {
    BigQueryTableFieldSchema fieldSchema = new BigQueryTableFieldSchema();
    fieldSchema.setName(field.getName());
    fieldSchema.setMode(getMode(field.getSchema()).name());
    LegacySQLTypeName type = getTableDataType(field.getSchema());
    fieldSchema.setType(type.name());
    if (type == LegacySQLTypeName.RECORD) {
      List<Schema.Field> schemaFields;
      if (Schema.Type.ARRAY == field.getSchema().getType()) {
        schemaFields = Objects.requireNonNull(field.getSchema().getComponentSchema()).getFields();
      } else {
        schemaFields = field.getSchema().isNullable()
          ? field.getSchema().getNonNullable().getFields()
          : field.getSchema().getFields();
      }
      fieldSchema.setFields(Objects.requireNonNull(schemaFields).stream()
        .map(BigQuerySinkUtils::generateTableFieldSchema)
        .collect(Collectors.toList()));

    }
    return fieldSchema;
  }

  private static Field.Mode getMode(Schema schema) {
    boolean isNullable = schema.isNullable();
    Schema.Type nonNullableType = isNullable ? schema.getNonNullable().getType() : schema.getType();
    if (isNullable && nonNullableType != Schema.Type.ARRAY) {
      return Field.Mode.NULLABLE;
    } else if (nonNullableType == Schema.Type.ARRAY) {
      return Field.Mode.REPEATED;
    }
    return Field.Mode.REQUIRED;
  }

  private static LegacySQLTypeName getTableDataType(Schema schema) {
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
          if ((schema.getScale() <= Numeric.SCALE) && (schema.getPrecision() <= Numeric.PRECISION)) {
            return LegacySQLTypeName.NUMERIC;
          }
          return LegacySQLTypeName.BIGNUMERIC;
        case DATETIME:
          return LegacySQLTypeName.DATETIME;
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
      case RECORD:
        return LegacySQLTypeName.RECORD;
      default:
        throw new IllegalStateException("Unsupported type " + type);
    }
  }

  private static BigQueryFileFormat getFileFormat(List<BigQueryTableFieldSchema> fields) {
    for (BigQueryTableFieldSchema field : fields) {
      if (DATETIME.equals(field.getType())) {
        return BigQueryFileFormat.NEWLINE_DELIMITED_JSON;
      }
    }
    return BigQueryFileFormat.AVRO;
  }

  private static Class<? extends FileOutputFormat> getOutputFormat(BigQueryFileFormat fileFormat) {
    if (fileFormat == BigQueryFileFormat.NEWLINE_DELIMITED_JSON) {
      return TextOutputFormat.class;
    }
    return AvroOutputFormat.class;
  }

  public static void recordLineage(BatchSinkContext context,
                                   String outputName,
                                   Schema tableSchema,
                                   List<String> fieldNames) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.", fieldNames);
    }
  }

  /**
   * Generates Big Query field instances based on given CDAP table schema after schema validation.
   *
   * @param bigQuery              big query object
   * @param tableName             table name
   * @param tableSchema           table schema
   * @param allowSchemaRelaxation if schema relaxation policy is allowed
   * @param project               GCP project id
   * @param dataset               dataset name
   * @param isTruncateTableSet    if truncate table option is set
   * @param collector             failure collector
   * @return list of Big Query fields
   */
  public static List<BigQueryTableFieldSchema> getBigQueryTableFields(BigQuery bigQuery, String tableName,
                                                                      @Nullable Schema tableSchema,
                                                                      boolean allowSchemaRelaxation, String project,
                                                                      String dataset, boolean isTruncateTableSet,
                                                                      FailureCollector collector) {
    if (tableSchema == null) {
      return Collections.emptyList();
    }

    TableId tableId = TableId.of(project, dataset, tableName);
    try {
      Table table = bigQuery.getTable(tableId);
      // if table is null that mean it does not exist. So there is no need to perform validation
      if (table != null) {
        com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
        validateSchema(tableName, bqSchema, tableSchema, allowSchemaRelaxation, isTruncateTableSet, dataset,
          collector);
      }
    } catch (BigQueryException e) {
      collector.addFailure("Unable to get details about the BigQuery table: " + e.getMessage(), null)
        .withConfigProperty("table");
      throw collector.getOrThrowException();
    }

    return BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(tableSchema);
  }


  /**
   * Validates output schema against Big Query table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than Big Query table or output schema field types does not match
   * Big Query column types unless schema relaxation policy is allowed.
   *
   * @param tableName             big query table
   * @param bqSchema              BigQuery table schema
   * @param tableSchema           Configured table schema
   * @param allowSchemaRelaxation allows schema relaxation policy
   * @param dataset               dataset name
   * @param isTruncateTableSet    if truncate table option is set
   * @param collector             failure collector
   */
  public static void validateSchema(
    String tableName,
    com.google.cloud.bigquery.Schema bqSchema,
    @Nullable Schema tableSchema,
    boolean allowSchemaRelaxation,
    boolean isTruncateTableSet,
    String dataset,
    FailureCollector collector
  ) {
    if (bqSchema == null || bqSchema.getFields().isEmpty() || tableSchema == null) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = Objects.requireNonNull(tableSchema.getFields());

    List<String> missingBQFields = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);

    if (allowSchemaRelaxation && !isTruncateTableSet) {
      // Required fields can be added only if truncate table option is set.
      List<String> nonNullableFields = missingBQFields.stream()
        .map(tableSchema::getField)
        .filter(Objects::nonNull)
        .filter(field -> !field.getSchema().isNullable())
        .map(Schema.Field::getName)
        .collect(Collectors.toList());

      for (String nonNullableField : nonNullableFields) {
        collector.addFailure(
          String.format("Required field '%s' does not exist in BigQuery table '%s.%s'.",
            nonNullableField, dataset, tableName),
          "Change the field to be nullable.")
          .withInputSchemaField(nonNullableField).withOutputSchemaField(nonNullableField);
      }
    }

    if (!allowSchemaRelaxation) {
      // schema should not have fields that are not present in BigQuery table,
      for (String missingField : missingBQFields) {
        collector.addFailure(
          String.format("Field '%s' does not exist in BigQuery table '%s.%s'.",
            missingField, dataset, tableName),
          String.format("Remove '%s' from the input, or add a column to the BigQuery table.", missingField))
          .withInputSchemaField(missingField).withOutputSchemaField(missingField);
      }

      // validate the missing columns in output schema are nullable fields in BigQuery
      List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
      for (String field : remainingBQFields) {
        Field.Mode mode = bqFields.get(field).getMode();
        // Mode is optional. If the mode is unspecified, the column defaults to NULLABLE.
        if (mode != null && mode != Field.Mode.NULLABLE) {
          collector.addFailure(String.format("Required Column '%s' is not present in the schema.", field),
            String.format("Add '%s' to the schema.", field));
        }
      }
    }

    // column type changes should be disallowed if either allowSchemaRelaxation or truncate table are not set.
    if (!allowSchemaRelaxation || !isTruncateTableSet) {
      // Match output schema field type with BigQuery column type
      for (Schema.Field field : tableSchema.getFields()) {
        String fieldName = field.getName();
        // skip checking schema if field is missing in BigQuery
        if (!missingBQFields.contains(fieldName)) {
          ValidationFailure failure = BigQueryUtil.validateFieldSchemaMatches(
            bqFields.get(field.getName()), field, dataset, tableName,
            AbstractBigQuerySinkConfig.SUPPORTED_TYPES, collector);
          if (failure != null) {
            failure.withInputSchemaField(fieldName).withOutputSchemaField(fieldName);
          }
          BigQueryUtil.validateFieldModeMatches(bqFields.get(fieldName), field,
            allowSchemaRelaxation,
            collector);
        }
      }
    }
    collector.getOrThrowException();
  }

  /**
   * Returns list of duplicated fields (case insensitive)
   */
  public static Set<String> getDuplicatedFields(List<String> schemaFields) {
    final Set<String> duplicatedFields = new HashSet<>();
    final Set<String> set = new HashSet<>();
    for (String field : schemaFields) {
      if (!set.add(field)) {
        duplicatedFields.add(field);
      }
    }
    return duplicatedFields;
  }

  public static Map<String, Integer> calculateDuplicates(List<String> values) {
    return values.stream()
      .map(v -> v.split(" ")[0])
      .collect(Collectors.toMap(p -> p, p -> 1, (x, y) -> x + y));
  }

  public static boolean isSupportedLogicalType(Schema.LogicalType logicalType) {
    if (logicalType != null) {
      return logicalType == Schema.LogicalType.DATE || logicalType == Schema.LogicalType.TIMESTAMP_MICROS ||
        logicalType == Schema.LogicalType.TIMESTAMP_MILLIS || logicalType == Schema.LogicalType.DECIMAL;
    }
    return false;
  }


  public static void validateInsertSchema(Table table, @Nullable Schema tableSchema,
                                          boolean allowSchemaRelaxation,
                                          boolean isTruncateTableSet,
                                          String dataset, FailureCollector collector) {
    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null || bqSchema.getFields().isEmpty()) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    if (isTruncateTableSet || tableSchema == null) {
      //no validation required for schema if truncate table is set.
      // BQ will overwrite the schema for normal tables when write disposition is WRITE_TRUNCATE
      //note - If write to single partition is supported in future, schema validation will be necessary
      return;
    }
    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = Objects.requireNonNull(tableSchema.getFields());

    List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
    for (String field : remainingBQFields) {
      if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
        collector.addFailure(String.format("Required Column '%s' is not present in the schema.", field),
          String.format("Add '%s' to the schema.", field));
      }
    }

    String tableName = table.getTableId().getTable();
    List<String> missingBQFields = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);
    // Match output schema field type with BigQuery column type
    for (Schema.Field field : tableSchema.getFields()) {
      String fieldName = field.getName();
      // skip checking schema if field is missing in BigQuery
      if (!missingBQFields.contains(fieldName)) {
        ValidationFailure failure = BigQueryUtil.validateFieldSchemaMatches(
          bqFields.get(field.getName()), field, dataset, tableName,
          AbstractBigQuerySinkConfig.SUPPORTED_TYPES, collector);
        if (failure != null) {
          failure.withInputSchemaField(fieldName).withOutputSchemaField(fieldName);
        }
        BigQueryUtil.validateFieldModeMatches(bqFields.get(fieldName), field,
          allowSchemaRelaxation,
          collector);
      }
    }
    collector.getOrThrowException();
  }

}
