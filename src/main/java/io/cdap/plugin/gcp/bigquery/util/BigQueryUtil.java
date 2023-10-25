/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.bigquery.util;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySink;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.BigNumeric;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.Numeric;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import javax.annotation.Nullable;

/**
 * Common Util class for big query plugins such as {@link BigQuerySource} and {@link BigQuerySink}
 */
public final class BigQueryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtil.class);

  private static final String DEFAULT_PARTITION_COLUMN_NAME = "_PARTITIONTIME";
  private static final String BIGQUERY_BUCKET_PREFIX_PROPERTY_NAME = "gcp.bigquery.bucket.prefix";

  public static final String BUCKET_PATTERN = "[a-z0-9._-]+";
  public static final String DATASET_PATTERN = "[A-Za-z0-9_]+";
  // Read more about table naming in BigQuery here: https://cloud.google.com/bigquery/docs/tables#table_naming
  public static final String TABLE_PATTERN = "[\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}\\p{Zs}]+";

  // Tags for BQ Jobs
  public static final String BQ_JOB_TYPE_SOURCE_TAG = "bq_source_plugin";
  public static final String BQ_JOB_TYPE_EXECUTE_TAG = "bq_execute_plugin";
  public static final String BQ_JOB_TYPE_SINK_TAG = "bq_sink_plugin";
  public static final String BQ_JOB_TYPE_PUSHDOWN_TAG = "bq_pushdown";

  // array of arrays and map of arrays are not supported by big query
  public static final Set<Schema.Type> UNSUPPORTED_ARRAY_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.MAP);

  // bigquery types to cdap schema types mapping
  public static final Map<LegacySQLTypeName, String> BQ_TYPE_MAP = ImmutableMap.<LegacySQLTypeName, String>builder()
    .put(LegacySQLTypeName.INTEGER, "long")
    .put(LegacySQLTypeName.FLOAT, "double")
    .put(LegacySQLTypeName.BOOLEAN, "boolean")
    .put(LegacySQLTypeName.BYTES, "bytes")
    .put(LegacySQLTypeName.RECORD, "record")
    .put(LegacySQLTypeName.STRING, "string or datetime")
    .put(LegacySQLTypeName.DATETIME, "datetime or string")
    .put(LegacySQLTypeName.DATE, "date")
    .put(LegacySQLTypeName.TIME, "time")
    .put(LegacySQLTypeName.TIMESTAMP, "timestamp")
    .put(LegacySQLTypeName.NUMERIC, "decimal")
    .build();

  public static final String BQ_JOB_SOURCE_TAG = "job_source";
  public static final String BQ_JOB_TYPE_TAG = "type";
  public static final Set<String> BQ_JOB_LABEL_SYSTEM_KEYS = ImmutableSet.of(BQ_JOB_SOURCE_TAG, BQ_JOB_TYPE_TAG);

  private static final Map<Schema.Type, Set<LegacySQLTypeName>> TYPE_MAP = ImmutableMap.<Schema.Type,
    Set<LegacySQLTypeName>>builder()
    .put(Schema.Type.INT, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.LONG, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.STRING, ImmutableSet.of(LegacySQLTypeName.STRING, LegacySQLTypeName.DATETIME,
            LegacySQLTypeName.valueOf("JSON")))
    .put(Schema.Type.FLOAT, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.DOUBLE, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.BOOLEAN, ImmutableSet.of(LegacySQLTypeName.BOOLEAN))
    .put(Schema.Type.BYTES, ImmutableSet.of(LegacySQLTypeName.BYTES))
    .put(Schema.Type.RECORD, ImmutableSet.of(LegacySQLTypeName.RECORD))
    .build();

  private static final Map<Schema.LogicalType, Set<LegacySQLTypeName>> LOGICAL_TYPE_MAP =
    ImmutableMap.<Schema.LogicalType, Set<LegacySQLTypeName>>builder()
      .put(Schema.LogicalType.DATE, ImmutableSet.of(LegacySQLTypeName.DATE))
      .put(Schema.LogicalType.DATETIME, ImmutableSet.of(LegacySQLTypeName.DATETIME, LegacySQLTypeName.STRING))
      .put(Schema.LogicalType.TIME_MILLIS, ImmutableSet.of(LegacySQLTypeName.TIME))
      .put(Schema.LogicalType.TIME_MICROS, ImmutableSet.of(LegacySQLTypeName.TIME))
      .put(Schema.LogicalType.TIMESTAMP_MILLIS, ImmutableSet.of(LegacySQLTypeName.TIMESTAMP))
      .put(Schema.LogicalType.TIMESTAMP_MICROS, ImmutableSet.of(LegacySQLTypeName.TIMESTAMP))
      .put(Schema.LogicalType.DECIMAL, ImmutableSet.of(LegacySQLTypeName.NUMERIC, LegacySQLTypeName.BIGNUMERIC))
      .build();

  /**
   * Gets non nullable type from provided schema.
   *
   * @param schema schema to be used
   * @return non-nullable {@link Schema}
   */
  public static Schema getNonNullableSchema(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable() : schema;
  }

  /**
   * Get Bigquery {@link Configuration}.
   *
   * @param serviceAccountInfo service account file path or JSON content
   * @param projectId          BigQuery project ID
   * @param cmekKeyName            the name of the cmek key
   * @param serviceAccountType type of the service account
   * @return indicator for whether service account is file or json
   * @throws IOException if not able to get credentials
   */
  public static Configuration getBigQueryConfig(@Nullable String serviceAccountInfo, String projectId,
                                                @Nullable CryptoKeyName cmekKeyName, String serviceAccountType)
    throws IOException {
    Job job = Job.getInstance();

    // some input formats require the credentials to be present in the job. We don't know for
    // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
    // effect, because this method is only used at configure time and will be ignored later on.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    Configuration configuration = job.getConfiguration();
    configuration.clear();
    Map<String, String> authProperties =
      GCPUtils.generateBigQueryAuthProperties(serviceAccountInfo, serviceAccountType);
    authProperties.forEach(configuration::set);
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", projectId);
    configuration.set("fs.gs.working.dir", GCSPath.ROOT_DIR);
    configuration.set(BigQueryConfiguration.PROJECT_ID.getKey(), projectId);
    if (cmekKeyName != null) {
      configuration.set(BigQueryConfiguration.OUTPUT_TABLE_KMS_KEY_NAME.getKey(),
          cmekKeyName.toString());
    }
    return configuration;
  }

  /**
   * Converts BigQuery Table Schema into a CDAP Schema object.
   *
   * @param bqSchema  BigQuery Schema to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return CDAP schema object
   */
  public static Schema getTableSchema(com.google.cloud.bigquery.Schema bqSchema, @Nullable FailureCollector collector) {
    FieldList fields = bqSchema.getFields();
    List<Schema.Field> schemafields = new ArrayList<>();

    for (Field field : fields) {
      Schema.Field schemaField = getSchemaField(field, collector);
      // if schema field is null, that means that there was a validation error. We will still continue in order to
      // collect more errors
      if (schemaField == null) {
        continue;
      }
      schemafields.add(schemaField);
    }
    if (schemafields.isEmpty() && collector != null && !collector.getValidationFailures().isEmpty()) {
      // throw if there was validation failure(s) added to the collector
      collector.getOrThrowException();
    }
    if (schemafields.isEmpty()) {
      return null;
    }
    return Schema.recordOf("output", schemafields);
  }

  /**
   * Converts BigQuery schema field into a corresponding CDAP Schema.Field.
   *
   * @param field     BigQuery field to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return A CDAP schema field
   */
  @Nullable
  public static Schema.Field getSchemaField(Field field, @Nullable FailureCollector collector) {
    return getSchemaField(field, collector, null);
  }

  /**
   * Converts BigQuery schema field into a corresponding CDAP Schema.Field.
   *
   * @param field        BigQuery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to prepend to recordNames to make them unique
   * @return A CDAP schema field
   */
  @Nullable
  private static Schema.Field getSchemaField(Field field, @Nullable FailureCollector collector,
                                            @Nullable String recordPrefix) {
    Schema schema = convertFieldType(field, collector, recordPrefix);
    if (schema == null) {
      return null;
    }

    Field.Mode mode = field.getMode() == null ? Field.Mode.NULLABLE : field.getMode();
    switch (mode) {
      case NULLABLE:
        return Schema.Field.of(field.getName(), Schema.nullableOf(schema));
      case REQUIRED:
        return Schema.Field.of(field.getName(), schema);
      case REPEATED:
        return Schema.Field.of(field.getName(), Schema.arrayOf(schema));
      default:
        // this should not happen, unless newer bigquery versions introduces new mode that is not supported by this
        // plugin.
        String error = String.format("Field '%s' has unsupported mode '%s'.", field.getName(), mode);
        if (collector != null) {
          collector.addFailure(error, null);
        } else {
          throw new RuntimeException(error);
        }
    }
    return null;
  }

  /**
   * Converts BiqQuery field type into a CDAP field type.
   *
   * @param field     Bigquery field to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return A CDAP field schema
   */
  @Nullable
  public static Schema convertFieldType(Field field, @Nullable FailureCollector collector) {
    return convertFieldType(field, collector, null);
  }

  /**
   * Converts BiqQuery field type into a CDAP field type.
   *
   * @param field        Bigquery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to add before a record name to ensure unique names.
   * @return A CDAP field schema
   */
  @Nullable
  public static Schema convertFieldType(Field field, @Nullable FailureCollector collector,
                                        @Nullable String recordPrefix) {
    LegacySQLTypeName type = field.getType();
    StandardSQLTypeName standardType = type.getStandardType();
    switch (standardType) {
      case FLOAT64:
        // float is a float64, so corresponding type becomes double
        return Schema.of(Schema.Type.DOUBLE);
      case BOOL:
        return Schema.of(Schema.Type.BOOLEAN);
      case INT64:
        // int is a int64, so corresponding type becomes long
        return Schema.of(Schema.Type.LONG);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case DATETIME:
        return Schema.of(Schema.LogicalType.DATETIME);
      case BYTES:
        return Schema.of(Schema.Type.BYTES);
      case TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case NUMERIC:
        // bigquery has Numeric.PRECISION digits of precision and Numeric.SCALE digits of scale for NUMERIC.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        return Schema.decimalOf(Numeric.PRECISION, Numeric.SCALE);
      case BIGNUMERIC:
        // bigquery has BigNumeric.PRECISION digits of precision and BigNumeric.SCALE digits of scale for BIGNUMERIC.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        return Schema.decimalOf(BigNumeric.PRECISION, BigNumeric.SCALE);
      case STRUCT:
        FieldList fields = field.getSubFields();
        List<Schema.Field> schemaFields = new ArrayList<>();

        // Record names have to be unique as Avro doesn't allow to redefine them.
        // We can make them unique by prepending the previous records names to their name.
        String recordTypeName = "";
        if (recordPrefix != null) {
          recordTypeName = recordPrefix + '.';
        }
        recordTypeName = recordTypeName + field.getName();

        for (Field f : fields) {
          Schema.Field schemaField = getSchemaField(f, collector, recordTypeName);
          // if schema field is null, that means that there was a validation error. We will still continue in order to
          // collect more errors
          if (schemaField == null) {
            continue;
          }
          schemaFields.add(schemaField);
        }
        // do not return schema for the struct field if none of the nested fields are of supported types
        if (!schemaFields.isEmpty()) {
          Schema namingSchema = Schema.recordOf(schemaFields);
          recordTypeName = recordTypeName + namingSchema.getRecordName();
          return Schema.recordOf(recordTypeName, schemaFields);
        } else {
          return null;
        }
      default:
        String error =
          String.format("BigQuery column '%s' is of unsupported type '%s'.", field.getName(), standardType.name());
        String action = String.format("Supported column types are: %s.",
                                      BigQueryUtil.BQ_TYPE_MAP.keySet().stream().map(t -> t.getStandardType().name())
                                        .collect(Collectors.joining(", ")));
        if (collector != null) {
          collector.addFailure(error, action);
        } else {
          throw new RuntimeException(error + action);
        }
        return null;
    }
  }

  /**
   * Validates if provided field schema matches with BigQuery table column type.
   *
   * @param bqField        bigquery table field
   * @param field          schema field
   * @param dataset        dataset name
   * @param table          table name
   * @param supportedTypes types supported
   * @param collector      failure collector
   * @return returns validation failure
   */
  @Nullable
  public static ValidationFailure validateFieldSchemaMatches(Field bqField, Schema.Field field, String dataset,
                                                             String table, Set<Schema.Type> supportedTypes,
                                                             FailureCollector collector) {
    // validate type of fields against BigQuery column type
    String name = field.getName();
    Schema fieldSchema = getNonNullableSchema(field.getSchema());
    Schema.Type type = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    // validate logical types
    if (logicalType != null) {
      if (LOGICAL_TYPE_MAP.get(logicalType) == null) {
        return collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", field.getName(), fieldSchema.getDisplayName()),
          String.format("Supported types are: %s, date, time, timestamp and decimal.",
                        supportedTypes.stream().map(t -> t.name().toLowerCase()).collect(Collectors.joining(", "))));
      }

      if (!LOGICAL_TYPE_MAP.get(logicalType).contains(bqField.getType())) {
        return collector.addFailure(
          String.format("Field '%s' of type '%s' has incompatible type with column '%s' in BigQuery table '%s.%s'.",
                        name, fieldSchema.getDisplayName(), bqField.getName(), dataset, table),
          String.format("Modify the input so that it is of type '%s'.", BQ_TYPE_MAP.get(bqField.getType())));
      }

      // BigQuery schema precision must be at most BigNumeric.PRECISION and scale at most BigNumeric.SCALE
      if (logicalType == Schema.LogicalType.DECIMAL) {
        if (fieldSchema.getPrecision() > BigNumeric.PRECISION || fieldSchema.getScale() > BigNumeric.SCALE) {
          return collector.addFailure(
              String.format("Decimal Field '%s' has invalid precision '%s' and scale '%s'. ",
                  name, fieldSchema.getPrecision(), fieldSchema.getScale()),
              String.format("Precision must be at most '%s' and scale must be at most '%s'.",
                  BigNumeric.PRECISION, BigNumeric.SCALE)
          );
        }
      }

      // Return once logical types are validated. This is because logical types are represented as primitive types
      // internally.
      return null;
    }

    // Complex types like maps and unions are not supported in BigQuery plugins.
    if (!supportedTypes.contains(type)) {
      return collector.addFailure(
        String.format("Field '%s' is of unsupported type '%s'.", name, type.name().toLowerCase()),
        String.format("Supported types are: %s, date, time, timestamp and decimal.",
                      supportedTypes.stream().map(t -> t.name().toLowerCase()).collect(Collectors.joining(", "))));
    }

    if (type == Schema.Type.ARRAY) {
      ValidationFailure failure = validateArraySchema(field.getSchema(), field.getName(), collector);
      if (failure != null) {
        return failure;
      }
      if (bqField.getMode() == Field.Mode.REPEATED) {
        fieldSchema = fieldSchema.getComponentSchema();
        type = fieldSchema.getType();
      }
    }

    if (TYPE_MAP.get(type) != null && !TYPE_MAP.get(type).contains(bqField.getType())) {
      return collector.addFailure(
        String.format("Field '%s' of type '%s' is incompatible with column '%s' of type '%s' " +
                        "in BigQuery table '%s.%s'.", field.getName(), fieldSchema.getDisplayName(), bqField.getName(),
                      BQ_TYPE_MAP.get(bqField.getType()), dataset, table),
        String.format("It must be of type '%s'.", BQ_TYPE_MAP.get(bqField.getType())));
    }
    return null;
  }

  /**
   * Check the mode of the output schema fields against big query table fields.
   *
   * @param bigQueryField schema fields
   * @param field         bigquery table fields
   * @param collector     failure collector
   */
  public static void validateFieldModeMatches(Field bigQueryField, Schema.Field field, boolean allowSchemaRelaxation,
                                              FailureCollector collector) {
    Field.Mode mode = bigQueryField.getMode();
    boolean isBqFieldNullable = mode == null || mode.equals(Field.Mode.NULLABLE);

    Schema fieldSchema = field.getSchema();
    if (!allowSchemaRelaxation && fieldSchema.isNullable() && !isBqFieldNullable) {
      // Nullable output schema field is incompatible with required BQ table field

      // In case of arrays, BigQuery handles null arrays and convert them to empty arrays at insert
      if (!getNonNullableSchema(fieldSchema).getType().equals(Schema.Type.ARRAY)) {
        collector.addFailure(String.format("Field '%s' cannot be nullable.", bigQueryField.getName()),
                "Change the field to be required.")
                .withOutputSchemaField(field.getName());
      }
    }
  }

  /**
   * Get difference of schema fields and big query table fields. The operation is equivalent to
   * (Names of schema fields - Names of bigQuery table fields).
   *
   * @param schemaFields schema fields
   * @param bqFields     bigquery table fields
   * @return list of remaining field names
   */
  public static List<String> getSchemaMinusBqFields(List<Schema.Field> schemaFields, FieldList bqFields) {
    List<String> diff = new ArrayList<>();

    for (Schema.Field field : schemaFields) {
      diff.add(field.getName());
    }

    for (Field field : bqFields) {
      diff.remove(field.getName());
    }
    return diff;
  }

  /**
   * Get difference of big query table fields and schema fields. The operation is equivalent to
   * (Names of bigQuery table fields - Names of schema fields).
   *
   * @param bqFields     bigquery table fields
   * @param schemaFields schema fields
   * @return list of remaining field names
   */
  public static List<String> getBqFieldsMinusSchema(FieldList bqFields, List<Schema.Field> schemaFields) {
    List<String> diff = new ArrayList<>();

    for (Field field : bqFields) {
      diff.add(field.getName());
    }

    for (Schema.Field field : schemaFields) {
      diff.remove(field.getName());
    }
    return diff;
  }

  /**
   * Converts Hadoop configuration to map of properties.
   *
   * @param config Hadoop configuration
   * @return properties map
   */
  public static Map<String, String> configToMap(Configuration config) {
    return StreamSupport.stream(config.spliterator(), false)
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Validates schema of type array. BigQuery does not allow nullable type within array.
   *
   * @param arraySchema schema of array field
   * @param name        name of the array field
   * @param collector   failure collector
   * @return returns validation failure if invalid array schema, otherwise returns null
   */
  @Nullable
  public static ValidationFailure validateArraySchema(Schema arraySchema, String name, FailureCollector collector) {
    Schema nonNullableSchema = getNonNullableSchema(arraySchema);
    Schema componentSchema = nonNullableSchema.getComponentSchema();

    if (componentSchema.isNullable()) {
      return collector.addFailure(String.format("Field '%s' contains null values in its array.", name),
                                  "Change the array component type to be non-nullable.");
    }

    if (UNSUPPORTED_ARRAY_TYPES.contains(componentSchema.getType())) {
      return collector.addFailure(String.format("Field '%s' is an array of unsupported type '%s'.",
                                                name, componentSchema.getDisplayName()),
                                  "Change the array component type to be a valid type.");
    }

    return null;
  }

  /**
   * Get BigQuery table.
   *
   * @param datasetProject           project where dataset is in
   * @param datasetId                BigQuery dataset ID
   * @param tableName                BigQuery table name
   * @param serviceAccount           service account file path or JSON content
   * @param isServiceAccountFilePath indicator for whether service account is file or json
   * @return BigQuery table
   */
  @Nullable
  public static Table getBigQueryTable(String datasetProject, String datasetId, String tableName,
                                       @Nullable String serviceAccount, boolean isServiceAccountFilePath) {
    TableId tableId = TableId.of(datasetProject, datasetId, tableName);

    com.google.auth.Credentials credentials = null;
    if (serviceAccount != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccount, isServiceAccountFilePath);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException(
          String.format("Unable to load credentials from %s", isServiceAccountFilePath ? serviceAccount : " JSON."),
          "serviceFilePath");
      }
    }
    BigQuery bigQuery = GCPUtils.getBigQuery(datasetProject, credentials);

    Table table;
    try {
      table = bigQuery.getTable(tableId);
    } catch (BigQueryException e) {
      throw new InvalidStageException("Unable to get details about the BigQuery table: " + e.getMessage(), e);
    }

    return table;
  }

  /**
   * Get BigQuery table.
   *
   * @param projectId          BigQuery project ID
   * @param datasetId          BigQuery dataset ID
   * @param tableName          BigQuery table name
   * @param serviceAccountPath service account file path
   * @param collector          failure collector
   * @return BigQuery table
   */
  @Nullable
  public static Table getBigQueryTable(String projectId, String datasetId, String tableName,
                                       @Nullable String serviceAccountPath, FailureCollector collector) {
    return getBigQueryTable(projectId, datasetId, tableName, serviceAccountPath, true, collector);
  }

  /**
   * Get BigQuery table.
   *
   * @param projectId                BigQuery project ID
   * @param dataset                  BigQuery dataset name
   * @param tableName                BigQuery table name
   * @param serviceAccount           service account file path or JSON content
   * @param isServiceAccountFilePath indicator for whether service account is file or json
   * @param collector                failure collector
   * @return BigQuery table
   */
  public static Table getBigQueryTable(String projectId, String dataset, String tableName,
                                       @Nullable String serviceAccount, @Nullable Boolean isServiceAccountFilePath,
                                       FailureCollector collector) {
    TableId tableId = TableId.of(projectId, dataset, tableName);
    com.google.auth.Credentials credentials = null;
    if (serviceAccount != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccount, isServiceAccountFilePath);
      } catch (IOException e) {
        collector.addFailure(String.format("Unable to load credentials from %s.",
                                           isServiceAccountFilePath ? serviceAccount : "provided JSON key"),
                             "Ensure the service account file is available on the local filesystem.")
          .withConfigProperty(GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH);
        throw collector.getOrThrowException();
      }
    }
    BigQuery bigQuery = GCPUtils.getBigQuery(projectId, credentials);

    Table table = null;
    try {
      table = bigQuery.getTable(tableId);
    } catch (BigQueryException e) {
      collector.addFailure("Unable to get details about the BigQuery table: " + e.getMessage(), null)
        .withConfigProperty(BigQuerySourceConfig.NAME_TABLE);
      throw collector.getOrThrowException();
    }

    return table;
  }

  /**
   * Validates allowed characters for bucket name.
   *
   * @param bucket             bucket name
   * @param bucketPropertyName bucket name property
   * @param collector          failure collector
   */
  public static void validateBucket(String bucket, String bucketPropertyName, FailureCollector collector) {
    // Allowed character validation for bucket name as per https://cloud.google.com/storage/docs/naming
    String errorMessage = "Bucket name can only contain lowercase letters, numbers, '.', '_', and '-'.";
    match(bucket, bucketPropertyName, BUCKET_PATTERN, collector, errorMessage);
  }

  /**
   * Validates allowed characters for dataset name.
   *
   * @param dataset             dataset name
   * @param datasetPropertyName dataset name property
   * @param collector           failure collector
   */
  public static void validateDataset(String dataset, String datasetPropertyName, FailureCollector collector) {
    // Allowed character validation for dataset name as per https://cloud.google.com/bigquery/docs/datasets
    String errorMessage = "Dataset name can only contain letters (lower or uppercase), numbers and '_'.";
    match(dataset, datasetPropertyName, DATASET_PATTERN, collector, errorMessage);
  }

  /**
   * Validates allowed characters for table name.
   *
   * @param table             table name
   * @param tablePropertyName table name property
   * @param collector         failure collector
   */
  public static void validateTable(String table, String tablePropertyName, FailureCollector collector) {
    // Allowed character validation for table name as per https://cloud.google.com/bigquery/docs/tables
    String errorMessage = "Table name can only contain letters (lower or uppercase), numbers, '_' and '-'.";
    match(table, tablePropertyName, TABLE_PATTERN, collector, errorMessage);
  }

  /**
   * Validates allowed GCS Upload Request Chunk Size.
   *
   * @param chunkSize             provided chunk size
   * @param chunkSizePropertyName GCS chunk size name property
   * @param collector             failure collector
   */
  public static void validateGCSChunkSize(String chunkSize, String chunkSizePropertyName, FailureCollector collector) {
    if (!Strings.isNullOrEmpty(chunkSize)) {
      try {
        if (Integer.parseInt(chunkSize) % MediaHttpUploader.MINIMUM_CHUNK_SIZE != 0) {
          collector.addFailure(
            String.format("Value must be a multiple of %s.", MediaHttpUploader.MINIMUM_CHUNK_SIZE), null)
            .withConfigProperty(chunkSizePropertyName);
        }
      } catch (NumberFormatException e) {
        collector.addFailure(e.getMessage(), "Input value must be a valid number.")
          .withConfigProperty(chunkSizePropertyName);
      }
    }
  }

  /**
   * Matches text with provided pattern. If the text does not match the pattern, the method adds a new failure to
   * failure collector.
   *
   * @param text         text to be matched
   * @param propertyName property name
   * @param pattern      pattern
   * @param collector    failure collector
   * @param errorMessage error message
   */
  private static void match(String text, String propertyName, String pattern,
                            FailureCollector collector, String errorMessage) {
    if (!Strings.isNullOrEmpty(text)) {
      Pattern p = Pattern.compile(pattern);
      if (!p.matcher(text).matches()) {
        collector.addFailure(errorMessage, null).withConfigProperty(propertyName);
      }
    }
  }

  /**
   * Deletes temporary directory.
   *
   * @param configuration Hadoop Configuration.
   * @param dir           directory to delete
   */
  public static void deleteTemporaryDirectory(Configuration configuration, String dir) throws IOException {
    Path path = new Path(dir);
    FileSystem fs = path.getFileSystem(configuration);
    if (fs.exists(path)) {
      fs.delete(path, true);
      LOG.debug("Deleted temporary directory '{}'", path);
    }
  }

  public static String generateTimePartitionCondition(StandardTableDefinition tableDefinition,
                                                      String partitionFromDate,
                                                      String partitionToDate) {

    TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();

    if (timePartitioning == null) {
      return StringUtils.EMPTY;
    }

    StringBuilder timePartitionCondition = new StringBuilder();
    String columnName = timePartitioning.getField() != null ?
      timePartitioning.getField() : DEFAULT_PARTITION_COLUMN_NAME;

    LegacySQLTypeName columnType = null;
    if (!DEFAULT_PARTITION_COLUMN_NAME.equals(columnName)) {
      columnType = tableDefinition.getSchema().getFields().get(columnName).getType();
    }

    String columnNameTS = columnName;
    if (!LegacySQLTypeName.TIMESTAMP.equals(columnType)) {
      columnNameTS = "TIMESTAMP(`" + columnNameTS + "`)";
    }
    if (partitionFromDate != null) {
      timePartitionCondition.append(columnNameTS).append(" >= ").append("TIMESTAMP(\"")
        .append(partitionFromDate).append("\")");
    }
    if (partitionFromDate != null && partitionToDate != null) {
      timePartitionCondition.append(" and ");
    }
    if (partitionToDate != null) {
      timePartitionCondition.append(columnNameTS).append(" < ").append("TIMESTAMP(\"")
        .append(partitionToDate).append("\")");
    }
    return timePartitionCondition.toString();
  }

  /**
   * Get fully-qualified name (FQN) for a BQ table (FQN format: bigquery:{projectId}.{datasetId}.{tableId}).
   *
   * @param datasetProject Name of the BQ project
   * @param datasetName    Name of the BQ dataset
   * @param tableName      Name of the BQ table
   * @return String fqn
   */
  public static String getFQN(String datasetProject, String datasetName, String tableName) {
    return String.format("%s:%s.%s.%s", BigQueryConstants.BQ_FQN_PREFIX,
                  datasetProject, datasetName, tableName);
  }

  /**
   * Get the bucket prefix from the runtime arguments. If not set, it will be created and set.
   *
   * @param arguments settable arguments instance to verify
   * @return the bucket prefix to use for this pipeline
   */
  @Nullable
  public static String getBucketPrefix(Map<String, String> arguments) {
    // If the bucket prefix property is set, use it.
    if (arguments.containsKey(BIGQUERY_BUCKET_PREFIX_PROPERTY_NAME)) {
      String bucketPrefix = arguments.get(BIGQUERY_BUCKET_PREFIX_PROPERTY_NAME);
      validateBucketPrefix(bucketPrefix);
      LOG.debug("Using bucket prefix for temporary buckets: {}", bucketPrefix);
      return bucketPrefix;
    }
    return null;
  }

  /**
   * Ensures configured bucket prefix is valid per the GCS naming convention.
   *
   * @param bucketPrefix
   */
  private static void validateBucketPrefix(String bucketPrefix) {
    if (!bucketPrefix.matches("^[a-z0-9-_.]+$")) {
      throw new IllegalArgumentException("The configured bucket prefix '" + bucketPrefix + "' is not a valid bucket " +
                                           "name. Bucket names can only contain lowercase letters, numeric " +
                                           "characters, dashes (-), underscores (_), and dots (.).");
    }

    if (!bucketPrefix.contains(".") && bucketPrefix.length() > 50) {
      throw new IllegalArgumentException("The configured bucket prefix '" + bucketPrefix + "' should be 50 " +
                                           "characters or shorter.");
    }
  }

  /**
   * Method to generate the CRC32 checksum for a location.
   * We use this to ensure location name length is constant (only 8 characters).
   *
   * @param location location to checksum
   * @return checksum value as an 8 character string (hex).
   */
  @VisibleForTesting
  public static String crc32location(String location) {
    byte[] bytes = location.toLowerCase().getBytes();
    Checksum checksum = new CRC32();
    checksum.update(bytes, 0, bytes.length);
    return Long.toHexString(checksum.getValue());
  }

  /**
   * Build bucket name concatenating the bucket prefix with the location crc32 hash using a hyphen (-)
   *
   * @param bucketPrefix Bucket prefix
   * @param location     location to use.
   * @return String containing the bucket location.
   */
  public static String getBucketNameForLocation(String bucketPrefix, String location) {
    return String.format("%s-%s", bucketPrefix, crc32location(location));
  }

  /**
   * Get labels for a BigQuery Job.
   * @param jobType the job type to set in the labels.
   * @return map containing labels for a BigQuery job launched by this plugin.
   */
  public static Map<String, String> getJobLabels(String jobType) {
    Map<String, String> labels = new HashMap<>();
    labels.put(BQ_JOB_SOURCE_TAG, "cdap");
    labels.put(BQ_JOB_TYPE_TAG, jobType);
    return labels;
  }

  /**
   * Get labels for a BigQuery Job.
   * @param jobType the job type to set in the labels.
   * @param userDefinedTags user defined tags to be added to the labels.
   * @return map containing labels for a BigQuery job launched by this plugin.
   */
  public static Map<String, String> getJobLabels(String jobType, String userDefinedTags) {
    Map<String, String> labels = getJobLabels(jobType);
    if (Strings.isNullOrEmpty(userDefinedTags)) {
        return labels;
    }
    for (String tag : userDefinedTags.trim().split(",")) {
      String[] keyValue = tag.split(":");
      if (keyValue.length == 1) {
        labels.put(keyValue[0], "");
      }
      if (keyValue.length == 2) {
        labels.put(keyValue[0], keyValue[1]);
      }
    }
    return labels;
  }

  /**
   * Identify a stating bucket name from the pipeline context and plugin configuration
   * @param arguments runtime arguments
   * @param configLocation location from plugin configuration
   * @param dataset BigQuery dataset
   * @param bucket bucket from plugin configuration
   * @return Bucket name to use for this sink.
   */
  @Nullable
  public static String getStagingBucketName(Map<String, String> arguments, @Nullable String configLocation,
                                            @Nullable Dataset dataset, @Nullable String bucket) {
    // Get Bucket Prefix from configuration
    String bucketPrefix = BigQueryUtil.getBucketPrefix(arguments);

    // If temp bucket name is not defined in configuration, and a common bucket name prefix has been specified,
    // we must set this prefix along with the destination location in order to create/reuse the bucket.
    // Otherwise, if temp bucket name is defined, or a prefix is not set, the configureBucket method will prepare
    // for a new bucket creation.
    if (Strings.isNullOrEmpty(bucket) && bucketPrefix != null) {
      // If the destination dataset exists, use the dataset location. Otherwise, use location from configuration.
      String datasetLocation = dataset != null ? dataset.getLocation() : configLocation;
      // Get the bucket name for the specified location.
      bucket = BigQueryUtil.getBucketNameForLocation(bucketPrefix, datasetLocation);
    }
    return bucket;
  }
}
