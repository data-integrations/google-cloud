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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
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
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Common Util class for big query plugins such as {@link BigQuerySource} and {@link BigQuerySink}
 */
public final class BigQueryUtil {
  public static final String BUCKET_PATTERN = "[a-z0-9._-]+";
  public static final String DATASET_PATTERN = "[A-Za-z0-9_]+";
  public static final String TABLE_PATTERN = "[A-Za-z0-9_]+";

  // array of arrays and map of arrays are not supported by big query
  public static final Set<Schema.Type> UNSUPPORTED_ARRAY_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.MAP);

  // bigquery types to cdap schema types mapping
  public static final Map<LegacySQLTypeName, String> BQ_TYPE_MAP = ImmutableMap.<LegacySQLTypeName, String>builder()
    .put(LegacySQLTypeName.INTEGER, "long")
    .put(LegacySQLTypeName.FLOAT, "double")
    .put(LegacySQLTypeName.BOOLEAN, "boolean")
    .put(LegacySQLTypeName.BYTES, "bytes")
    .put(LegacySQLTypeName.RECORD, "record")
    .put(LegacySQLTypeName.STRING, "string")
    .put(LegacySQLTypeName.DATETIME, "string")
    .put(LegacySQLTypeName.DATE, "date")
    .put(LegacySQLTypeName.TIME, "time")
    .put(LegacySQLTypeName.TIMESTAMP, "timestamp")
    .put(LegacySQLTypeName.NUMERIC, "decimal")
    .build();

  private static final Map<Schema.Type, Set<LegacySQLTypeName>> TYPE_MAP = ImmutableMap.<Schema.Type,
    Set<LegacySQLTypeName>>builder()
    .put(Schema.Type.INT, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.LONG, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.STRING, ImmutableSet.of(LegacySQLTypeName.STRING, LegacySQLTypeName.DATETIME))
    .put(Schema.Type.FLOAT, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.DOUBLE, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.BOOLEAN, ImmutableSet.of(LegacySQLTypeName.BOOLEAN))
    .put(Schema.Type.BYTES, ImmutableSet.of(LegacySQLTypeName.BYTES))
    .put(Schema.Type.RECORD, ImmutableSet.of(LegacySQLTypeName.RECORD))
    .build();

  private static final Map<Schema.LogicalType, LegacySQLTypeName> LOGICAL_TYPE_MAP =
    ImmutableMap.<Schema.LogicalType, LegacySQLTypeName>builder()
      .put(Schema.LogicalType.DATE, LegacySQLTypeName.DATE)
      .put(Schema.LogicalType.TIME_MILLIS, LegacySQLTypeName.TIME)
      .put(Schema.LogicalType.TIME_MICROS, LegacySQLTypeName.TIME)
      .put(Schema.LogicalType.TIMESTAMP_MILLIS, LegacySQLTypeName.TIMESTAMP)
      .put(Schema.LogicalType.TIMESTAMP_MICROS, LegacySQLTypeName.TIMESTAMP)
      .put(Schema.LogicalType.DECIMAL, LegacySQLTypeName.NUMERIC)
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
   * @param serviceAccountFilePath service account file path
   * @param projectId BigQuery project ID
   * @param cmekKey the name of the cmek key
   * @return {@link Configuration} with config set for BigQuery
   * @throws IOException if not able to get credentials
   */
  public static Configuration getBigQueryConfig(@Nullable String serviceAccountFilePath, String projectId,
                                                @Nullable String cmekKey)
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
    if (serviceAccountFilePath != null) {
      configuration.set("mapred.bq.auth.service.account.json.keyfile", serviceAccountFilePath);
      configuration.set("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", projectId);
    configuration.set("fs.gs.working.dir", GCSPath.ROOT_DIR);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
    if (cmekKey != null) {
      configuration.set(BigQueryConfiguration.OUTPUT_TABLE_KMS_KEY_NAME_KEY, cmekKey);
    }
    return configuration;
  }

  /**
   * Converts BigQuery Table Schema into a CDAP Schema object.
   * @param bqSchema BigQuery Schema to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return CDAP schema object
   */
  public static Schema getTableSchema(com.google.cloud.bigquery.Schema bqSchema, FailureCollector collector) {
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
    if (schemafields.isEmpty() && !collector.getValidationFailures().isEmpty()) {
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
   * @param field BigQuery field to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return A CDAP schema field
   */
  @Nullable
  public static Schema.Field getSchemaField(Field field, FailureCollector collector) {
    Schema schema = convertFieldType(field, collector);
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
        collector.addFailure(String.format("Field '%s' has unsupported mode '%s'.", field.getName(), mode), null);
    }
    return null;
  }

  /**
   * Converts BiqQuery field type into a CDAP field type.
   * @param field Bigquery field to be converted.
   * @param collector Failure collector to collect failure messages for the client.
   * @return A CDAP field schema
   */
  @Nullable
  public static Schema convertFieldType(Field field, FailureCollector collector) {
    LegacySQLTypeName type = field.getType();
    Schema schema = null;
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
    } else if (value == StandardSQLTypeName.STRUCT) {
      FieldList fields = field.getSubFields();
      List<Schema.Field> schemafields = new ArrayList<>();
      for (Field f : fields) {
        Schema.Field schemaField = getSchemaField(f, collector);
        // if schema field is null, that means that there was a validation error. We will still continue in order to
        // collect more errors
        if (schemaField == null) {
          continue;
        }
        schemafields.add(schemaField);
      }
      // do not return schema for the struct field if none of the nested fields are of supported types
      if (!schemafields.isEmpty()) {
        schema = Schema.recordOf(field.getName(), schemafields);
      }
    } else {
      collector.addFailure(
          String.format("BigQuery column '%s' is of unsupported type '%s'.", field.getName(), value.name()),
          String.format("Supported column types are: %s.", BigQueryUtil.BQ_TYPE_MAP.keySet().stream()
              .map(t -> t.getStandardType().name()).collect(Collectors.joining(", "))));
    }
    return schema;
  }

  /**
   * Validates if provided field schema matches with BigQuery table column type.
   *
   * @param bqField bigquery table field
   * @param field schema field
   * @param dataset dataset name
   * @param table table name
   * @param supportedTypes types supported
   * @param collector failure collector
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

      if (LOGICAL_TYPE_MAP.get(logicalType) != bqField.getType()) {
        return collector.addFailure(
          String.format("Field '%s' of type '%s' has incompatible type with column '%s' in BigQuery table '%s.%s'.",
                        name, fieldSchema.getDisplayName(), bqField.getName(), dataset, table),
          String.format("Modify the input so that it is of type '%s'.", BQ_TYPE_MAP.get(bqField.getType())));
      }

      // BigQuery schema precision must be at most 38 and scale at most 9
      if (logicalType == Schema.LogicalType.DECIMAL) {
        if (fieldSchema.getPrecision() > 38 || fieldSchema.getScale() > 9) {
          return collector.addFailure(String.format("Decimal Field '%s' has invalid precision '%s' and scale '%s'. ",
                                                    name, fieldSchema.getPrecision(), fieldSchema.getScale()),
                                      "Precision must be at most 38 and scale must be at most 9.");
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
   * Get difference of schema fields and big query table fields. The operation is equivalent to
   * (Names of schema fields - Names of bigQuery table fields).
   *
   * @param schemaFields schema fields
   * @param bqFields bigquery table fields
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
   * @param bqFields bigquery table fields
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
   * Validates schema of type array. BigQuery does not allow nullable arrays or nullable type within array.
   *
   * @param arraySchema schema of array field
   * @param name name of the array field
   * @param collector failure collector
   * @return returns validation failure if invalid array schema, otherwise returns null
   */
  @Nullable
  public static ValidationFailure validateArraySchema(Schema arraySchema, String name, FailureCollector collector) {
    if (arraySchema.isNullable()) {
      return collector.addFailure(String.format("Field '%s' is of type array.", name),
                                  "Change the field to be non-nullable.");
    }

    Schema componentSchema = arraySchema.getComponentSchema();
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
   * @param projectId BigQuery project ID
   * @param datasetId BigQuery dataset ID
   * @param tableName BigQuery table name
   * @param serviceAccountPath service account file path
   * @return BigQuery table
   */
  @Nullable
  public static Table getBigQueryTable(String projectId, String datasetId, String tableName,
                                       @Nullable String serviceAccountPath) {
    TableId tableId = TableId.of(projectId, datasetId, tableName);

    com.google.auth.Credentials credentials = null;
    if (serviceAccountPath != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountPath);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException(
          String.format("Unable to load credentials from %s", serviceAccountPath), "serviceFilePath");
      }
    }
    BigQuery bigQuery = GCPUtils.getBigQuery(projectId, credentials);

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
   * @param projectId BigQuery project ID
   * @param datasetId BigQuery dataset ID
   * @param tableName BigQuery table name
   * @param serviceAccountPath service account file path
   * @param collector failure collector
   * @return BigQuery table
   */
  @Nullable
  public static Table getBigQueryTable(String projectId, String datasetId, String tableName,
                                       @Nullable String serviceAccountPath, FailureCollector collector) {
    TableId tableId = TableId.of(projectId, datasetId, tableName);
    com.google.auth.Credentials credentials = null;
    if (serviceAccountPath != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountPath);
      } catch (IOException e) {
        collector.addFailure(String.format("Unable to load credentials from %s.", serviceAccountPath),
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
   * @param bucket bucket name
   * @param bucketPropertyName bucket name property
   * @param collector failure collector
   */
  public static void validateBucket(String bucket, String bucketPropertyName, FailureCollector collector) {
    // Allowed character validation for bucket name as per https://cloud.google.com/storage/docs/naming
    String errorMessage = "Bucket name can only contain lowercase letters, numbers, '.', '_', and '-'.";
    match(bucket, bucketPropertyName, BUCKET_PATTERN, collector, errorMessage);
  }

  /**
   * Validates allowed characters for dataset name.
   *
   * @param dataset dataset name
   * @param datasetPropertyName dataset name property
   * @param collector failure collector
   */
  public static void validateDataset(String dataset, String datasetPropertyName, FailureCollector collector) {
    // Allowed character validation for dataset name as per https://cloud.google.com/bigquery/docs/datasets
    String errorMessage = "Dataset name can only contain letters (lower or uppercase), numbers and '_'.";
    match(dataset, datasetPropertyName, DATASET_PATTERN, collector, errorMessage);
  }

  /**
   * Validates allowed characters for table name.
   *
   * @param table table name
   * @param tablePropertyName table name property
   * @param collector failure collector
   */
  public static void validateTable(String table, String tablePropertyName, FailureCollector collector) {
    // Allowed character validation for table name as per https://cloud.google.com/bigquery/docs/tables
    String errorMessage = "Table name can only contain letters (lower or uppercase), numbers and '_'.";
    match(table, tablePropertyName, TABLE_PATTERN, collector, errorMessage);
  }

  /**
   * Matches text with provided pattern. If the text does not match the pattern, the method adds a new failure to
   * failure collector.
   *
   * @param text text to be matched
   * @param propertyName property name
   * @param pattern pattern
   * @param collector failure collector
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
}
