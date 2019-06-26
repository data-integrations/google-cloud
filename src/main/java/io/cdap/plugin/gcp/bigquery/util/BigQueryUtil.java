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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySink;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Common Util class for big query plugins such as {@link BigQuerySource} and {@link BigQuerySink}
 */
public final class BigQueryUtil {
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                    Schema.Type.BOOLEAN, Schema.Type.BYTES, Schema.Type.ARRAY);
  // array of arrays and map of arrays are not supported by big query
  public static final Set<Schema.Type> UNSUPPORTED_ARRAY_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.MAP);

  private static final Map<Schema.Type, Set<LegacySQLTypeName>> TYPE_MAP = ImmutableMap.<Schema.Type,
    Set<LegacySQLTypeName>>builder()
    .put(Schema.Type.INT, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.LONG, ImmutableSet.of(LegacySQLTypeName.INTEGER))
    .put(Schema.Type.STRING, ImmutableSet.of(LegacySQLTypeName.STRING, LegacySQLTypeName.DATETIME))
    .put(Schema.Type.FLOAT, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.DOUBLE, ImmutableSet.of(LegacySQLTypeName.FLOAT))
    .put(Schema.Type.BOOLEAN, ImmutableSet.of(LegacySQLTypeName.BOOLEAN))
    .put(Schema.Type.BYTES, ImmutableSet.of(LegacySQLTypeName.BYTES))
    .build();

  private static final Map<Schema.LogicalType, LegacySQLTypeName> LOGICAL_TYPE_MAP =
    ImmutableMap.<Schema.LogicalType, LegacySQLTypeName>builder()
    .put(Schema.LogicalType.DATE, LegacySQLTypeName.DATE)
    .put(Schema.LogicalType.TIME_MILLIS, LegacySQLTypeName.TIME)
    .put(Schema.LogicalType.TIME_MICROS, LegacySQLTypeName.TIME)
    .put(Schema.LogicalType.TIMESTAMP_MILLIS, LegacySQLTypeName.TIMESTAMP)
    .put(Schema.LogicalType.TIMESTAMP_MICROS, LegacySQLTypeName.TIMESTAMP)
    .put(Schema.LogicalType.DECIMAL, LegacySQLTypeName.NUMERIC).build();

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
   * @return {@link Configuration} with config set for BigQuery
   * @throws IOException if not able to get credentials
   */
  public static Configuration getBigQueryConfig(@Nullable String serviceAccountFilePath, String projectId)
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
    return configuration;
  }

  /**
   * Creates the given dataset and bucket if they do not already exist. If the dataset already exists but the
   * bucket does not, the bucket will be created in the same location as the dataset. If the bucket already exists
   * but the dataset does not, the dataset will attempt to be created in the same location. This may fail if the bucket
   * is in a location that BigQuery does not yet support.
   *
   * @param bigQuery the bigquery client for the project
   * @param storage the storage client for the project
   * @param datasetName the name of the dataset
   * @param bucketName the name of the bucket
   * @throws IOException if there was an error creating or fetching any GCP resource
   */
  public static void createResources(BigQuery bigQuery, Storage storage,
                                     String datasetName, String bucketName) throws IOException {
    Dataset dataset = bigQuery.getDataset(datasetName);
    Bucket bucket = storage.get(bucketName);

    if (dataset == null && bucket == null) {
      createBucket(storage, bucketName, null,
                   () -> String.format("Unable to create Cloud Storage bucket '%s'", bucketName));
      createDataset(bigQuery, datasetName, null,
                    () -> String.format("Unable to create BigQuery dataset '%s'", datasetName));
    } else if (bucket == null) {
      createBucket(
        storage, bucketName, dataset.getLocation(),
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

  private static void createBucket(Storage storage, String bucket, @Nullable String location,
                                   Supplier<String> errorMessage) throws IOException {
    BucketInfo.Builder builder = BucketInfo.newBuilder(bucket);
    if (location != null) {
      builder.setLocation(location);
    }
    try {
      storage.create(builder.build());
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
   * Validates if provided field schema matches with BigQuery table column type.
   *
   * @param bqField bigquery table field
   * @param field schema field
   * @param dataset dataset name
   * @param table table name
   * @throws IllegalArgumentException if schema types do not match
   */
  public static void validateFieldSchemaMatches(Field bqField, Schema.Field field, String dataset, String table) {
    // validate type of fields against BigQuery column type
    Schema fieldSchema = getNonNullableSchema(field.getSchema());
    Schema.Type type = fieldSchema.getType();

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    // validate logical types
    if (logicalType != null) {
      if (LOGICAL_TYPE_MAP.get(logicalType) == null) {
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                                                         field.getName(), logicalType));
      }
      if (LOGICAL_TYPE_MAP.get(logicalType) != bqField.getType()) {
        throw new IllegalArgumentException(
          String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                          " '%s.%s' of type '%s'. It must be of type '%s'.",
                        field.getName(), logicalType, bqField.getName(), dataset, table,
                        bqField.getType(), bqField.getType()));
      }

      // BigQuery schema precision must be at most 38 and scale at most 9
      if (logicalType == Schema.LogicalType.DECIMAL) {
        if (fieldSchema.getPrecision() > 38 || fieldSchema.getScale() > 9) {
          throw new IllegalArgumentException(
            String.format("Numeric Field '%s' has invalid precision '%s' and scale '%s'. " +
                            "Precision must be at most 38 and scale must be at most 9.",
                          field.getName(), fieldSchema.getPrecision(), fieldSchema.getScale()));
        }
      }

      // Return once logical types are validated. This is because logical types are represented as primitive types
      // internally.
      return;
    }

    if (!SUPPORTED_TYPES.contains(type)) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                                                       field.getName(), type));
    }

    if (type == Schema.Type.ARRAY) {
      validateArraySchema(field.getSchema(), field.getName());
      if (bqField.getMode() == Field.Mode.REPEATED) {
        type = fieldSchema.getComponentSchema().getType();
      }
    }

    if (!TYPE_MAP.get(type).contains(bqField.getType())) {
      throw new IllegalArgumentException(
        String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                        " '%s.%s' of type '%s'. It must be of type '%s'.",
                      field.getName(), type, bqField.getName(), dataset, table, bqField.getType(), bqField.getType()));
    }
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
   * @throws IllegalArgumentException if the schema is not a valid BigQuery schema
   */
  public static void validateArraySchema(Schema arraySchema, String name) {
    if (arraySchema.isNullable()) {
      throw new IllegalArgumentException(String.format("Field '%s' is of type array. " +
                                                         "BigQuery does not allow nullable arrays.", name));
    }

    Schema componentSchema = arraySchema.getComponentSchema();
    if (componentSchema.isNullable()) {
      throw new IllegalArgumentException(String.format("Field '%s' contains null values in its array, " +
                                                         "which is not allowed by BigQuery.", name));
    }

    if (UNSUPPORTED_ARRAY_TYPES.contains(componentSchema.getType())) {
      throw new IllegalArgumentException(String.format("Field '%s' is of %s type within array, " +
                                                         "which is not a valid BigQuery type.",
                                                       name, componentSchema));
    }
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

}
