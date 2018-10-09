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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.data.schema.Schema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import static co.cask.gcp.common.GCPUtils.loadServiceAccountCredentials;

/**
 * Common Util class for big query plugins such as {@link BigQuerySource} and {@link BigQuerySink}
 */
final class BigQueryUtils {
  private static final Map<Schema.Type, LegacySQLTypeName> TYPE_MAP = ImmutableMap.<Schema.Type,
    LegacySQLTypeName>builder()
    .put(Schema.Type.INT, LegacySQLTypeName.INTEGER)
    .put(Schema.Type.LONG, LegacySQLTypeName.INTEGER)
    .put(Schema.Type.STRING, LegacySQLTypeName.STRING)
    .put(Schema.Type.FLOAT, LegacySQLTypeName.FLOAT)
    .put(Schema.Type.DOUBLE, LegacySQLTypeName.FLOAT)
    .put(Schema.Type.BOOLEAN, LegacySQLTypeName.BOOLEAN)
    .put(Schema.Type.BYTES, LegacySQLTypeName.BYTES)
    .build();

  private static final Map<Schema.LogicalType, LegacySQLTypeName> LOGICAL_TYPE_MAP = ImmutableMap.of(
    Schema.LogicalType.DATE, LegacySQLTypeName.DATE,
    Schema.LogicalType.TIME_MILLIS, LegacySQLTypeName.TIME, Schema.LogicalType.TIME_MICROS, LegacySQLTypeName.TIME,
    Schema.LogicalType.TIMESTAMP_MILLIS, LegacySQLTypeName.TIMESTAMP,
    Schema.LogicalType.TIMESTAMP_MICROS, LegacySQLTypeName.TIMESTAMP
  );

  /**
   * Gets non nullable type from provided schema.
   *
   * @param schema schema to be used
   * @return non-nullable {@link Schema}
   */
  static Schema getNonNullableSchema(Schema schema) {
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
  static Configuration getBigQueryConfig(@Nullable String serviceAccountFilePath, String projectId) throws IOException {
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
    configuration.set("fs.AbstractFileSystm.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", projectId);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
    return configuration;
  }

  /**
   * Get BigQuery Table.
   *
   * @param serviceAccountFilePath service account file path
   * @param project BigQuery project ID
   * @param dataset dataset for the BigQuery table
   * @param table BigQuery table
   * @return returns BigQuery table
   * @throws IOException if not able to load credentials
   */
  @Nullable
  static Table getBigQueryTable(@Nullable String serviceAccountFilePath, String project,
                                String dataset, String table) throws IOException {
    BigQuery bigquery = getBigQuery(serviceAccountFilePath, project);

    TableId id = TableId.of(project, dataset, table);
    return bigquery.getTable(id);
  }

  /**
   * Get BigQuery service
   * @param serviceAccountFilePath service account file path
   * @param project BigQuery project ID
   */
  static BigQuery getBigQuery(@Nullable String serviceAccountFilePath, String project) throws IOException {
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
    if (serviceAccountFilePath != null) {
      bigqueryBuilder.setCredentials(loadServiceAccountCredentials(serviceAccountFilePath));
    }

    bigqueryBuilder.setProjectId(project);
    return bigqueryBuilder.build().getService();
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
  static void validateFieldSchemaMatches(Field bqField, Schema.Field field, String dataset, String table) {
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
      // Return once logical types are validated. This is because logical types are represented as primitive types
      // internally.
      return;
    }

    if (TYPE_MAP.get(type) == null) {
      throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                                                       field.getName(), type));
    }

    if (TYPE_MAP.get(type) != bqField.getType()) {
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
  static List<String> getSchemaMinusBqFields(List<Schema.Field> schemaFields, FieldList bqFields) {
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
  static List<String> getBqFieldsMinusSchema(FieldList bqFields, List<Schema.Field> schemaFields) {
    List<String> diff = new ArrayList<>();

    for (Field field : bqFields) {
      diff.add(field.getName());
    }

    for (Schema.Field field : schemaFields) {
      diff.remove(field.getName());
    }
    return diff;
  }

  static LegacySQLTypeName getTableDataType(Schema schema) {
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
        default:
          throw new IllegalStateException("Unsupported logical type " + logicalType);
      }
    }

    Schema.Type type = schema.getType();
    switch(type) {
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
      default:
        throw new IllegalStateException("Unsupported type " + type);
    }
  }

  private BigQueryUtils() {
  }
}
