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
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryOutputConfiguration;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableSchema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.Numeric;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
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
  private static final String RECORD = "RECORD";
  private static final String JSON = "JSON";
  private static final Gson GSON = new Gson();
  private static final Type LIST_OF_FIELD_TYPE = new TypeToken<ArrayList<Field>>() {
  }.getType();

  // Fields used to build update/upsert queries
  private static final String CRITERIA_TEMPLATE = "T.`%s` = S.`%s`";
  private static final String SOURCE_DATA_QUERY = "(SELECT * FROM (SELECT row_number() OVER (PARTITION BY %s%s) " +
    "as rowid, * FROM %s) where rowid = 1)";
  private static final String UPDATE_QUERY = "UPDATE %s T SET %s FROM %s S WHERE %s";
  private static final String UPSERT_QUERY = "MERGE %s T USING %s S ON %s WHEN MATCHED THEN UPDATE SET %s " +
    "WHEN NOT MATCHED THEN INSERT (%s) VALUES(%s)";
  private static final String INSERT_ONLY_UPSERT_QUERY = "MERGE %s T USING %s S ON %s WHEN NOT MATCHED THEN " +
    "INSERT (%s) VALUES(%s)";
  private static final List<String> COMPARISON_OPERATORS =
    Arrays.asList("=", "<", ">", "<=", ">=", "!=", "<>",
                  "LIKE", "NOT LIKE", "BETWEEN", "NOT BETWEEN", "IN", "NOT IN", "IS NULL", "IS NOT NULL",
                  "IS TRUE", "IS NOT TRUE", "IS FALSE", "IS NOT FALSE");
  public static final String BYTES_PROCESSED_METRIC = "bytes.processed";

  /**
   * Creates the given dataset and bucket if they do not already exist. If the dataset already exists but the
   * bucket does not, the bucket will be created in the same location as the dataset. If the bucket already exists
   * but the dataset does not, the dataset will attempt to be created in the same location. This may fail if the bucket
   * is in a location that BigQuery does not yet support.
   *
   * @param bigQuery    the bigquery client for the project
   * @param storage     the storage client for the project
   * @param datasetId   the Id of the dataset
   * @param bucketName  the name of the bucket
   * @param location    the location of the resources, this is only applied if both the bucket and dataset do not exist
   * @param cmekKeyName the name of the cmek key
   * @throws IOException if there was an error creating or fetching any GCP resource
   */
  public static void createResources(BigQuery bigQuery, Storage storage,
                                     DatasetId datasetId, String bucketName, @Nullable String location,
                                     @Nullable CryptoKeyName cmekKeyName) throws IOException {
    // Get the required Dataset and bucket instances using the supplied clients
    Dataset dataset = bigQuery.getDataset(datasetId);
    Bucket bucket = storage.get(bucketName);

    createResources(bigQuery, dataset, datasetId, storage, bucket, bucketName, location, cmekKeyName);
  }

  /**
   * Creates the given dataset and bucket if the supplied ones are null.
   * <p>
   * If the dataset already exists but the
   * bucket does not, the bucket will be created in the same location as the dataset. If the bucket already exists
   * but the dataset does not, the dataset will attempt to be created in the same location. This may fail if the bucket
   * is in a location that BigQuery does not yet support.
   *
   * @param bigQuery   the bigquery client for the project
   * @param dataset    the bigquery dataset instance (may be null)
   * @param datasetId  the Id of the dataset
   * @param storage    the storage client for the project
   * @param bucket     the storage bucket instance (may be null)
   * @param bucketName the name of the bucket
   * @param location   the location of the resources, this is only applied if both the bucket and dataset do not exist
   * @param cmekKey    the name of the cmek key
   * @throws IOException
   */
  public static void createResources(BigQuery bigQuery, @Nullable Dataset dataset, DatasetId datasetId,
                                     Storage storage, @Nullable Bucket bucket, String bucketName,
                                     @Nullable String location, @Nullable CryptoKeyName cmekKey) throws IOException {
    if (dataset == null && bucket == null) {
      createBucket(storage, bucketName, location, cmekKey,
                   () -> String.format("Unable to create Cloud Storage bucket '%s'", bucketName));
      createDataset(bigQuery, datasetId, location, cmekKey,
                    () -> String.format("Unable to create BigQuery dataset '%s.%s'", datasetId.getProject(),
                                        datasetId.getDataset()));
    } else if (bucket == null) {
      createBucket(
        storage, bucketName, dataset.getLocation(), cmekKey,
        () -> String.format(
          "Unable to create Cloud Storage bucket '%s' in the same location ('%s') as BigQuery dataset '%s'. "
            + "Please use a bucket that is in the same location as the dataset.",
          bucketName, dataset.getLocation(), datasetId.getProject() + "." + datasetId.getDataset()));
    } else if (dataset == null) {
      createDataset(
        bigQuery, datasetId, bucket.getLocation(), cmekKey,
        () -> String.format(
          "Unable to create BigQuery dataset '%s' in the same location ('%s') as Cloud Storage bucket '%s'. "
            + "Please use a bucket that is in a supported location.",
          datasetId, bucket.getLocation(), bucketName));
    }
  }

  /**
   * Creates a Dataset in the specified location using the supplied BigQuery client.
   *
   * @param bigQuery     the bigQuery client.
   * @param dataset      the Id of the dataset to create.
   * @param location     Location for this dataset.
   * @param cmekKeyName  CMEK key to use for this dataset.
   * @param errorMessage Supplier for the error message to output if the dataset could not be created.
   * @throws IOException if the dataset could not be created.
   */
  private static void createDataset(BigQuery bigQuery, DatasetId dataset, @Nullable String location,
                                    @Nullable CryptoKeyName cmekKeyName,
                                    Supplier<String> errorMessage) throws IOException {
    DatasetInfo.Builder builder = DatasetInfo.newBuilder(dataset);
    if (location != null) {
      builder.setLocation(location);
    }
    if (cmekKeyName != null) {
      builder.setDefaultEncryptionConfiguration(
        EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKeyName.toString()).build());
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
   * Creates a Dataset in the specified location using the supplied BigQuery client if it does not exist.
   *
   * @param bigQuery     the bigQuery client.
   * @param datasetId    the Id of the dataset to create.
   * @param location     Location for this dataset.
   * @param cmekKeyName  CMEK key to use for this dataset.
   * @param errorMessage Supplier for the error message to output if the dataset could not be created.
   * @throws IOException if the dataset could not be created.
   */
  public static void createDatasetIfNotExists(BigQuery bigQuery, DatasetId datasetId, @Nullable String location,
                                              @Nullable CryptoKeyName cmekKeyName,
                                              Supplier<String> errorMessage) throws IOException {
    // Check if dataset exists
    Dataset ds = bigQuery.getDataset(datasetId);
    // Create dataset if needed
    if (ds == null) {
      createDataset(bigQuery, datasetId, location, cmekKeyName, errorMessage);
    }
  }

  /**
   * Creates the specified GCS bucket using the supplied GCS client.
   *
   * @param storage      GCS Client.
   * @param bucket       Bucket Name.
   * @param location     Location for this bucket.
   * @param cmekKeyName  CMEK key to use for this bucket.
   * @param errorMessage Supplier for the error message to output if the bucket could not be created.
   * @throws IOException if the bucket could not be created.
   */
  private static void createBucket(Storage storage, String bucket, @Nullable String location,
                                   @Nullable CryptoKeyName cmekKeyName,
                                   Supplier<String> errorMessage) throws IOException {
    try {
      GCPUtils.createBucket(storage, bucket, location, cmekKeyName);
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
   * Sets string fields to JSON type if they are present in the provided list of fields.
   * @param fields list of BigQuery table fields.
   * @param jsonStringFields Comma separated list of fields that should be set to JSON type.
   *
   */
  public static void setJsonStringFields(List<BigQueryTableFieldSchema> fields,
                                                                   String jsonStringFields) {
    Set<String> jsonFields = new HashSet<>(Arrays.asList(jsonStringFields.split(",")));
    setJsonStringFields(fields, jsonFields, new ArrayList<>());
  }

  private static void setJsonStringFields(List<BigQueryTableFieldSchema> fields, Set<String> jsonFields,
                                             List<String> path) {
    for (BigQueryTableFieldSchema field : fields) {
      String fieldName = field.getName();
      String fieldType = field.getType();
      String separator = path.isEmpty() ? "" : ".";
      String fieldPath = String.join(".", path) + separator + fieldName;
      if (jsonFields.contains(fieldPath) && fieldType.equals(LegacySQLTypeName.STRING.name())) {
        field.setType(LegacySQLTypeName.valueOf(JSON).name());
      } else if (field.getType().equals(LegacySQLTypeName.RECORD.name())) {
        path.add(fieldName);
        setJsonStringFields(field.getFields(), jsonFields, path);
        path.remove(path.size() - 1);
      }
    }
  }

  /**
   * Configures output for Sink
   *
   * @param configuration Hadoop configuration instance
   * @param datasetId     id of the dataset to use
   * @param tableName     name of the table to use
   * @param gcsPath       GCS path to use for output
   * @param fields        list of BigQuery table fields
   * @throws IOException if the output cannot be configured
   */
  public static void configureOutput(Configuration configuration,
                                     DatasetId datasetId,
                                     String tableName,
                                     String gcsPath,
                                     List<BigQueryTableFieldSchema> fields) throws IOException {

    // Set up table schema
    BigQueryTableSchema outputTableSchema = new BigQueryTableSchema();
    if (!fields.isEmpty()) {
      String jsonStringFields = configuration.get(BigQueryConstants.CONFIG_JSON_STRING_FIELDS, null);
      if (jsonStringFields != null) {
        setJsonStringFields(fields, jsonStringFields);
      }
      outputTableSchema.setFields(fields);
    }

    BigQueryFileFormat fileFormat = getFileFormat(fields);
    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s:%s.%s", datasetId.getProject(), datasetId.getDataset(), tableName),
      outputTableSchema,
      gcsPath,
      fileFormat,
      getOutputFormat(fileFormat));
  }

  /**
   * Configures output for MultiSink
   *
   * @param configuration Hadoop configuration instance
   * @param datasetId     name of the dataset to use
   * @param tableName     name of the table to use
   * @param gcsPath       GCS path to use for output
   * @param fields        list of BigQuery table fields
   * @throws IOException if the output cannot be configured
   */
  public static void configureMultiSinkOutput(Configuration configuration,
                                              DatasetId datasetId,
                                              String tableName,
                                              String gcsPath,
                                              List<BigQueryTableFieldSchema> fields) throws IOException {
    configureOutput(configuration,
                    datasetId,
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

  /**
   * Relaxes the Destination Table Schema based on the matching field names from the source table
   *
   * @param bigquery         BigQuery client
   * @param sourceTable      source table, which contains the updated field definition
   * @param destinationTable destination table, whose fields definitions may be relaxed depending on the source table.
   */
  public static void relaxTableSchema(BigQuery bigquery,
                                      Table sourceTable,
                                      Table destinationTable) {
    List<Field> sourceFields = sourceTable.getDefinition().getSchema().getFields();
    List<Field> destinationFields = destinationTable.getDefinition().getSchema().getFields();

    relaxTableSchema(bigquery, destinationTable, sourceFields, destinationFields);
  }


  /**
   * Relaxes the Destination Table Schema based on the matching field names from the source table
   *
   * @param bigquery          BigQuery client
   * @param destinationTable  destination table, whose fields definitions may be relaxed depending on the source fields.
   * @param sourceFields      fields in the source table that need to be used to relax the destination table
   * @param destinationFields fields in the destination table that may be relaxed depending on the source fields
   */
  public static void relaxTableSchema(BigQuery bigquery,
                                      Table destinationTable,
                                      List<Field> sourceFields,
                                      List<Field> destinationFields) {
    List<Field> resultFieldsList = getRelaxedTableFields(sourceFields, destinationFields);

    // Update table definition, relaxing field definitions.
    com.google.cloud.bigquery.Schema newSchema = com.google.cloud.bigquery.Schema.of(resultFieldsList);
    bigquery.update(
      destinationTable.toBuilder().setDefinition(
        destinationTable.getDefinition().toBuilder().setSchema(newSchema).build()
      ).build()
    );
  }

  public static List<Field> getRelaxedTableFields(List<Field> sourceFields,
                                                  List<Field> destinationFields) {
    // Collect all fields form the destination table into a Map for quick lookups by name
    Map<String, Field> destinationFieldMap = destinationFields.stream()
      .collect(Collectors.toMap(Field::getName, x -> x));

    // Collect all fields form the source table into a Map for quick lookups by name
    Map<String, Field> sourceFieldMap = sourceFields.stream()
      .collect(Collectors.toMap(Field::getName, x -> x));

    // Collects all fields in the destination table that are not present in the source table, in order to retain them
    // as-is in the destination schema
    List<Field> resultingFields = destinationFields.stream()
      .filter(field -> !sourceFieldMap.containsKey(field.getName()))
      .collect(Collectors.toList());

    // Add fields from the source table into the destination table
    // We need to ensure that fields in the destination table that are Nullable remain as such, even when the source
    // field defines this field as required, so we need  to relax any fields in the source table that are already
    // defined in the destination table.
    sourceFieldMap.values().stream()
      .map(sourceField -> {
        String fieldName = sourceField.getName();
        if (destinationFieldMap.containsKey(fieldName)) {
          Field destinationField = destinationFieldMap.get(fieldName);

          // If the field in the destination table is nullable, but not in the source table, we need to set update
          // field's mode to NULLABLE in order to ensure the underlying schema doesn't change.
          if (destinationField.getMode() == Field.Mode.NULLABLE && sourceField.getMode() == Field.Mode.REQUIRED) {
            sourceField = sourceField.toBuilder().setMode(Field.Mode.NULLABLE).build();
          }
        }

        return sourceField;
      })
      .forEach(resultingFields::add);

    return resultingFields;
  }

  private static BigQueryTableFieldSchema generateTableFieldSchema(Schema.Field field) {
    BigQueryTableFieldSchema fieldSchema = new BigQueryTableFieldSchema();
    fieldSchema.setName(field.getName());
    fieldSchema.setMode(getMode(field.getSchema()).name());
    LegacySQLTypeName type = getTableDataType(field.getSchema());
    fieldSchema.setType(type.name());
    if (type == LegacySQLTypeName.RECORD) {
      List<Schema.Field> schemaFields;
      Schema fieldCdapSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());

      // If its an Array of records we need to get the component schema of the array
      // which will be the Record. Which can itself be nullable, and then get the fields
      // of that record.
      if (Schema.Type.ARRAY == fieldCdapSchema.getType()) {
        schemaFields = Objects.requireNonNull(
          BigQueryUtil.getNonNullableSchema(fieldCdapSchema.getComponentSchema()).getFields());
      } else {
        schemaFields = fieldCdapSchema.getFields();
      }
      fieldSchema.setFields(Objects.requireNonNull(schemaFields).stream()
                              .map(BigQuerySinkUtils::generateTableFieldSchema)
                              .collect(Collectors.toList()));

    }
    return fieldSchema;
  }

  public static com.google.cloud.bigquery.Schema convertCdapSchemaToBigQuerySchema(Schema schema) {
    List<Schema.Field> inputFields = Objects.requireNonNull(schema.getFields(), "Schema must have fields");
    List<com.google.cloud.bigquery.Field> fields = inputFields.stream()
      .map(BigQuerySinkUtils::convertCdapFieldToBigQueryField)
      .collect(Collectors.toList());
    return com.google.cloud.bigquery.Schema.of(fields);
  }

  private static Field convertCdapFieldToBigQueryField(Schema.Field field) {
    String name = field.getName();
    LegacySQLTypeName type = getTableDataType(field.getSchema());
    Field.Mode mode = getMode(field.getSchema());

    Field.Builder fieldBuilder;

    // For record fields, we need to get all subfields and re-create the builder.
    if (type == LegacySQLTypeName.RECORD) {
      List<Schema.Field> schemaFields;
      Schema fieldCdapSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());

      // If its an Array of records we need to get the component schema of the array
      // which will be the Record. Which can itself be nullable, and then get the fields
      // of that record.
      if (Schema.Type.ARRAY == fieldCdapSchema.getType()) {
        schemaFields = Objects.requireNonNull(
          BigQueryUtil.getNonNullableSchema(fieldCdapSchema.getComponentSchema()).getFields());
      } else {
        schemaFields = fieldCdapSchema.getFields();
      }

      FieldList subFields = FieldList.of(Objects.requireNonNull(schemaFields).stream()
                                           .map(BigQuerySinkUtils::convertCdapFieldToBigQueryField)
                                           .collect(Collectors.toList()));

      fieldBuilder = Field.newBuilder(name, type, subFields);
    } else {
      fieldBuilder = Field.newBuilder(name, type);
    }

    fieldBuilder.setMode(mode);

    // Set precision for numeric fields
    if (type == LegacySQLTypeName.NUMERIC || type == LegacySQLTypeName.BIGNUMERIC) {
      Schema decimalFieldSchema = BigQueryUtil.getNonNullableSchema(field.getSchema());
      fieldBuilder.setPrecision((long) decimalFieldSchema.getPrecision());
      fieldBuilder.setScale((long) decimalFieldSchema.getScale());
    }

    return fieldBuilder.build();
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

  /**
   * This function returns the LegacySQLTypeName that maps to the given CDAP Schema.
   * If the CDAP Schema is an Array it will return the LegacySQLTypename of the components.
   */
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
          // Following the restrictions given by:
          // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
          if ((schema.getScale() <= Numeric.SCALE) && (schema.getPrecision() <= Numeric.PRECISION) &&
            ((schema.getPrecision() - schema.getScale()) <= (Numeric.PRECISION - Numeric.SCALE))) {
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
      // If the field name is not in english characters, then we will use json format
      // We do this as the avro load job in BQ does not support non-english characters in field names for now
      String fieldName = field.getName();
      final String englishCharactersRegex = "[\\w]+";
      if (!Pattern.matches(englishCharactersRegex, fieldName)) {
        return BigQueryFileFormat.NEWLINE_DELIMITED_JSON;
      }
      // If the field is a record we have to check its subfields.
      if (RECORD.equals(field.getType())) {
        if (getFileFormat(field.getFields()) == BigQueryFileFormat.NEWLINE_DELIMITED_JSON) {
          return BigQueryFileFormat.NEWLINE_DELIMITED_JSON;
        }
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

  public static String generateUpdateUpsertQuery(Operation operation,
                                                 TableId sourceTableId,
                                                 TableId destinationTableId,
                                                 List<String> tableFieldsList,
                                                 List<String> tableKeyList,
                                                 List<String> orderedByList,
                                                 String partitionFilter) {

    String source = String.format("`%s.%s.%s`",
                                  sourceTableId.getProject(),
                                  sourceTableId.getDataset(),
                                  sourceTableId.getTable());
    String destination = String.format("`%s.%s.%s`",
                                       destinationTableId.getProject(),
                                       destinationTableId.getDataset(),
                                       destinationTableId.getTable());

    String criteria = tableKeyList.stream().map(s -> String.format(CRITERIA_TEMPLATE, s, s))
      .collect(Collectors.joining(" AND "));
    criteria = partitionFilter != null ? String.format("(%s) AND %s",
                                                       formatPartitionFilter(partitionFilter), criteria) : criteria;
    String fieldsForUpdate = tableFieldsList.stream().filter(s -> !tableKeyList.contains(s))
      .map(s -> String.format(CRITERIA_TEMPLATE, s, s)).collect(Collectors.joining(", "));

    orderedByList = orderedByList.stream().map(s -> s.trim()).map(s -> {
      StringBuilder sb = new StringBuilder("`").append(s.split(" ")[0]).append("` ");
      if (s.split(" ").length > 1) {
        sb.append(s.split(" ", 2)[1]);
      }
      return sb.toString();
    }).collect(Collectors.toList());

    String orderedBy = orderedByList.isEmpty() ? "" : " ORDER BY " + String.join(", ", orderedByList);
    String sourceTable = String.format(SOURCE_DATA_QUERY, "`" + String.join("`, `", tableKeyList) + "`",
                                       orderedBy, source);
    switch (operation) {
      case UPDATE:
        return String.format(UPDATE_QUERY, destination, fieldsForUpdate, sourceTable, criteria);
      case UPSERT:
        String insertFields = "`" + String.join("`, `", tableFieldsList) + "`";
        // if all fields are keys we update no field and only insert new rows
        if (fieldsForUpdate.isEmpty()) {
          return String.format(INSERT_ONLY_UPSERT_QUERY, destination, sourceTable, criteria,
                               insertFields, insertFields);
        }
        return String.format(UPSERT_QUERY, destination, sourceTable, criteria, fieldsForUpdate,
                             insertFields, insertFields);
      default:
        return "";
    }
  }

  private static String formatPartitionFilter(String partitionFilter) {
    String[] queryWords = partitionFilter.split(" ");
    int index = 0;
    for (String word : queryWords) {
      if (COMPARISON_OPERATORS.contains(word.toUpperCase())) {
        queryWords[index - 1] = queryWords[index - 1].replace(queryWords[index - 1],
                                                              "T." + queryWords[index - 1]);
      }
      index++;
    }
    return String.join(" ", queryWords);
  }

  /**
   * Generates Big Query field instances based on given CDAP table schema after schema validation.
   *
   * @param bigQuery              big query object
   * @param tableName             table name
   * @param tableSchema           table schema
   * @param allowSchemaRelaxation if schema relaxation policy is allowed
   * @param datasetProject        project name of dataset
   * @param dataset               dataset name
   * @param isTruncateTableSet    to truncate the table before writing or not while inserting
   * @param collector             failure collector
   * @return list of Big Query fields
   */
  public static List<BigQueryTableFieldSchema> getBigQueryTableFields(BigQuery bigQuery, String tableName,
                                                                      @Nullable Schema tableSchema,
                                                                      boolean allowSchemaRelaxation,
                                                                      String datasetProject,
                                                                      String dataset, boolean isTruncateTableSet,
                                                                      FailureCollector collector) {
    if (tableSchema == null) {
      return Collections.emptyList();
    }

    TableId tableId = TableId.of(datasetProject, dataset, tableName);
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
   * @param isTruncateTableSet    to truncate the table before writing or not while inserting
   * @param dataset               dataset name
   * @param collector             failure collector
   */
  public static void validateSchema(
    String tableName,
    com.google.cloud.bigquery.Schema bqSchema,
    @Nullable Schema tableSchema,
    boolean allowSchemaRelaxation,
    boolean isTruncateTableSet,
    String dataset,
    FailureCollector collector) {
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
   * Validates output schema against Big Query table schema for Insert operation. It throws
   * {@link IllegalArgumentException}
   * if the output schema has more fields than Big Query table or output schema field types does not match
   * Big Query column types unless schema relaxation policy is allowed.
   *
   * @param table                 table name
   * @param tableSchema           configured table schema
   * @param allowSchemaRelaxation allows schema relaxation policy
   * @param isTruncateTableSet    to truncate the table before writing or not while inserting
   * @param dataset               dataset name
   * @param collector             failure collector
   */
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

  /**
   * Returns list of duplicated fields (case insensitive)
   *
   * @param schemaFields List of schema fields
   * @return return set of duplicate fields
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

  /**
   * returns whether any logical type is supported or not in BigQuery.
   *
   * @param logicalType logical type of schema field
   * @return boolean value
   */
  public static boolean isSupportedLogicalType(Schema.LogicalType logicalType) {
    if (logicalType != null) {
      return logicalType == Schema.LogicalType.DATE || logicalType == Schema.LogicalType.TIMESTAMP_MICROS ||
        logicalType == Schema.LogicalType.TIMESTAMP_MILLIS || logicalType == Schema.LogicalType.DECIMAL;
    }
    return false;
  }

  /**
   * returns a Map, if fields are coming multiple times in fieldList.
   *
   * @param values
   * @return Map containing Field as key and occurrence as value
   */
  public static Map<String, Integer> calculateDuplicates(List<String> values) {
    return values.stream()
      .map(v -> v.split(" ")[0])
      .collect(Collectors.toMap(p -> p, p -> 1, (x, y) -> x + y));
  }

  /**
   * Record the lineage for the plugin
   *
   * @param context     Batch sink context
   * @param asset       asset object of table
   * @param tableSchema schema of table
   * @param fieldNames  field list
   * @param operationNameSuffix suffix for operation name, if applicable
   */
  public static void recordLineage(BatchSinkContext context,
                                   Asset asset,
                                   Schema tableSchema,
                                   List<String> fieldNames, @Nullable String operationNameSuffix) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      String operationName = operationNameSuffix == null ? "Write" : "Write_To_" + operationNameSuffix;
      lineageRecorder.recordWrite(operationName, "Wrote to BigQuery table.", fieldNames);
    }
  }

  /**
   * Get the list of fields that are of type JSON from the BigQuery schema.
   * @param bqSchema BigQuery schema.
   * @return comma separated list of fields that are of type JSON.
   */
  public static String getJsonStringFieldsFromBQSchema(com.google.cloud.bigquery.Schema bqSchema) {
    ArrayList<String> fields = new ArrayList<>();
    getJsonStringFieldsFromBQSchema(bqSchema.getFields(), fields, new ArrayList<>());
    return String.join(",", fields);
  }
  private static void getJsonStringFieldsFromBQSchema(FieldList fieldList,
                                                      ArrayList<String> fields, ArrayList<String> path) {
    for (Field field : fieldList) {
      path.add(field.getName());
      if (field.getType() == LegacySQLTypeName.RECORD) {
        getJsonStringFieldsFromBQSchema(field.getSubFields(), fields, path);
      } else if (field.getType().equals(LegacySQLTypeName.valueOf(JSON))) {
        fields.add(String.join(".", path));
      }
      path.remove(path.size() - 1);
    }
  }
}
