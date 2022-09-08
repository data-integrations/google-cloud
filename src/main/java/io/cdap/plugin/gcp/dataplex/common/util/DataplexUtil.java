/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.common.util;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.DataplexServiceSettings;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.Job;
import com.google.cloud.dataplex.v1.JobName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.MetadataServiceSettings;
import com.google.cloud.dataplex.v1.Partition;
import com.google.cloud.dataplex.v1.Schema.Mode;
import com.google.cloud.dataplex.v1.Schema.PartitionField;
import com.google.cloud.dataplex.v1.Schema.PartitionStyle;
import com.google.cloud.dataplex.v1.Schema.SchemaField;
import com.google.cloud.dataplex.v1.Schema.Type;
import com.google.cloud.dataplex.v1.TaskName;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Common Util class for dataplex source and sink plugins.
 */
public final class DataplexUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DataplexUtil.class);
  private static final String SERVICE_ACCOUNT_JSON = "google.cloud.auth.service.account.json";
  private static final String SERVICE_ACCOUNT_KEYFILE = "google.cloud.auth.service.account.json.keyfile";

  /**
   * Converts Entity Schema into a CDAP Schema object.
   *
   * @param dataplexTableSchema Dataplex Schema to be converted.
   * @param collector           Failure collector to collect failure messages for the client.
   * @return CDAP schema object
   */
  public static Schema getTableSchema(com.google.cloud.dataplex.v1.Schema dataplexTableSchema,
                                      @Nullable FailureCollector collector) {
    List<com.google.cloud.dataplex.v1.Schema.SchemaField> fields = dataplexTableSchema.getFieldsList();
    List<Schema.Field> schemaFields = new ArrayList<>();

    for (com.google.cloud.dataplex.v1.Schema.SchemaField field : fields) {
      Schema.Field schemaField = getPartitionField(field, collector, null);
      // if schema field is null, that means that there was a validation error. We will still continue in order to
      // collect more errors
      if (schemaField == null) {
        continue;
      }
      schemaFields.add(schemaField);
    }
    // Add partitioned fields to schema
    if (dataplexTableSchema.getPartitionFieldsList() != null &&
      !dataplexTableSchema.getPartitionFieldsList().isEmpty()) {
      for (com.google.cloud.dataplex.v1.Schema.PartitionField partitionField :
        dataplexTableSchema.getPartitionFieldsList()) {
        Schema.Field schemaField = getPartitionField(partitionField, collector, null);
        // if schema field is null, that means that there was a validation error. We will still continue in order to
        // collect more errors
        if (schemaField == null) {
          continue;
        }
        schemaFields.add(schemaField);
      }
    }
    if (!schemaFields.isEmpty() && collector != null && !collector.getValidationFailures().isEmpty()) {
      // throw if there was validation failure(s) added to the collector
      collector.getOrThrowException();
    }
    if (schemaFields.isEmpty()) {
      return null;
    }
    return Schema.recordOf("output", schemaFields);
  }

  /**
   * Converts Entity schema field into a corresponding CDAP Schema.Field.
   *
   * @param field        BigQuery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to prepend to recordNames to make them unique
   * @return A CDAP schema field
   */
  @Nullable
  private static Schema.Field getPartitionField(com.google.cloud.dataplex.v1.Schema.SchemaField field,
                                                @Nullable FailureCollector collector,
                                                @Nullable String recordPrefix) {
    Schema schema = convertFieldTypeForPartitionFields(field, collector, recordPrefix);
    if (schema == null) {
      return null;
    }

    com.google.cloud.dataplex.v1.Schema.Mode mode =
      field.getMode() == null ? com.google.cloud.dataplex.v1.Schema.Mode.NULLABLE : field.getMode();
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
   * Converts Entity field type into a CDAP field type.
   *
   * @param field        Bigquery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to add before a record name to ensure unique names.
   * @return A CDAP field schema
   */
  @Nullable
  private static Schema convertFieldTypeForPartitionFields(com.google.cloud.dataplex.v1.Schema.SchemaField field,
                                                           @Nullable FailureCollector collector,
                                                           @Nullable String recordPrefix) {
    com.google.cloud.dataplex.v1.Schema.Type standardType = field.getType();
    // if type is RECORD, need to read the subFields and add to schema.
    if (standardType.equals(com.google.cloud.dataplex.v1.Schema.Type.RECORD)) {
      List<com.google.cloud.dataplex.v1.Schema.SchemaField> fields = field.getFieldsList();
      List<Schema.Field> schemaFields = new ArrayList<>();

      // Record names have to be unique as Avro doesn't allow to redefine them.
      // We can make them unique by prepending the previous records names to their name.
      String recordName = "";
      if (recordPrefix != null) {
        recordName = recordPrefix + '.';
      }
      recordName = recordName + field.getName();

      for (com.google.cloud.dataplex.v1.Schema.SchemaField f : fields) {
        Schema.Field schemaField = getPartitionField(f, collector, recordName);
        // if schema field is null, that means that there was a validation error. We will still continue in order to
        // collect more errors
        if (schemaField == null) {
          continue;
        }
        schemaFields.add(schemaField);
      }
      // do not return schema for the struct field if none of the nested fields are of supported types
      if (!schemaFields.isEmpty()) {
        return Schema.recordOf(recordName, schemaFields);
      } else {
        return null;
      }
    } else {
      return convertFieldBasedOnStandardType(standardType, field.getName(), collector);
    }
  }

  /**
   * This method will return the schema of specific fields from dataplex type schema to cdap specific schema.
   *
   * @param standardType Dataplex Schema type
   * @param fieldName    fieldname of the schema field
   * @param collector    FailureCollector
   * @return
   */
  private static Schema convertFieldBasedOnStandardType(com.google.cloud.dataplex.v1.Schema.Type standardType,
                                                        String fieldName, FailureCollector collector) {
    switch (standardType) {
      case FLOAT:
      case DOUBLE:
        // float is a float64, so corresponding type becomes double
        return Schema.of(Schema.Type.DOUBLE);
      case BOOLEAN:
        return Schema.of(Schema.Type.BOOLEAN);
      case BYTE:
      case INT16:
      case INT32:
        return Schema.of(Schema.Type.INT);
      case INT64:
        return Schema.of(Schema.Type.LONG);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case BINARY:
        return Schema.of(Schema.Type.BYTES);
      case TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case DECIMAL:
        // bigquery has Numeric.PRECISION digits of precision and Numeric.SCALE digits of scale for NUMERIC.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        return Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION, BigQueryTypeSize.Numeric.SCALE);
      case NULL:
        return Schema.of(Schema.Type.NULL);
      default:
        String error =
          String.format("Entity column '%s' is of unsupported type '%s'.", fieldName, standardType.name());
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
   * Converts Entity schema for partitioned field into a corresponding CDAP Schema.Field.
   *
   * @param field        Partitioned field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to prepend to recordNames to make them unique
   * @return A CDAP schema field
   */
  @Nullable
  private static Schema.Field getPartitionField(com.google.cloud.dataplex.v1.Schema.PartitionField field,
                                                @Nullable FailureCollector collector,
                                                @Nullable String recordPrefix) {
    Schema schema = convertFieldTypeForPartitionFields(field, collector, recordPrefix);
    if (schema == null) {
      return null;
    }
    return Schema.Field.of(field.getName(), Schema.nullableOf(schema));
  }

  /**
   * Converts Entity Partitioned field type into a CDAP field type.
   *
   * @param field        Partitioned field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to add before a record name to ensure unique names.
   * @return A CDAP field schema
   */
  @Nullable
  private static Schema convertFieldTypeForPartitionFields(com.google.cloud.dataplex.v1.Schema.PartitionField field,
                                                           @Nullable FailureCollector collector,
                                                           @Nullable String recordPrefix) {
    com.google.cloud.dataplex.v1.Schema.Type standardType = field.getType();
    return convertFieldBasedOnStandardType(standardType, field.getName(), collector);
  }

  /**
   * This method will check the dataplex job completion. If it is not completed, it will wait until it got completed or
   * failed.
   *
   * @param conf contains all the configuration properties.
   * @throws IOException
   */
  public static void getJobCompletion(Configuration conf) throws IOException {
    GoogleCredentials googleCredentials = getCredentialsFromConfiguration(conf);
    String projectID = conf.get(DataplexConstants.DATAPLEX_PROJECT_ID);
    String location = conf.get(DataplexConstants.DATAPLEX_LOCATION);
    String lake = conf.get(DataplexConstants.DATAPLEX_LAKE);
    String taskId = conf.get(DataplexConstants.DATAPLEX_TASK_ID);
    try (DataplexServiceClient dataplexServiceClient = DataplexUtil.getDataplexServiceClient(googleCredentials)) {
      DataplexServiceClient.ListJobsPagedResponse
        jobList = dataplexServiceClient.listJobs(TaskName.newBuilder().setProject(projectID).setLake(lake).
                                                   setLocation(location).setTask(taskId).build());
      Job dataplexJob = jobList.iterateAll().iterator().next();
      try {
        Awaitility.await()
          .atMost(30, TimeUnit.MINUTES)
          .pollInterval(15, TimeUnit.SECONDS)
          .pollDelay(5, TimeUnit.SECONDS)
          .until(() -> {
            Job currentJob =
              dataplexServiceClient.getJob(JobName.newBuilder().setProject(projectID).setLocation(location)
                                             .setLake(lake).setTask(taskId).setJob(dataplexJob.getUid()).build());
            LOG.debug("State of the Job is still " + currentJob.getState());
            return currentJob.getState() != null && !Job.State.RUNNING.equals(currentJob.getState()) &&
              !Job.State.STATE_UNSPECIFIED.equals(currentJob.getState());
          });
      } catch (ConditionTimeoutException e) {
        throw new IOException("Job timed out.", e);
      }
      Job completedJob = dataplexServiceClient.getJob(JobName.newBuilder().setProject(projectID).setLocation(location)
                                                        .setLake(lake).setTask(taskId).setJob(dataplexJob.getUid())
                                                        .build());
      if (!Job.State.SUCCEEDED.equals(completedJob.getState())) {
        throw new IOException("Job failed with message: " + completedJob.getMessage());
      }
      // Default input dir path
      String outputLocation = conf.get(FileInputFormat.INPUT_DIR);
      // Output location path for dataplex job will be in format
      // 'projects/projectId/locations/locationName/lakes/lakeName/tasks/taskName/jobId/0/'
      // Need to remove 'jobs/' and add '/0/' to get the actual location
      outputLocation = outputLocation + completedJob.getName().replace("/jobs/", "/") + "-0/0/";
      conf.set(FileInputFormat.INPUT_DIR, outputLocation);
    }
  }

  private static GoogleCredentials getCredentialsFromConfiguration(Configuration configuration) throws IOException {
    String serviceAccount;
    String serviceAccountType = configuration.get(DataplexConstants.SERVICE_ACCOUNT_TYPE);
    Boolean isServiceAccountJson = false;
    if (serviceAccountType == GCPConnectorConfig.SERVICE_ACCOUNT_JSON) {
      isServiceAccountJson = true;
      serviceAccount = configuration.get(SERVICE_ACCOUNT_JSON);
    } else {
      serviceAccount = configuration.get(SERVICE_ACCOUNT_KEYFILE);
    }
    String filePath = configuration.get(DataplexConstants.SERVICE_ACCOUNT_FILEPATH);
    return getCredentialsFromServiceAccount(isServiceAccountJson, filePath, serviceAccount);
  }

  private static GoogleCredentials getCredentialsFromServiceAccount(Boolean isServiceAccountJson, String filePath,
                                                                    String serviceAccount) throws IOException {
    if (isServiceAccountJson || (filePath != null && !filePath.equalsIgnoreCase(DataplexConstants.NONE))) {
      return GCPUtils.loadServiceAccountCredentials(serviceAccount, !isServiceAccountJson);
    } else {
      return GoogleCredentials.getApplicationDefault();
    }
  }

  /**
   * Return DataplexServiceClient to call dataplex methods.
   *
   * @param credentials GoogleCredentials
   * @return DataplexServiceClient
   */
  public static DataplexServiceClient getDataplexServiceClient(GoogleCredentials credentials) throws IOException {
    DataplexServiceSettings dataplexServiceSettings = null;
    DataplexServiceClient dataplexServiceClient = null;
    try {
      dataplexServiceSettings = DataplexServiceSettings.newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build();
      dataplexServiceClient = DataplexServiceClient.create(dataplexServiceSettings);
    } catch (IOException e) {
      LOG.error("Failed to get DataplexServiceClient. Check the credentials");
      throw new IOException("Failed to get DataplexServiceClient");
    }
    return dataplexServiceClient;
  }

  /**
   * Return MetadataServiceClient to call dataplex methods.
   *
   * @param credentials
   * @return MetadataServiceClient
   */
  public static MetadataServiceClient getMetadataServiceClient(GoogleCredentials credentials) throws IOException {
    MetadataServiceSettings metadataServiceSettings = null;
    MetadataServiceClient metadataServiceClient = null;
    try {
      metadataServiceSettings =
        MetadataServiceSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
          .build();
      metadataServiceClient =
        MetadataServiceClient.create(metadataServiceSettings);
    } catch (RuntimeException | IOException e) {
      LOG.error("Failed to get MetadataServiceClient. Check the credentials");
      throw new IOException("Failed to get DataplexServiceClient");
    }
    return metadataServiceClient;
  }

  /**
   * Get storage format for entity data.
   *
   * @param format
   */
  public static String getStorageFormatForEntity(String format) {
    switch (format) {
      case "avro":
        return "application/x-avro";
      case "csv":
        return "text/csv";
      case "json":
        return "application/json";
      case "orc":
        return "application/x-orc";
      case "parquet":
        return "application/x-parquet";
      default:
        return "undefined";
    }

  }

  /**
   * Return Dataplex Schema from CDAP Schema.
   *
   * @param schema input schema from cdap
   * @return MetadataServiceClient
   */
  public static com.google.cloud.dataplex.v1.Schema getDataplexSchema(Schema schema) throws IOException {
    com.google.cloud.dataplex.v1.Schema.Builder dataplexSchemaBuilder =
      com.google.cloud.dataplex.v1.Schema.newBuilder();
    dataplexSchemaBuilder.setUserManaged(true);
    if (schema == null) {
      return dataplexSchemaBuilder.build();
    }
    // Since "ts" is used by dataplex sink to create time partitioned layout on GCS, we should remove any
    // existing columns named "ts" to avoid conflict.
    dataplexSchemaBuilder.addAllFields(Objects.requireNonNull(schema.getFields()).stream()
      .filter(avroField -> !avroField.getName().equals(DataplexConstants.STORAGE_BUCKET_PARTITION_KEY))
      .map(DataplexUtil::toDataplexSchemaField).collect(Collectors.toList()));
    // Add partitioning scheme to the schema
    Schema.Field partitionField = Schema.Field.of(DataplexConstants.STORAGE_BUCKET_PARTITION_KEY, schema);
    dataplexSchemaBuilder.setPartitionStyle(PartitionStyle.HIVE_COMPATIBLE);
    dataplexSchemaBuilder.addPartitionFields(toDataplexPartitionField(partitionField));
    return dataplexSchemaBuilder.build();
  }

  private static SchemaField toDataplexSchemaField(
    Schema.Field avroField) {
    SchemaField.Builder fieldBuilder = SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(dataplexFieldType(avroField));
    fieldBuilder.setMode(dataplexFieldMode(avroField));
    if (avroField.getSchema().getType() == Schema.Type.RECORD) {
      // Handle nested records. Filtering "ts" column for the same reason in getDataplexSchema
      fieldBuilder.addAllFields(avroField.getSchema().getFields().stream()
        .filter(schemaField -> !schemaField.getName().equals(DataplexConstants.STORAGE_BUCKET_PARTITION_KEY))
        .map(DataplexUtil::toDataplexSchemaField).collect(Collectors.toList()));
    }
    return fieldBuilder.build();
  }

  private static PartitionField toDataplexPartitionField(
    Schema.Field avroField) {
    PartitionField.Builder partitionFieldBuilder = PartitionField.newBuilder();
    partitionFieldBuilder.setName(avroField.getName());
    // Dataplex supports only partition field type of String for files on GCS.
    partitionFieldBuilder.setType(Type.STRING);
    return partitionFieldBuilder.build();
}

  private static Mode dataplexFieldMode(Schema.Field field) {
    /*
     Field modes supported by Dataplex:

     MODE_UNSPECIFIED: Mode unspecified.
     REQUIRED: The field has required semantics.
     NULLABLE: The field has optional semantics, and may be null.
     REPEATED: The field has repeated (0 or more) semantics, and is a list of values.
    */

    Schema.Type type = field.getSchema().getType();
    if (type == Schema.Type.ARRAY) {
      return Mode.REPEATED;
    } else if (type == Schema.Type.UNION) {
      for (Schema innerSchema : field.getSchema().getUnionSchemas()) {
        if (innerSchema.getType() == Schema.Type.NULL) {
          return Mode.NULLABLE;
        }
      }
    }
    return Mode.REQUIRED;
  }

  private static Type dataplexFieldType(Schema.Field field) {
    /*
    Field types supported by Dataplex:

    TYPE_UNSPECIFIED SchemaType unspecified.
    BOOLEAN: Boolean field.
    BYTE: Single byte numeric field.
    INT16: 16-bit numeric field.
    INT32: 32-bit numeric field.
    INT64: 64-bit numeric field.
    FLOAT: Floating point numeric field.
    DOUBLE: Double precision numeric field.
    DECIMAL: Real value numeric field.
    STRING: Sequence of characters field.
    BINARY: Sequence of bytes field.
    TIMESTAMP: Date and time field.
    DATE: Date field.
    TIME: Time field.
    RECORD: Structured field. Nested fields that define the structure of the map. 
                      If all nested fields are nullable, this field represents a union.
    NULL: Null field that does not have values.
    */

    Schema schema = field.getSchema();

    if (schema.getType() == Schema.Type.UNION) {
      // Special case for UNION: a union of ["null", "non-null type"] means this is a
      // nullable field. In Dataplex this will be a field with Mode = NULLABLE and Type = <non-null
      // type>. So here we have to return the type of the other, non-NULL, element.
      // A union of 3+ elements is not supported (can't be represented as a Dataplex type).
      if (schema.isNullable()) {
        return dataplexPrimitiveFieldType(schema.getNonNullable());
      }
      return Type.TYPE_UNSPECIFIED;
    }

    if (schema.getType() == Schema.Type.ARRAY) {
      // Special case for ARRAY: check the type of the underlying elements.
      // In Dataplex this will be a field with Mode = REPEATED and Type = <array element type>.
      return dataplexPrimitiveFieldType(schema.getComponentSchema());
    }

    return dataplexPrimitiveFieldType(schema);
  }

  private static Type dataplexPrimitiveFieldType(Schema schema) {
    if (schema.getLogicalType() != null) {
      Type type = dataplexLogicalFieldType(schema);
      if (type != null) {
        return type;
      }
    }

    Schema.Type avroType = schema.getType();
    switch (avroType) {
      case RECORD:
        return Type.RECORD;
      case STRING:
        return Type.STRING;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case BOOLEAN:
        return Type.BOOLEAN;
      case NULL:
        return Type.NULL;
      case BYTES: // BYTES is binary data with variable size.
        return Type.BINARY;
      case INT:
        return Type.INT32;
      case LONG:
        return Type.INT64;
      case UNION: // Shouldn't happen. Unions can not contain other unions as per Avro spec.
      case ARRAY: // Not supported as a primitive type (e.g. if this is an ARRAY of ARRAYs).
      case MAP:
      case ENUM:
      default:
        return Type.TYPE_UNSPECIFIED;
    }
  }

  private static Type dataplexLogicalFieldType(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();

    if (logicalType == LogicalType.DECIMAL) {
      return Type.DECIMAL;
    } else if (logicalType == LogicalType.DATE) {
      return Type.DATE;
    } else if (logicalType == LogicalType.TIME_MICROS
      || logicalType == LogicalType.TIME_MILLIS) {
      return Type.TIME;
    } else if (logicalType == LogicalType.TIMESTAMP_MICROS
      || logicalType == LogicalType.TIMESTAMP_MILLIS) {
      return Type.TIMESTAMP;
    }

    return null;
  }

  /**
   * Add partition info to dataplex entity.
   *
   * @param entity dataplex entity
   * @param credentials Google Credentials
   * @param bucketName
   * @param tableName
   * @param project
   * @throws IOException
   */
  public static void addPartitionInfo(Entity entity,
                                      GoogleCredentials credentials,
                                      String bucketName, String tableName, String project) throws IOException {
    Storage storage = GCPUtils.getStorage(project, credentials);
    String delimiter = "/";
    String partitionPrefix = DataplexConstants.STORAGE_BUCKET_PARTITION_KEY + "=";
    Page<Blob> blobs =
      storage.list(
        bucketName,
        Storage.BlobListOption.prefix(tableName + delimiter + partitionPrefix),
        Storage.BlobListOption.currentDirectory());
    String lastPartition = null;
    // blob name example: bq_source_2/ts=2022-08-22-21-52/
    // Get the last blob's name in the iterator as it is the one that corresponds to the entity that was just
    // created/updated.
    for (Blob blob : blobs.iterateAll()) {
      lastPartition = blob.getName();
    }
    // Remove the delimiter from the end of the blob name for creating location string
    String location = DataplexConstants.STORAGE_BUCKET_PATH_PREFIX + bucketName +
      delimiter + lastPartition.substring(0, lastPartition.length() - 1);
    String[] lastPartitionParts = lastPartition.split(delimiter);
    Partition.Builder dataplexPartitionBuilder =
      Partition.newBuilder();
    try (MetadataServiceClient metadataServiceClient =
           getMetadataServiceClient(credentials)) {
      Partition partition = dataplexPartitionBuilder
        .setLocation(location)
        //extract the date value from blob name (e.g., 2022-08-22-21-52) to pass as partition value
        .addValues(lastPartitionParts[lastPartitionParts.length - 1].substring(partitionPrefix.length()))
        .build();
        metadataServiceClient.createPartition(entity.getName(), partition);
    }
  }
}
