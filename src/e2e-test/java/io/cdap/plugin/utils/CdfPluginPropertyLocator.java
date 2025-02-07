package io.cdap.plugin.utils;

import org.sparkproject.guava.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Plugin property data-cy attributes of elements which will be used to locate element by xpath.
 */
public enum CdfPluginPropertyLocator {
  PROJECT_ID("project"),
  DATASET_PROJECT_ID("datasetProject"),
  DATASET("dataset"),
  TABLE("table"),
  FORMAT("format"),
  PATH("path"),
  SAMPLE_SIZE("sampleSize"),
  DELIMITER("delimiter"),
  SKIP_HEADER("skipHeader"),
  SUFFIX("suffix"),
  CMEK_KEY("cmekKey"),
  SERVICE_ACCOUNT_TYPE("serviceAccountType"),
  SERVICE_ACCOUNT_PATH("serviceFilePath"),
  SERVICE_ACCOUNT_JSON("serviceAccountJSON"),
  TRUNCATE_TABLE("truncateTable"),
  UPDATE_TABLE_SCHEMA("allowSchemaRelaxation"),
  PUBSUB_TOPIC("topic"),
  PUBSUB_MAXIMUM_BATCH_COUNT("messageCountBatchSize"),
  PUBSUB_MAXIMUM_BATCH_SIZE("requestThresholdKB"),
  PUBSUB_PUBLISH_DELAY_THRESHOLD("publishDelayThresholdMillis"),
  PUBSUB_RETRY_TIMEOUT("retryTimeoutSeconds"),
  PUBSUB_ERROR_THRESHOLD("errorThreshold"),
  OUTPUT_SCHEMA_MACRO_INPUT("Output Schema-macro-input"),
  GCS_DELETE_OBJECTS_TO_DELETE("paths"),
  GCS_CREATE_OBJECTS_TO_CREATE("paths"),
  GCS_CREATE_FAIL_IF_OBJECT_EXISTS("failIfExists"),
  GCS_MOVE_SOURCE_PATH("sourcePath"),
  GCS_MOVE_DESTINATION_PATH("destPath"),
  PARTITION_START_DATE("partitionFrom"),
  PARTITION_END_DATE("partitionTo"),
  FILTER("filter");

  public String pluginProperty;
  CdfPluginPropertyLocator(String property) {
    this.pluginProperty = property;
  }

  private static final Map<String, CdfPluginPropertyLocator> CDF_PLUGIN_PROPERTY_MAPPING;
  static {
    CDF_PLUGIN_PROPERTY_MAPPING = new ImmutableMap.Builder<String, CdfPluginPropertyLocator>()
      .put("projectId", CdfPluginPropertyLocator.PROJECT_ID)
      .put("datasetProjectId", CdfPluginPropertyLocator.DATASET_PROJECT_ID)
      .put("dataset", CdfPluginPropertyLocator.DATASET)
      .put("table", CdfPluginPropertyLocator.TABLE)
      .put("format", CdfPluginPropertyLocator.FORMAT)
      .put("path", CdfPluginPropertyLocator.PATH)
      .put("sampleSize", CdfPluginPropertyLocator.SAMPLE_SIZE)
      .put("delimiter", CdfPluginPropertyLocator.DELIMITER)
      .put("skipHeader", CdfPluginPropertyLocator.SKIP_HEADER)
      .put("pathSuffix", CdfPluginPropertyLocator.SUFFIX)
      .put("encryptionKeyName", CdfPluginPropertyLocator.CMEK_KEY)
      .put("serviceAccountType", CdfPluginPropertyLocator.SERVICE_ACCOUNT_TYPE)
      .put("serviceAccountFilePath", CdfPluginPropertyLocator.SERVICE_ACCOUNT_PATH)
      .put("serviceAccountJSON", CdfPluginPropertyLocator.SERVICE_ACCOUNT_JSON)
      .put("truncateTable", CdfPluginPropertyLocator.TRUNCATE_TABLE)
      .put("updateTableSchema", CdfPluginPropertyLocator.UPDATE_TABLE_SCHEMA)
      .put("topic", CdfPluginPropertyLocator.PUBSUB_TOPIC)
      .put("maximumBatchCount", CdfPluginPropertyLocator.PUBSUB_MAXIMUM_BATCH_COUNT)
      .put("maximumBatchSize", CdfPluginPropertyLocator.PUBSUB_MAXIMUM_BATCH_SIZE)
      .put("publishDelayThreshold", CdfPluginPropertyLocator.PUBSUB_PUBLISH_DELAY_THRESHOLD)
      .put("retryTimeout", CdfPluginPropertyLocator.PUBSUB_RETRY_TIMEOUT)
      .put("errorThreshold", CdfPluginPropertyLocator.PUBSUB_ERROR_THRESHOLD)
      .put("outputSchema", CdfPluginPropertyLocator.OUTPUT_SCHEMA_MACRO_INPUT)
      .put("objectsToDelete", CdfPluginPropertyLocator.GCS_DELETE_OBJECTS_TO_DELETE)
      .put("objectsToCreate", CdfPluginPropertyLocator.GCS_CREATE_OBJECTS_TO_CREATE)
      .put("createFailIfObjectExists", CdfPluginPropertyLocator.GCS_CREATE_FAIL_IF_OBJECT_EXISTS)
      .put("gcsMoveSourcePath", CdfPluginPropertyLocator.GCS_MOVE_SOURCE_PATH)
      .put("gcsMoveDestinationPath", CdfPluginPropertyLocator.GCS_MOVE_DESTINATION_PATH)
      .put("filter", CdfPluginPropertyLocator.FILTER)
      .put("partitionFrom", CdfPluginPropertyLocator.PARTITION_START_DATE)
      .put("partitionTo", CdfPluginPropertyLocator.PARTITION_END_DATE)
      .build();
  }

  @Nullable
  public static CdfPluginPropertyLocator fromPropertyString(String property) {
    return CDF_PLUGIN_PROPERTY_MAPPING.get(property);
  }
}
