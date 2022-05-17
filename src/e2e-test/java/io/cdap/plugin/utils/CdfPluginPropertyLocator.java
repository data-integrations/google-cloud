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
  OUTPUT_SCHEMA_MACRO_INPUT("Output Schema-macro-input");

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
      .build();
  }

  @Nullable
  public static CdfPluginPropertyLocator fromPropertyString(String property) {
    return CDF_PLUGIN_PROPERTY_MAPPING.get(property);
  }
}
