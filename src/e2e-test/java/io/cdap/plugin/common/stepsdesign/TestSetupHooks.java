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
package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.utils.PubSubClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;
import stepsdesign.PipelineSteps;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {

  public static boolean beforeAllFlag = true;
  public static String gcsSourceBucketName = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceTable = StringUtils.EMPTY;
  public static String pubSubTargetTopic = StringUtils.EMPTY;

  @Before(order = 1)
  public static void overrideServiceAccountFilePathIfProvided() {
    if (beforeAllFlag) {
      String serviceAccountType = System.getenv("SERVICE_ACCOUNT_TYPE");
      if (serviceAccountType != null && !serviceAccountType.isEmpty()) {
        if (serviceAccountType.equalsIgnoreCase("FilePath")) {
          PluginPropertyUtils.addPluginProp("serviceAccountType", "filePath");
          String serviceAccountFilePath = System.getenv("SERVICE_ACCOUNT_FILE_PATH");
          if (!(serviceAccountFilePath == null) && !serviceAccountFilePath.equalsIgnoreCase("auto-detect")) {
            PluginPropertyUtils.addPluginProp("serviceAccount", serviceAccountFilePath);
          }
          return;
        }
        if (serviceAccountType.equalsIgnoreCase("JSON")) {
          PluginPropertyUtils.addPluginProp("serviceAccountType", "JSON");
          String serviceAccountJSON = System.getenv("SERVICE_ACCOUNT_JSON");
          if (!(serviceAccountJSON == null) && !serviceAccountJSON.equalsIgnoreCase("auto-detect")) {
            PluginPropertyUtils.addPluginProp("serviceAccount", serviceAccountJSON);
          }
          return;
        }
        Assert.fail("ServiceAccount override failed - ServiceAccount type set in environment variable " +
                      "'SERVICE_ACCOUNT_TYPE' with invalid value: '" + serviceAccountType + "'. " +
                      "Value should be either 'FilePath' or 'JSON'");
      }
      beforeAllFlag = false;
    }
  }

  @Before(order = 1, value = "@GCS_CSV_TEST")
  public static void createBucketWithCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsCsvFile"));
  }

  @Before(order = 1, value = "@GCS_TSV_TEST")
  public static void createBucketWithTSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsTsvFile"));
  }

  @Before(order = 1, value = "@GCS_BLOB_TEST")
  public static void createBucketWithBlobFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsBlobFile"));
  }

  @Before(order = 1, value = "@GCS_DELIMITED_TEST")
  public static void createBucketWithDelimitedFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsDelimitedFile"));
  }

  @Before(order = 1, value = "@GCS_TEXT_TEST")
  public static void createBucketWithTextFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsTextFile"));
  }

  @Before(order = 1, value = "@GCS_OUTPUT_FIELD_TEST")
  public static void createBucketWithOutputFieldFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsOutputFieldTestFile"));
  }

  @After(order = 1, value = "@GCS_CSV_TEST or @GCS_TSV_TEST or @GCS_BLOB_TEST " +
    "or @GCS_DELIMITED_TEST or @GCS_TEXT_TEST or @GCS_OUTPUT_FIELD_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(gcsSourceBucketName);
    gcsSourceBucketName = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() {
    gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(gcsTargetBucketName);
    gcsTargetBucketName = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    bqTargetTable = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetTable);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTable + " deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table with 3 columns (Id - Int, Value - Int, UID - string) containing random testdata.
   * Sample row:
   *   Id | Value | UID
   *   22 | 968   | 245308db-6088-4db2-a933-f0eea650846a
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    StringBuilder records = new StringBuilder(StringUtils.EMPTY);
    for (int index = 2; index <= 25; index++) {
      records.append(" (").append(index).append(", ").append((int) (Math.random() * (1000) + 1)).append(", '")
        .append(UUID.randomUUID()).append("'), ");
    }
    BigQueryClient.getSoleQueryResult("create table `test_automation." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ " +
                                        " STRUCT(1 AS Id, " + ((int) (Math.random() * (1000) + 1)) + " as Value, " +
                                        "'" + UUID.randomUUID() + "' as UID), " +
                                        records +
                                        "  (26, " + ((int) (Math.random() * (1000) + 1)) + ", " +
                                        "'" + UUID.randomUUID() + "') " +
                                        "])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST or @BQ_PARTITIONED_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    bqSourceTable = StringUtils.EMPTY;
  }

  /**
   * Create BigQuery partitioned table(transaction_id INT64, transaction_uid STRING, transaction_date DATE)
   * containing random testdata.
   * Sample row:
   *   transaction_id | transaction_uid                       | transaction_date
   *   1              | 51c76c5c-543c-4066-8032-f4870f9e9a0b  | 2022-01-31
   */
  @Before(order = 1, value = "@BQ_PARTITIONED_SOURCE_TEST")
  public static void createTempPartitionedSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
      BigQueryClient.getSoleQueryResult("create table `test_automation." + bqSourceTable + "` " +
                                          "(transaction_id INT64, transaction_uid STRING, transaction_date DATE ) " +
                                          "PARTITION BY _PARTITIONDATE");
    try {
      BigQueryClient.getSoleQueryResult("INSERT INTO `test_automation." + bqSourceTable + "` " +
                                          "(transaction_id, transaction_uid, transaction_date) " +
                                          "SELECT ROW_NUMBER() OVER(ORDER BY GENERATE_UUID()), GENERATE_UUID(), date " +
                                          "FROM UNNEST(GENERATE_DATE_ARRAY('2022-01-01', current_date())) AS date");
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  private static String createGCSBucketWithFile(String filePath) throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing " + filePath + " file");
    return bucketName;
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Before(order = 1, value = "@PUBSUB_SINK_TEST")
  public static void createTargetPubSubTopic() {
    pubSubTargetTopic = "cdf-e2e-test-" + UUID.randomUUID();
    BeforeActions.scenario.write("Target PubSub topic " + pubSubTargetTopic);
  }

  @After(order = 1, value = "@PUBSUB_SINK_TEST")
  public static void deleteTargetPubSubTopic() {
    try {
      PubSubClient.deleteTopic(pubSubTargetTopic);
      BeforeActions.scenario.write("Deleted target PubSub topic " + pubSubTargetTopic);
      pubSubTargetTopic = StringUtils.EMPTY;
    } catch (Exception e) {
      if (e.getMessage().contains("Invalid resource name given") || e.getMessage().contains("Resource not found")) {
        BeforeActions.scenario.write("Target PubSub topic " + pubSubTargetTopic + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
