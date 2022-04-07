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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {

  public static String gcsSourceBucketName = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceTable = StringUtils.EMPTY;
  public static String bqSourceView = StringUtils.EMPTY;
  public static String pubSubTargetTopic = StringUtils.EMPTY;

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

  @Before(order = 1, value = "@GCS_DATATYPE_1_TEST")
  public static void createBucketWithDataTypeTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsDataTypeTest1File"));
  }

  @Before(order = 1, value = "@GCS_DATATYPE_2_TEST")
  public static void createBucketWithDataTypeTest2File() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsDataTypeTest2File"));
  }

  @Before(order = 1, value = "@GCS_READ_RECURSIVE_TEST")
  public static void createBucketWithRecursiveTestFiles() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithMultipleFiles(PluginPropertyUtils.pluginProp("gcsReadRecursivePath"));
  }

  @Before(order = 1, value = "@GCS_CSV_RANGE_TEST")
  public static void createBucketWithRangeCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsCsvRangeFile"));
  }

  @After(order = 1, value = "@GCS_CSV_TEST or @GCS_TSV_TEST or @GCS_BLOB_TEST " +
    "or @GCS_DELIMITED_TEST or @GCS_TEXT_TEST or @GCS_OUTPUT_FIELD_TEST or @GCS_DATATYPE_1_TEST or " +
    "@GCS_DATATYPE_2_TEST or @GCS_READ_RECURSIVE_TEST or @GCS_CSV_RANGE_TEST")
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

  @After(order = 1, value = "@BQ_SOURCE_TEST or @BQ_PARTITIONED_SOURCE_TEST or @BQ_SOURCE_DATATYPE_TEST")
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

  /**
   * Create BigQuery table with different list of dataypes (transaction_info BOOL, transaction_num BYTES,
   * transaction_uid STRING, transaction_date DATE, transaction_dt DATETIME, transaction_time TIME, latitude FLOAT64,
   * unique_key INT64, business_ratio NUMERIC, updated_on TIMESTAMP, parent ARRAY<STRING>,
   * inputs STRUCT<input_script_bytes BYTES, input_script_string STRING, input_sequence_number INT64>,
   * business_bigratio BIGNUMERIC, committer STRUCT<name STRING, email STRING, time_sec INT64, tz_offset INT64,
   * date STRUCT<seconds INT64, nanos INT64>>, trailer ARRAY<STRUCT<key STRING, value STRING, email STRING>>,
   * difference ARRAY<STRUCT<old_mode INT64, new_mode INT64, old_path STRING, new_path STRING, old_sha1 STRING,
   * new_sha1 STRING, old_repo STRING, new_repo STRING>>, Userdata STRUCT<age INT64, company STRING>)
   * containing random testdata.
   */
  @Before(order = 1, value = "@BQ_SOURCE_DATATYPE_TEST")
  public static void createSourceBQTableWithDifferentDataTypes() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("bqCreateTableQueryFile"),
                                   PluginPropertyUtils.pluginProp("bqInsertDataQueryFile"));
  }

  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile) throws
    IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");

    String createTableQuery = StringUtils.EMPTY;
    try {
      createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqCreateTableQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create table query file " + e.getMessage());
    }

    String insertDataQuery = StringUtils.EMPTY;
    try {
      insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqInsertDataQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      insertDataQuery = insertDataQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading insert data query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createTableQuery);
    try {
      BigQueryClient.getSoleQueryResult(insertDataQuery);
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 2, value = "@BQ_SOURCE_VIEW_TEST")
  public static void createSourceBQViewWithQueries() throws IOException, InterruptedException {
    createSourceBQViewWithQueries(PluginPropertyUtils.pluginProp("bqCreateViewQueryFile"));
  }

  @After(order = 2, value = "@BQ_SOURCE_VIEW_TEST")
  public static void deleteTempSourceBQView() throws IOException, InterruptedException {
    BigQueryClient.getSoleQueryResult("DROP VIEW IF EXISTS " + PluginPropertyUtils.pluginProp("dataset") +
                                        "." + bqSourceView);
    BeforeActions.scenario.write("BQ source View " + bqSourceView + " deleted successfully");
    bqSourceView = StringUtils.EMPTY;
  }

  private static void createSourceBQViewWithQueries(String bqCreateViewQueryFile) throws
    IOException, InterruptedException {
    bqSourceView = "E2E_SOURCE_VIEW" + UUID.randomUUID().toString().replaceAll("-", "_");

    String createViewQuery = StringUtils.EMPTY;
    try {
      createViewQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqCreateViewQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      createViewQuery = createViewQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable).replace("VIEW_NAME", bqSourceView);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateViewQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create view query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createViewQuery);
    BeforeActions.scenario.write("BQ Source View " + bqSourceView + " created successfully");
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

  private static String createGCSBucketWithMultipleFiles(String folderPath) throws IOException, URISyntaxException {
    List<File> files = Files.list(Paths.get(StorageClient.class.getResource("/" + folderPath).toURI()))
      .filter(Files::isRegularFile)
      .map(Path::toFile)
      .collect(Collectors.toList());

    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    for (File file : files) {
      String filePath = folderPath + "/" + file.getName();
      StorageClient.uploadObject(bucketName, filePath, filePath);
    }
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing "
                                   + files.size() + " files in " + folderPath);
    return bucketName;
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
