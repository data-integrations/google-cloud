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
import io.cdap.e2e.pages.actions.CdfConnectionActions;
import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.utils.PubSubClient;
import io.cdap.plugin.utils.SpannerClient;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {

  public static boolean beforeAllFlag = true;
  public static String gcsSourceBucketName = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceTable = StringUtils.EMPTY;
  public static String bqSourceView = StringUtils.EMPTY;
  public static String pubSubTargetTopic = StringUtils.EMPTY;
  public static String spannerInstance = StringUtils.EMPTY;
  public static String spannerDatabase = StringUtils.EMPTY;
  public static String spannerSourceTable = StringUtils.EMPTY;
  public static String spannerTargetDatabase = StringUtils.EMPTY;
  public static String spannerTargetTable = StringUtils.EMPTY;
  public static boolean firstSpannerTestFlag = true;
  public static String datasetName = PluginPropertyUtils.pluginProp("dataset");

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
          String serviceAccountJSON = System.getenv("SERVICE_ACCOUNT_JSON").replaceAll("[\r\n]+", " ");
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

  @Before(order = 1, value = "@GCS_DATATYPE_TEST")
  public static void createBucketWithDataTypeTestFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsDataTypeTestFile"));
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

  @Before(order = 1, value = "@GCS_DELETE_WILDCARD_TEST")
  public static void createBucketWithRecursiveTestFiles2() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFilesAndFolder(PluginPropertyUtils.pluginProp("gcsReadWildcardPath"));
  }

  @Before(order = 1, value = "@GCS_CSV_RANGE_TEST")
  public static void createBucketWithRangeCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsCsvRangeFile"));
  }

  @Before(order = 1, value = "@GCS_DELETE_MULTIPLE_BUCKETS_TEST")
  public static void createMultipleBucketsWithRecursiveTestFiles() throws IOException, URISyntaxException {
    gcsSourceBucketName =
      createMultipleGCSBucketsWithMultipleFiles(PluginPropertyUtils.pluginProp("gcsReadRecursivePath"),
                                                PluginPropertyUtils.pluginProp("bucketNumber"));
  }

  @Before(order = 1, value = "@GCS_PARQUET_TEST")
  public static void createBucketWithParquetTestFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsParquetFile"));
  }

  @Before(order = 1, value = "@GCS_AVRO_TEST")
  public static void createBucketWithAvroTestFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsAvroFile"));
  }

  @After(order = 1, value = "@GCS_CSV_TEST or @GCS_TSV_TEST or @GCS_BLOB_TEST " +
    "or @GCS_DELIMITED_TEST or @GCS_TEXT_TEST or @GCS_OUTPUT_FIELD_TEST or @GCS_DATATYPE_1_TEST or " +
    "@GCS_DATATYPE_2_TEST or @GCS_READ_RECURSIVE_TEST or @GCS_DELETE_WILDCARD_TEST or @GCS_CSV_RANGE_TEST or" +
    " @GCS_PARQUET_TEST or @GCS_AVRO_TEST or @GCS_DATATYPE_TEST or @GCS_AVRO_FILE")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(gcsSourceBucketName);
    PluginPropertyUtils.removePluginProp("gcsSourceBucketName");
    PluginPropertyUtils.removePluginProp("gcsSourcePath");
    gcsSourceBucketName = StringUtils.EMPTY;
  }

  @After(order = 1, value = "@GCS_DELETE_MULTIPLE_BUCKETS_TEST")
  public static void deleteMultipleSourceBucketsWithFile() {
    String[] bucketNames = gcsSourceBucketName.split(",");
    for (int index = 0; index < bucketNames.length; index++) {
      deleteGCSBucket(bucketNames[index]);
    }
    PluginPropertyUtils.removePluginProp("gcsSourceBucketName");
    PluginPropertyUtils.removePluginProp("gcsSourcePath");
    gcsSourceBucketName = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() {
    gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
    PluginPropertyUtils.addPluginProp("gcsTargetBucketName", gcsTargetBucketName);
    PluginPropertyUtils.addPluginProp("gcsTargetPath", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @Before(order = 1, value = "@GCS_SINK_EXISTING_BUCKET_TEST")
  public static void createTargetGCSBucketWithCSVFile() throws IOException, URISyntaxException {
    gcsTargetBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsCsvFile"));
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST or @GCS_SINK_EXISTING_BUCKET_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(gcsTargetBucketName);
    PluginPropertyUtils.removePluginProp("gcsTargetBucketName");
    PluginPropertyUtils.removePluginProp("gcsTargetPath");
    gcsTargetBucketName = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    bqTargetTable = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetTable);
      PluginPropertyUtils.removePluginProp("bqTargetTable");
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
   * Id | Value | UID
   * 22 | 968   | 245308db-6088-4db2-a933-f0eea650846a
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    StringBuilder records = new StringBuilder(StringUtils.EMPTY);
    for (int index = 2; index <= 25; index++) {
      records.append(" (").append(index).append(", ").append((int) (Math.random() * 1000 + 1)).append(", '")
        .append(UUID.randomUUID()).append("'), ");
    }
    BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ " +
                                        " STRUCT(1 AS Id, " + ((int) (Math.random() * 1000 + 1)) + " as Value, " +
                                        "'" + UUID.randomUUID() + "' as UID), " +
                                        records +
                                        "  (26, " + ((int) (Math.random() * 1000 + 1)) + ", " +
                                        "'" + UUID.randomUUID() + "') " +
                                        "])");
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST or @BQ_PARTITIONED_SOURCE_TEST or @BQ_SOURCE_DATATYPE_TEST or " +
    "@BQ_INSERT_SOURCE_TEST or @BQ_UPDATE_SINK_TEST" + "@BQ_EXISTING_SOURCE_TEST or @BQ_EXISTING_SINK_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    BigQueryClient.dropBqQuery(bqSourceTable);
    PluginPropertyUtils.removePluginProp("bqSourceTable");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    bqSourceTable = StringUtils.EMPTY;
  }

  /**
   * Create BigQuery partitioned table(transaction_id INT64, transaction_uid STRING, transaction_date DATE)
   * containing random testdata.
   * Sample row:
   * transaction_id | transaction_uid                       | transaction_date
   * 1              | 51c76c5c-543c-4066-8032-f4870f9e9a0b  | 2022-01-31
   */
  @Before(order = 1, value = "@BQ_PARTITIONED_SOURCE_TEST")
  public static void createTempPartitionedSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceTable + "` " +
                                        "(transaction_id INT64, transaction_uid STRING, transaction_date DATE ) " +
                                        "PARTITION BY _PARTITIONDATE");
    try {
      BigQueryClient.getSoleQueryResult("INSERT INTO `" + datasetName + "." + bqSourceTable + "` " +
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
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 2, value = "@BQ_SOURCE_VIEW_TEST")
  public static void createSourceBQViewWithQueries() throws IOException, InterruptedException {
    createSourceBQViewWithQueries(PluginPropertyUtils.pluginProp("bqCreateViewQueryFile"));
  }

  @After(order = 2, value = "@BQ_SOURCE_VIEW_TESTT")
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
    String bucketName = StorageClient.createBucket("00000000-e2e-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    PluginPropertyUtils.addPluginProp("gcsSourceBucketName", bucketName);
    PluginPropertyUtils.addPluginProp("gcsSourcePath", "gs://" + bucketName + "/" + filePath);
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

  private static String createMultipleGCSBucketsWithMultipleFiles(String folderPath, String bucketNumber)
    throws IOException, URISyntaxException {
    int bucketN = Integer.valueOf(bucketNumber);
    List<File> files = Files.list(Paths.get(StorageClient.class.getResource("/" + folderPath).toURI()))
      .filter(Files::isRegularFile)
      .map(Path::toFile)
      .collect(Collectors.toList());
    List<String> bucketNames = new ArrayList<>();
    for (int i = 0; i < bucketN; i++) {
      bucketNames.add(StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName());
    }
    for (File file : files) {
      String filePath = folderPath + "/" + file.getName();
      for (String bucketName : bucketNames) {
        StorageClient.uploadObject(bucketName, filePath, filePath);
      }
    }
    for (String bucketName : bucketNames) {
      BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing "
                                     + files.size() + " files in " + folderPath);
    }
    return String.join(",", bucketNames);
  }


  private static String createGCSBucketWithFilesAndFolder(String folderPath) throws IOException, URISyntaxException {
    List<String> folderPaths = Arrays.asList(folderPath.split(","));
    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    int fileCount = 0;
    for (String fp : folderPaths) {
      List<File> files = Files.list(Paths.get(StorageClient.class.getResource("/" + fp).toURI()))
        .filter(Files::isRegularFile)
        .map(Path::toFile)
        .collect(Collectors.toList());
      for (File file : files) {
        String filePath = fp + "/" + file.getName();
        StorageClient.uploadObject(bucketName, filePath, filePath);
      }
      fileCount += files.size();
    }
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing "
                                   + fileCount + " files in " + folderPaths.get(0));
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

  /**
   * Create Spanner Instance with 1 database containing sample tables
   * with queries provided in file spannerTestDataCreateTableQueriesFile and spannerTestDataInsertDataQueriesFile.
   */
  @Before(order = 1, value = "@SPANNER_TEST")
  public static void createTempSpannerInstance() throws InterruptedException, ExecutionException {
    if (firstSpannerTestFlag) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          SpannerClient.deleteInstance(spannerInstance);
          BeforeActions.scenario.write("Spanner instance" + spannerInstance + " deleted successfully");
          spannerInstance = StringUtils.EMPTY;
        } catch (Exception e) {
          if (e.getMessage().contains("NOT FOUND")) {
            BeforeActions.scenario.write("Spanner instance " + spannerInstance + " does not exist.");
          }
        }
      }));
      firstSpannerTestFlag = false;

      //Create spanner instance
      String spannerInstanceId = "e2e-" + (new SimpleDateFormat("yyyyMMdd-HH-mm-ssSSS").format(new Date()))
        + (int) (Math.random() * 10000 + 1);
      try {
        spannerInstance = SpannerClient
          .createInstance(spannerInstanceId, PluginPropertyUtils.pluginProp("spannerInstanceRegion"))
          .getDisplayName();
        BeforeActions.scenario.write("Spanner instance " + spannerInstance + " created successfully");
      } catch (ExecutionException e) {
        if (e.getMessage().contains("Instance already exists")) {
          spannerInstance = spannerInstanceId;
          BeforeActions.scenario.write("Spanner instance " + spannerInstanceId);
        } else {
          throw e;
        }
      }

      //Create Spanner DB with empty tables
      spannerSourceTable = PluginPropertyUtils.pluginProp("spannerSourceTable");
      List<String> listOfCreateTableQueries = new ArrayList<>();
      try {
        listOfCreateTableQueries = Files.readAllLines(Paths.get(TestSetupHooks.class.getResource
          ("/" + PluginPropertyUtils.pluginProp("spannerTestDataCreateTableQueriesFile")).toURI()));
      } catch (Exception e) {
        BeforeActions.scenario.write("Exception in reading "
                                       + PluginPropertyUtils.pluginProp("spannerTestDataCreateTableQueriesFile")
                                       + " - " + e.getMessage());
        Assert.fail("Exception in Spanner testdata prerequisite setup - error in reading create table queries file "
                      + e.getMessage());
      }
      spannerDatabase = "e2e-source-db-" + UUID.randomUUID().toString().substring(0, 10);
      SpannerClient.createDatabase(spannerInstance, spannerDatabase, listOfCreateTableQueries);
      BeforeActions.scenario.write("Spanner source DB " + spannerDatabase + " created successfully");

      //Insert data into table
      String insertQuery = StringUtils.EMPTY;
      try {
        insertQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
          ("/" + PluginPropertyUtils.pluginProp("spannerTestDataInsertDataQueriesFile")).toURI()))
          , StandardCharsets.UTF_8);
      } catch (Exception e) {
        BeforeActions.scenario.write("Exception in reading "
                                       + PluginPropertyUtils.pluginProp("spannerTestDataInsertDataQueriesFile")
                                       + " - " + e.getMessage());
        Assert.fail("Exception in Spanner testdata prerequisite setup " +
                      "- error in reading insert queries file " + e.getMessage());
      }
      SpannerClient.executeDMLQuery(spannerInstance, spannerDatabase, insertQuery);
      PluginPropertyUtils.addPluginProp("spannerInstance", spannerInstance);
      PluginPropertyUtils.addPluginProp("spannerDatabase", spannerDatabase);
      PluginPropertyUtils.addPluginProp("spannerSourceTable", spannerSourceTable);
      BeforeActions.scenario.write("Spanner source table " + spannerSourceTable + " created successfully");
    } else {
      BeforeActions.scenario.write("Spanner instance - " + spannerInstance);
    }
  }

  @Before(order = 2, value = "@SPANNER_SOURCE_BASIC_TEST")
  public static void createSpannerSourceBasicTable() {
    String insertQuery = StringUtils.EMPTY;
    try {
      insertQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("spannerTestDataInsertBasicDataQueriesFile")).toURI()))
        , StandardCharsets.UTF_8);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading "
                                     + PluginPropertyUtils.pluginProp("spannerTestDataInsertBasicDataQueriesFile")
                                     + " - " + e.getMessage());
      Assert.fail("Exception in Spanner testdata prerequisite setup " +
                    "- error in reading insert queries file " + e.getMessage());
    }
    SpannerClient.executeDMLQuery(spannerInstance, spannerDatabase, insertQuery);
    PluginPropertyUtils.addPluginProp("spannerSourceTable",
                                      PluginPropertyUtils.pluginProp("spannerSourceBasicTable"));
    BeforeActions.scenario.write("Spanner source table " + PluginPropertyUtils.pluginProp("spannerSourceBasicTable")
                                   + " created successfully");
  }

  @After(order = 2, value = "@SPANNER_SOURCE_BASIC_TEST")
  public static void resetSpannerSourceTable() {
    PluginPropertyUtils.addPluginProp("spannerSourceTable", spannerSourceTable);
  }

  @Before(order = 2, value = "@SPANNER_SINK_TEST")
  public static void setTempTargetSpannerDBAndTableName() {
    spannerTargetDatabase = spannerDatabase;
    spannerTargetTable = "e2e_target_table_" + UUID.randomUUID().toString().substring(0, 10).replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("spannerTargetDatabase", spannerTargetDatabase);
    PluginPropertyUtils.addPluginProp("spannerTargetTable", spannerTargetTable);
    BeforeActions.scenario.write("Spanner Target db name - " + spannerTargetDatabase);
    BeforeActions.scenario.write("Spanner Target table name - " + spannerTargetTable);
  }

  @After(order = 2, value = "@SPANNER_SINK_TEST")
  public static void emptyTempTargetSpannerDBAndTable() {
    PluginPropertyUtils.removePluginProp("spannerTargetDatabase");
    PluginPropertyUtils.removePluginProp("spannerTargetTable");
    spannerTargetDatabase = StringUtils.EMPTY;
    spannerTargetTable = StringUtils.EMPTY;
  }

  @Before(order = 2, value = "@SPANNER_SINK_NEWDB_TEST")
  public static void setTempTargetSpannerNewDBAndTableName() {
    spannerTargetDatabase = "e2e-target-db-" + UUID.randomUUID().toString().substring(0, 10);
    spannerTargetTable = "e2e_target_table_" + UUID.randomUUID().toString().substring(0, 10).replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("spannerTargetDatabase", spannerTargetDatabase);
    PluginPropertyUtils.addPluginProp("spannerTargetTable", spannerTargetTable);
    BeforeActions.scenario.write("Spanner Target db name - " + spannerTargetDatabase);
    BeforeActions.scenario.write("Spanner Target table name - " + spannerTargetTable);
  }

  @After(order = 2, value = "@SPANNER_SINK_NEWDB_TEST")
  public static void emptyTempTargetSpannerNewDBAndTable() {
    PluginPropertyUtils.removePluginProp("spannerTargetDatabase");
    PluginPropertyUtils.removePluginProp("spannerTargetTable");
    spannerTargetDatabase = StringUtils.EMPTY;
    spannerTargetTable = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_CONNECTION")
  public static void setGCSConnectionName() {
    PluginPropertyUtils.addPluginProp("gcsConnectionName", "GCS-" + UUID.randomUUID());
  }

  @After(order = 1, value = "@GCS_CONNECTION")
  public static void removeGCSConnectionName() {
    PluginPropertyUtils.removePluginProp("gcsConnectionName");
  }

  @Before(order = 1, value = "@EXISTING_GCS_CONNECTION")
  public static void addGCSConnection() throws IOException {
    String connectionName = "GCS-" + UUID.randomUUID();
    navigateToConnectionPageAndAddCommonProperties(connectionName, "gcsConnectionRow");
    testAndCreateConnection(connectionName);
    PluginPropertyUtils.addPluginProp("gcsConnectionName", connectionName);
  }

  @After(order = 1, value = "@EXISTING_GCS_CONNECTION")
  public static void deleteGCSConnection() throws IOException {
    deleteConnection("GCS", "gcsConnectionName");
    PluginPropertyUtils.removePluginProp("gcsConnectionName");
  }

  @Before(order = 1, value = "@BQ_CONNECTION")
  public static void setBQConnectionName() {
    PluginPropertyUtils.addPluginProp("bqConnectionName", "BQ-" + UUID.randomUUID());
  }

  @After(order = 1, value = "@BQ_CONNECTION")
  public static void removeBQConnectionName() {
    PluginPropertyUtils.removePluginProp("bqConnectionName");
  }

  @Before(order = 1, value = "@EXISTING_BQ_CONNECTION")
  public static void addBQConnection() throws IOException {
    String connectionName = "BQ-" + UUID.randomUUID();
    navigateToConnectionPageAndAddCommonProperties(connectionName, "bqConnectionRow");
    CdfPluginPropertiesActions.enterValueInInputProperty("datasetProjectId", "projectId");
    testAndCreateConnection(connectionName);
    PluginPropertyUtils.addPluginProp("bqConnectionName", connectionName);
  }

  @After(order = 1, value = "@EXISTING_BQ_CONNECTION")
  public static void deleteBQConnection() throws IOException {
    deleteConnection("BigQuery", "bqConnectionName");
    PluginPropertyUtils.removePluginProp("bqConnectionName");
  }

  @Before(order = 1, value = "@SPANNER_CONNECTION")
  public static void setSpannerConnectionName() {
    PluginPropertyUtils.addPluginProp("spannerConnectionName", "Spanner-" + UUID.randomUUID());
  }

  @After(order = 1, value = "@SPANNER_CONNECTION")
  public static void removeSpannerConnectionName() throws IOException {
    PluginPropertyUtils.removePluginProp("spannerConnectionName");
  }

  @Before(order = 1, value = "@EXISTING_SPANNER_CONNECTION")
  public static void addSpannerConnection() throws IOException {
    String connectionName = "Spanner-" + UUID.randomUUID();
    navigateToConnectionPageAndAddCommonProperties(connectionName, "spannerConnectionRow");
    testAndCreateConnection(connectionName);
    PluginPropertyUtils.addPluginProp("spannerConnectionName", connectionName);
  }

  @After(order = 1, value = "@EXISTING_SPANNER_CONNECTION")
  public static void deleteSpannerConnection() throws IOException {
    deleteConnection("Spanner", "spannerConnectionName");
    PluginPropertyUtils.removePluginProp("spannerConnectionName");
  }

  private static void navigateToConnectionPageAndAddCommonProperties(String connectionName, String connectionType)
    throws IOException {
    CdfConnectionActions.openWranglerConnectionsPage();
    CdfPluginPropertiesActions.clickPluginPropertyButton("addConnection");
    CdfPluginPropertiesActions.clickPluginPropertyElement(connectionType);
    CdfPluginPropertiesActions.enterValueInInputProperty("name", connectionName);
    CdfPluginPropertiesActions.replaceValueInInputProperty("projectId", "projectId");
    CdfPluginPropertiesActions.overrideServiceAccountDetailsInWranglerConnectionPageIfProvided();
  }

  private static void testAndCreateConnection(String connectionName) {
    CdfPluginPropertiesActions.clickPluginPropertyButton("testConnection");
    CdfConnectionActions.verifyTheTestConnectionIsSuccessful();
    CdfPluginPropertiesActions.clickPluginPropertyButton("connectionCreate");
    CdfConnectionActions.verifyConnectionIsCreatedSuccessfully(connectionName);
  }

  private static void deleteConnection(String connectionType, String connectionName) throws IOException {
    CdfConnectionActions.openWranglerConnectionsPage();
    CdfConnectionActions.expandConnections(connectionType);
    CdfConnectionActions.openConnectionActionMenu(connectionType, connectionName);
    CdfConnectionActions.selectConnectionAction(connectionType, connectionName, "Delete");
    CdfPluginPropertiesActions.clickPluginPropertyButton("Delete");
  }

  @Before(order = 2, value = "@BQ_EXECUTE_SQL")
  public static void replaceTableDetailsInQuery() {
    replaceTableDetailsInQuery("bqExecuteQuery", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_SQL")
  public static void setQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteQuery");
  }

  @Before(order = 2, value = "@BQ_EXECUTE_INSERT_SQL")
  public static void replaceTableDetailsInInsertQuery() {
    replaceTableDetailsInQuery("bqExecuteDMLInsert", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_INSERT_SQL")
  public static void setInsertQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteDMLInsert");
  }

  /**
   * Create BigQuery table with 3 columns (Id - Int, ProjectId - String, Dataset - string).
   * Sample row:
   * Id | ProjectId   | Dataset
   * 1  | cdf-athena  | test-automation
   */
  @Before(order = 1, value = "@BQ_SOURCE_BQ_EXECUTE_TEST")
  public static void createBQTableForBQExecuteTest() throws IOException, InterruptedException {
    String bqSourceBQExecuteTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceBQExecuteTable + "` as " +
                                        "SELECT * FROM UNNEST([ " +
                                        " STRUCT(1 AS Id, '" + PluginPropertyUtils.pluginProp("projectId")
                                        + "' as ProjectId, " +
                                        "'" + datasetName + "' as Dataset)" + "])");
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceBQExecuteTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceBQExecuteTable + " " +
                                   "for @BQ_SOURCE_BQ_EXECUTE_TEST created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_BQ_EXECUTE_TEST")
  public static void deleteBQTableForBQExecuteTest() throws IOException, InterruptedException {
    try {
      String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
      BigQueryClient.dropBqQuery(bqSourceTable);
      PluginPropertyUtils.removePluginProp("bqSourceTable");
      BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ source Table " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
  }
  }

  @Before(order = 2, value = "@BQ_EXECUTE_ROW_AS_ARG_SQL")
  public static void replaceTableDetailsInRowAsArgQuery() {
    replaceTableDetailsInQuery("bqExecuteRowAsArgQuery", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_ROW_AS_ARG_SQL")
  public static void setRowAsArgQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteRowAsArgQuery");

  }

  @Before(order = 1, value = "@BQ_EXECUTE_DDL_CREATE_TEST")
  public static void setTempCreateBQTableName() {
    PluginPropertyUtils.addPluginProp("bqExecuteCreateTable"
      , "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_"));
    replaceTableDetailsInQuery("bqExecuteDDLCreate", "bqExecuteCreateTable");
  }

  @After(order = 1, value = "@BQ_EXECUTE_DDL_CREATE_TEST")
  public static void deleteTempCreateBQTable() throws IOException, InterruptedException {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteDDLCreate");
    String bqExecuteTable = PluginPropertyUtils.pluginProp("bqExecuteCreateTable");
    try {
      BigQueryClient.dropBqQuery(bqExecuteTable);
      PluginPropertyUtils.removePluginProp("bqExecuteCreateTable");
      BeforeActions.scenario.write("BQ Execute created Target table - " + bqExecuteTable + " deleted successfully");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Execute created Target table " + bqExecuteTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Before(order = 2, value = "@BQ_EXECUTE_DDL_DROP_TEST")
  public static void replaceTableDetailsInDDLDropQuery() {
    replaceTableDetailsInQuery("bqExecuteDDLDrop", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_DDL_DROP_TEST")
  public static void setDDLDropQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteDDLDrop");
  }

  private static void replaceTableDetailsInQuery(String queryProperty, String tableProperty) {
    String bqExecuteQuery = PluginPropertyUtils.pluginProp(queryProperty);
    PluginPropertyUtils.addPluginProp("tempStore" + queryProperty, bqExecuteQuery);
    bqExecuteQuery = bqExecuteQuery
      .replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
      .replace("PROJECT_NAME", PluginPropertyUtils.pluginProp("projectId"))
      .replace("TABLENAME", PluginPropertyUtils.pluginProp(tableProperty));
    PluginPropertyUtils.addPluginProp(queryProperty, bqExecuteQuery);
  }

  private static void setQueryBackWithTableDetailsPlaceholder(String queryProperty) {
    PluginPropertyUtils.addPluginProp(queryProperty, PluginPropertyUtils.pluginProp("tempStore" + queryProperty));
    PluginPropertyUtils.removePluginProp("tempStore" + queryProperty);
  }

  public static int getBigQueryRecordsCountByQuery(String table, String countQuery)
    throws IOException, InterruptedException {
    replaceTableDetailsInQuery(countQuery, table);
    Optional<String> result = BigQueryClient.getSoleQueryResult(PluginPropertyUtils.pluginProp(countQuery));
    setQueryBackWithTableDetailsPlaceholder(countQuery);
    return result.map(Integer::parseInt).orElse(0);
  }

  @Before(order = 2, value = "@BQ_EXECUTE_UPSERT_SQL")
  public static void replaceTableDetailsInUpsertQuery() {
    replaceTableDetailsInQuery("bqExecuteDMLUpsert", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_UPSERT_SQL")
  public static void setUpsertQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteDMLUpsert");
  }

  @Before(order = 2, value = "@BQ_EXECUTE_UPDATE_SQL")
  public static void replaceTableDetailsInUpdateQuery() {
    replaceTableDetailsInQuery("bqExecuteDMLUpdate", "bqSourceTable");
  }

  @After(order = 2, value = "@BQ_EXECUTE_UPDATE_SQL")
  public static void setUpdateQueryBackWithTableDetailsPlaceholder() {
    setQueryBackWithTableDetailsPlaceholder("bqExecuteDMLUpdate");
  }

  @Before(order = 1, value = "@BQ_INSERT_SOURCE_TEST")
  public static void createSourceBQInsertTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-" , "_");
    io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceTable + "` " +
                                                          "(PersonID INT64, Name STRING, " + "Money FLOAT64," +
                                                          "Person_Exists BOOL ) ");
    try {
      io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("INSERT INTO `" + datasetName + "." + bqSourceTable + "` " +
                                                            "(PersonID,  Name, Money, Person_Exists)" +
                                                            "VALUES" + "(5, 'Raja', 500.0, true)");

    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }
  @Before(order = 1, value = "@BQ_UPDATE_SINK_TEST")
  public static void createSourceBQUpdateTable() throws IOException, InterruptedException {

    bqTargetTable = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTable);
    io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqTargetTable + "` " +
                                                          "(PersonID INT64, Name STRING, " + "Money FLOAT64," +
                                                          "Person_Exists BOOL ) ");

    try {
      io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("INSERT INTO `" + datasetName + "." + bqTargetTable + "` " +
                                                            "(PersonID,  Name, Money, Person_Exists)" +
                                                            "VALUES" + "(5, 'Roy', 500.0, true)");

    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }

    PluginPropertyUtils.addPluginProp(" bqTargetTable",  bqTargetTable);
    BeforeActions.scenario.write("BQ Target Table " +  bqTargetTable + " updated successfully");
  }

  @Before(order = 1, value = "@BQ_EXISTING_SOURCE_TEST")
  public static void createSourceBQExistingTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-" , "_");
    io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceTable + "` " +
                                                          "(ID INT64, Name STRING, " + "Price FLOAT64," +
                                                          "Customer_Exists BOOL ) ");
    try {
      io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("INSERT INTO `" + datasetName + "." + bqSourceTable + "` " +
                                                            "(ID, Name, Price, Customer_Exists)" +
                                                            "VALUES" + "(1, 'Raja Sharma', 200.0, true)");

    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }
  @Before(order = 1, value = "@BQ_EXISTING_SINK_TEST")
  public static void createSinkBQExistingTable() throws IOException, InterruptedException {

    bqTargetTable = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTable);
    io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqTargetTable + "` " +
                                                          "(ID INT64,Name STRING," +
                                                          "Price FLOAT64, Customer_Exists BOOL ) ");

    try {
      io.cdap.e2e.utils.BigQueryClient.getSoleQueryResult("INSERT INTO `" + datasetName + "." + bqTargetTable + "` " +
                                                            "(ID,  Name, Price, Customer_Exists)" +
                                                            "VALUES" + "(3, 'Rajan Kumar', 100.0, true)");

    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }

    PluginPropertyUtils.addPluginProp(" bqTargetTable",  bqTargetTable);
    BeforeActions.scenario.write("BQ Target Table " +  bqTargetTable + " updated successfully");
  }
  @Before(order = 1, value = "@GCS_AVRO_FILE")
  public static void createGcsBucketWithAvro() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("gcsAvroAllDataFile"));
  }
}
