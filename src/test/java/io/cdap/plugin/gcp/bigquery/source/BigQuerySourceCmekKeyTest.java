/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 *  BigQuerySource Cmek Key unit test. This test will only be run when below property is provided:
 *  project.id -- the name of the project where staging bucket may be created or new resource needs to be created.
 *  It will default to active google project if you have google cloud client installed.
 *  service.account.file -- the path to the service account key file
 */
public class BigQuerySourceCmekKeyTest {
  private static String serviceAccountKey;
  private static String project;
  private static String serviceAccountFilePath;
  private static String dataset;
  private static String table;
  private static String bucket;
  private static Storage storage;
  private static BigQuery bigQuery;
  private static final String location = "us-east1";

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    Assume.assumeFalse(String.format(messageTemplate, "project id"), project == null);
    System.setProperty("GCLOUD_PROJECT", project);

    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    dataset = String.format("bq_cmektest_%s", UUID.randomUUID().toString().replace("-", "_"));
    table = String.format("bq_cmektest_%s", UUID.randomUUID().toString().replace("-", "_"));
    bucket = String.format("bq-cmektest-%s", UUID.randomUUID());

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);
    Credentials credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountKey, false);
    storage = GCPUtils.getStorage(project, credentials);
    bigQuery = GCPUtils.getBigQuery(project, credentials);
  }

  @Before
  public void createTable() throws IOException {
    DatasetId datasetId = DatasetId.of(project, dataset);
    BigQuerySinkUtils.createDatasetIfNotExists(bigQuery, datasetId, location, null,
                                               () -> String.format("Unable to create BigQuery dataset '%s.%s'",
                                                                   datasetId.getProject(), datasetId.getDataset()));
    Schema schema = Schema.of();
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    bigQuery.create(TableInfo.newBuilder(TableId.of(project, dataset, table), tableDefinition).build());
  }

  @After
  public void deleteTable() {
    DatasetId datasetId = DatasetId.of(project, dataset);
    bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
  }

  private BigQuerySourceConfig.Builder getBuilder() {
    return BigQuerySourceConfig.builder()
      .setDataset(dataset)
      .setTable(table)
      .setBucket(bucket);
  }

  @Test
  public void testServiceAccountPath() throws Exception {
    BigQueryConnectorConfig connection = new
      BigQueryConnectorConfig(project, null, GCPConnectorConfig.SERVICE_ACCOUNT_FILE_PATH,
                              serviceAccountFilePath, null);
    BigQuerySourceConfig.Builder builder = getBuilder().setConnection(connection);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
    testCmekKeyWithExistingBucket(builder);
  }

  @Test
  public void testServiceAccountJson() throws Exception {
    BigQueryConnectorConfig connection = new
      BigQueryConnectorConfig(project, null, GCPConnectorConfig.SERVICE_ACCOUNT_JSON,
                              null, serviceAccountKey);
    BigQuerySourceConfig.Builder builder = getBuilder().setConnection(connection);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
    testCmekKeyWithExistingBucket(builder);
  }

  private void testValidCmekKey(BigQuerySourceConfig.Builder builder) {
    String cmekKey = String.format("projects/%s/locations/%s/keyRings/my_ring/cryptoKeys/test_key", project, location);
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySourceConfig config = builder
      .setCmekKey(cmekKey)
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private void testInvalidCmekKeyName(BigQuerySourceConfig.Builder builder) {
    String cmekKey = String.format("projects/%s/locations/%s/keyRings", project, location);
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySourceConfig config = builder
      .setCmekKey(cmekKey)
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testInvalidCmekKeyLocation(BigQuerySourceConfig.Builder builder) {
    String location = "us-west1";
    String cmekKey = String.format("projects/%s/locations/%s/keyRings/my_ring/cryptoKeys/test_key", project, location);
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySourceConfig config = builder
      .setCmekKey(cmekKey)
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
  
  private void testCmekKeyWithExistingBucket(BigQuerySourceConfig.Builder builder) {
    String location = "us-west1";
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings/my_ring/cryptoKeys/test_key", project);
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    // Even though the location of bucket and cmekKey is not same but the validation should not fail because
    // the bucket already exists.
    BigQuerySourceConfig config = builder
      .setCmekKey(cmekKey)
      .setBucket(bucket)
      .build();
    // creating bucket before validating cmek key
    GCPUtils.createBucket(storage, GCSPath.from(bucket).getBucket(), location, null);
    config.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
    // deleting bucket after successful validation
    storage.delete(GCSPath.from(bucket).getBucket());
  }
}
