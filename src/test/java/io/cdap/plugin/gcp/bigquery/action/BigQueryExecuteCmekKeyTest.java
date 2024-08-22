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

package io.cdap.plugin.gcp.bigquery.action;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.junit.Assert;
import org.junit.Assume;
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
 *  BigQueryExecute Cmek Key unit test. This test will only be run when below property is provided:
 *  project.id -- the name of the project where staging bucket may be created or new resource needs to be created.
 *  It will default to active google project if you have google cloud client installed.
 *  service.account.file -- the path to the service account key file
 */
public class BigQueryExecuteCmekKeyTest {
  private static String serviceAccountKey;
  private static String project;
  private static String serviceAccountFilePath;
  private static String dataset;
  private static String table;
  private static BigQuery bigQuery;

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

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);
    Credentials credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountKey, false);
    bigQuery = GCPUtils.getBigQuery(project, credentials, null);
  }

  private BigQueryExecute.Config.Builder getBuilder() throws NoSuchFieldException {
    return BigQueryExecute.Config.builder()
      .setProject(project)
      .setDataset(dataset)
      .setTable(table);
  }

  @Test
  public void testServiceAccountPath() throws Exception {
    BigQueryExecute.Config.Builder builder = getBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_FILE_PATH)
      .setServiceFilePath(serviceAccountFilePath);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
    testCmekKeyWithExistingTable(builder);
  }

  @Test
  public void testServiceAccountJson() throws Exception {
    BigQueryExecute.Config.Builder builder = getBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_JSON)
      .setServiceAccountJson(serviceAccountKey);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
    testCmekKeyWithExistingTable(builder);
  }

  private void testValidCmekKey(BigQueryExecute.Config.Builder builder) {
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings/my_ring/cryptoKeys/test_key", project);
    MockFailureCollector collector = new MockFailureCollector();
    BigQueryExecute.Config config = builder
      .setCmekKey(cmekKey)
      .setLocation("key-location")
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private void testInvalidCmekKeyName(BigQueryExecute.Config.Builder builder) {
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings", project);
    MockFailureCollector collector = new MockFailureCollector();
    BigQueryExecute.Config config = builder
      .setCmekKey(cmekKey)
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testInvalidCmekKeyLocation(BigQueryExecute.Config.Builder builder) {
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings/my_ring/cryptoKeys/test_key", project);
    MockFailureCollector collector = new MockFailureCollector();
    BigQueryExecute.Config config = builder
      .setCmekKey(cmekKey)
      .setLocation("dataset-location")
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    //testing the default location of the bucket ("US") if location config is empty or null
    config = builder
      .setCmekKey(cmekKey)
      .setLocation("")
      .build();
    config.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testCmekKeyWithExistingTable(BigQueryExecute.Config.Builder builder) throws IOException {
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings/my_ring/cryptoKeys/test_key", project);
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    // Even though the location set in config is not same as of the cmek key but the validation should not fail because
    // the table already exists.
    BigQueryExecute.Config config = builder
      .setCmekKey(cmekKey)
      .setLocation("dataset-location")
      .build();
    DatasetId datasetId = DatasetId.of(project, dataset);
    // creating table before validating cmek key
    createTable(datasetId);
    config.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
    // deleting dataset after successful validation
    bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
  }

  private void createTable(DatasetId datasetId) throws IOException {
    BigQuerySinkUtils.createDatasetIfNotExists(bigQuery, datasetId, "us-east1", null,
                                               () -> String.format("Unable to create BigQuery dataset '%s.%s'",
                                                                   datasetId.getProject(), datasetId.getDataset()));
    Schema schema = Schema.of();
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    bigQuery.create(TableInfo.newBuilder(TableId.of(project, dataset, table), tableDefinition).build());
  }
}
