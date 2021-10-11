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

package io.cdap.plugin.gcp.bigquery.sink;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConfig;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 *  BigQuerySink Cmek Key unit test. This test will only be run when below property is provided:
 *  project.id -- the name of the project where staging bucket may be created or new resource needs to be created.
 *  It will default to active google project if you have google cloud client installed.
 *  service.account.file -- the path to the service account key file
 */
public class BigQuerySinkCmekKeyTest {
  private static String serviceAccountKey;
  private static String project;
  private static String serviceAccountFilePath;
  private static String dataset;
  private static String table;

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

    dataset = UUID.randomUUID().toString();
    table = UUID.randomUUID().toString();

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);
  }

  private BigQuerySinkConfig.Builder getBuilder() throws NoSuchFieldException {
    String referenceName = "test-ref";
    return BigQuerySinkConfig.builder()
      .setReferenceName(referenceName)
      .setProject(project)
      .setDataset(dataset)
      .setTable(table);
  }

  @Test
  public void testServiceAccountPath() throws Exception {
    BigQuerySinkConfig.Builder builder = getBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_FILE_PATH)
      .setServiceFilePath(serviceAccountFilePath);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
  }

  @Test
  public void testServiceAccountJson() throws Exception {
    BigQuerySinkConfig.Builder builder = getBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_JSON)
      .setServiceAccountJson(serviceAccountKey);
    testValidCmekKey(builder);
    testInvalidCmekKeyName(builder);
    testInvalidCmekKeyLocation(builder);
  }

  private void testValidCmekKey(BigQuerySinkConfig.Builder builder) throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySinkConfig config = builder
      .setCmekKey(String.format("projects/%s/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key", project))
      .setLocation("us-east1")
      .build();
    config.validateCmekKey(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private void testInvalidCmekKeyName(BigQuerySinkConfig.Builder builder) throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySinkConfig config = builder
      .setCmekKey(String.format("projects/%s/locations/us-east1/keyRings", project))
      .build();
    config.validateCmekKey(collector);
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testInvalidCmekKeyLocation(BigQuerySinkConfig.Builder builder) throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySinkConfig config = builder
      .setCmekKey(String.format("projects/%s/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key", project))
      .setLocation("us")
      .build();
    config.validateCmekKey(collector);
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    //testing the default location of the bucket ("US") if location config is empty or null
    config = builder
      .setCmekKey(String.format("projects/%s/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key", project))
      .setLocation("")
      .build();
    config.validateCmekKey(collector);
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
