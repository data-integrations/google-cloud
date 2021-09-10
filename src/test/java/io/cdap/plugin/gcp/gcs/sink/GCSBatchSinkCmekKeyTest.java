package io.cdap.plugin.gcp.gcs.sink;

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

import com.google.common.base.Strings;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import javax.annotation.Nullable;

/**
 * GCSBatchSink Cmek Key integration test. This test will only be run when below property is provided:
 * project.id -- the name of the project where staging bucket may be created or new resource needs to be created.
 * It will default to active google project if you have google cloud client installed.
 * bucket.path -- the path of the gcs bucket
 * service.account.file -- the path to the service account key file
 */
public class GCSBatchSinkCmekKeyTest {
  private static String serviceAccountKey;
  private static String project;
  private static String serviceAccountFilePath;
  private static String bucketPath;

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

    bucketPath = System.getProperty("bucket.path");
    Assume.assumeFalse(String.format(messageTemplate, "bucket path"), bucketPath == null);

    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);

  }

  private GCSBatchSink.GCSBatchSinkConfig getConfig(@Nullable String project, @Nullable String path,
                                                    @Nullable String serviceAccountType,
                                                    @Nullable String serviceFilePath,
                                                    @Nullable String serviceAccountJson) throws NoSuchFieldException {
    GCSBatchSink.GCSBatchSinkConfig gcsBatchSinkConfig = new GCSBatchSink.GCSBatchSinkConfig();
    FieldSetter
      .setField(gcsBatchSinkConfig, GCPConfig.class.getDeclaredField("project"), project);
    FieldSetter
      .setField(gcsBatchSinkConfig, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("path"), path);
    FieldSetter.setField(gcsBatchSinkConfig, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format")
      , "csv");
    FieldSetter
      .setField(gcsBatchSinkConfig, GCPReferenceSinkConfig.class.getDeclaredField("referenceName")
        , "testref");
    FieldSetter
      .setField(gcsBatchSinkConfig, GCPConfig.class.getDeclaredField("serviceAccountType"), serviceAccountType);
    if (!Strings.isNullOrEmpty(serviceFilePath)) {
      FieldSetter
        .setField(gcsBatchSinkConfig, GCPConfig.class.getDeclaredField("serviceFilePath"), serviceFilePath);
    }
    if (!Strings.isNullOrEmpty(serviceAccountJson)) {
      FieldSetter
        .setField(gcsBatchSinkConfig, GCPConfig.class.getDeclaredField("serviceAccountJson"), serviceAccountJson);
    }
    return gcsBatchSinkConfig;
  }

  @Test
  public void testServiceAccountPath() throws Exception {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig(project, bucketPath, "filePath",
                                                       serviceAccountFilePath, null);
    testValidCmekKey(config);
    testInvalidCmekKey(config);
  }

  @Test
  public void testServiceAccountJson() throws Exception {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig(project, bucketPath, "JSON",
                                                       null, serviceAccountKey);
    testValidCmekKey(config);
    testInvalidCmekKey(config);
  }

  public void testValidCmekKey(GCSBatchSink.GCSBatchSinkConfig config) throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("location"), "us-east1");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("cmekKey"),
                "projects/cdf-test-322418/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  public void testInvalidCmekKey(GCSBatchSink.GCSBatchSinkConfig config) throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("cmekKey"),
                "keyRings/my_ring/cryptoKeys/test_key");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("location"), "us");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("cmekKey"),
                "projects/cdf-test-322418/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key");
    config.validate(collector);
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
