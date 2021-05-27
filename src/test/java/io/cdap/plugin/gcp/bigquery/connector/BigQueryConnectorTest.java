/*
 *
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

package io.cdap.plugin.gcp.bigquery.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.validation.SimpleFailureCollector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig.SERVICE_ACCOUNT_JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * BigQuery Connector integration test. This test will only be run when below property is provided:
 * project.id -- the name of the project where temporary table or staging bucket may be created. It will default to
 * active google project if you have google cloud client installed.
 * dataset.project -- optional, the name of the project where the dataset is
 * dataset.name -- the name of the dataset
 * table.name -- the name of the table
 * service.account.file-- the path to the service account key file
 */
public class BigQueryConnectorTest {
  private static String serviceAccountKey;
  private static String project;
  private static String datasetProject;
  private static String dataset;
  private static String table;
  private static String serviceAccountFilePath;


  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured, please refer to README for details.";

    project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    assumeFalse(String.format(messageTemplate, "project id"), project == null);
    System.setProperty("GCLOUD_PROJECT", project);

    datasetProject =  System.getProperty("dataset.project");

    dataset = System.getProperty("dataset.name");
    assumeFalse(String.format(messageTemplate, "dataset name"), dataset == null);

    table = System.getProperty("table.name");
    assumeFalse(String.format(messageTemplate, "table name"), table == null);

    serviceAccountFilePath = System.getProperty("service.account.file");
    assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
      StandardCharsets.UTF_8);
  }

  @Test
  public void testServiceAccountPath() throws IOException {
    BigQueryConnectorConfig config = new BigQueryConnectorConfig(project, datasetProject, null,
      serviceAccountFilePath, null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    BigQueryConnectorConfig config = new BigQueryConnectorConfig(project, datasetProject, SERVICE_ACCOUNT_JSON,
      null, serviceAccountKey);
    test(config);
  }

  private void test(BigQueryConnectorConfig config) throws IOException {
    BigQueryConnector connector = new BigQueryConnector(config);
    testTest(connector);
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector);
  }

  private void testGenerateSpec(BigQueryConnector connector) {
    ConnectorSpec connectorSpec =
      connector.generateSpec(ConnectorSpecRequest.builder().setPath(dataset + "/" + table).build());
    Map<String, String> properties = connectorSpec.getProperties();
    assertEquals(dataset, properties.get("dataset"));
    assertEquals(table, properties.get("table"));
  }

  private void testSample(BigQueryConnector connector) throws IOException {
    List<StructuredRecord> sample = connector.sample(SampleRequest.builder(1).setPath(dataset + "/" + table).build());
    assertEquals(1, sample.size());
    StructuredRecord record = sample.get(0);
    Schema schema = record.getSchema();
    assertNotNull(schema);
    for (Schema.Field field : schema.getFields()) {
      assertNotNull(field.getSchema());
      assertTrue(record.get(field.getName()) != null || field.getSchema().isNullable());
    }

    //invalid path
    assertThrows(IllegalArgumentException.class,
      () -> connector.sample(SampleRequest.builder(1).setPath("a/b/c").build()));

    //sample dataset
    assertThrows(IllegalArgumentException.class,
      () -> connector.sample(SampleRequest.builder(1).setPath(dataset).build()));
  }

  private void testBrowse(BigQueryConnector connector) throws IOException {
    // browse project
    BrowseDetail detail = connector.browse(BrowseRequest.builder("/").build());
    assertTrue(detail.getTotalCount() > 0);
    assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      assertEquals(BigQueryConnector.ENTITY_TYPE_DATASET, entity.getType());
      assertTrue(entity.canBrowse());
      assertFalse(entity.canSample());
    }

    // browse dataset
    detail = connector.browse(BrowseRequest.builder(dataset).build());
    assertTrue(detail.getTotalCount() > 0);
    assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      assertEquals(BigQueryConnector.ENTITY_TYPE_TABLE, entity.getType());
      assertFalse(entity.canBrowse());
      assertTrue(entity.canSample());
    }

    // browse table
    detail = connector.browse(BrowseRequest.builder(dataset + "/" + table).build());
    assertEquals(1, detail.getTotalCount());
    assertEquals(1, detail.getEntities().size());
    for (BrowseEntity entity : detail.getEntities()) {
      assertEquals(BigQueryConnector.ENTITY_TYPE_TABLE, entity.getType());
      assertFalse(entity.canBrowse());
      assertTrue(entity.canSample());
    }

    // invalid path
    assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder("a/b/c").build()));

    // not existing dataset
    assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder("/notexisting").build()));

    // not existing table
    assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder(dataset + "/notexisting").build()));
  }

  private void testTest(BigQueryConnector connector) {
    SimpleFailureCollector collector = new SimpleFailureCollector();
    connector.test(collector);
    ValidationException validationException = collector.getOrThrowException();
    assertTrue(validationException.getFailures().isEmpty());
  }

}
