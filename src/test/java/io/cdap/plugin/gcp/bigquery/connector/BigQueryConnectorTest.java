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

package io.cdap.plugin.gcp.bigquery.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * BigQuery Connector integration test. This test will only be run when below property is provided:
 * project.id -- the name of the project where temporary table or staging bucket may be created or BigQuery job may
 * be run. It will default to active google project if you have google cloud client installed.
 * dataset.project -- optional, the name of the project where the dataset is
 * dataset.name -- the name of the dataset
 * table.name -- the name of the table
 * service.account.file -- the path to the service account key file
 */
public class BigQueryConnectorTest {
  private static final Set<String> SUPPORTED_TYPES = new HashSet<>(Arrays.asList("TABLE", "VIEW"));
  private static String serviceAccountKey;
  private static String project;
  private static String datasetProject;
  private static String dataset;
  private static String table;
  private static String serviceAccountFilePath;

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

    datasetProject = System.getProperty("dataset.project");

    dataset = System.getProperty("dataset.name");
    Assume.assumeFalse(String.format(messageTemplate, "dataset name"), dataset == null);

    table = System.getProperty("table.name");
    Assume.assumeFalse(String.format(messageTemplate, "table name"), table == null);

    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);

  }

  @Test
  public void testServiceAccountPath() throws IOException {
    BigQueryConnectorConfig config =
      new BigQueryConnectorConfig(project, datasetProject, null, serviceAccountFilePath, null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    BigQueryConnectorConfig config =
      new BigQueryConnectorConfig(project, datasetProject, BigQueryConnectorConfig.SERVICE_ACCOUNT_JSON, null,
                                  serviceAccountKey);
    test(config);
  }

  private void test(BigQueryConnectorConfig config) throws IOException {
    BigQueryConnector connector = new BigQueryConnector(config);
    testTest(connector);
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector);
  }

  private void testGenerateSpec(BigQueryConnector connector) throws IOException {
    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         ConnectorSpecRequest.builder().setPath(dataset + "/" + table)
                                                           .setConnection("${conn(connection-id)}").build());

    Schema schema = connectorSpec.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(BigQuerySource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());

    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals(dataset, properties.get("dataset"));
    Assert.assertEquals(table, properties.get("table"));
    Assert.assertEquals("true", properties.get(BigQuerySourceConfig.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(BigQuerySourceConfig.NAME_CONNECTION));
  }

  private void testSample(BigQueryConnector connector) throws IOException {
    List<StructuredRecord> sample =
      connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                       SampleRequest.builder(1).setPath(dataset + "/" + table).build());
    Assert.assertEquals(1, sample.size());
    StructuredRecord record = sample.get(0);
    Schema schema = record.getSchema();
    Assert.assertNotNull(schema);
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
      Assert.assertTrue(record.get(field.getName()) != null || field.getSchema().isNullable());
    }

    //invalid path
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                               SampleRequest.builder(1).setPath("a/b/c").build()));

    //sample dataset
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                               SampleRequest.builder(1).setPath(dataset).build()));
  }

  private void testBrowse(BigQueryConnector connector) throws IOException {
    // browse project
    BrowseDetail detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                           BrowseRequest.builder("/").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertEquals(BigQueryConnector.ENTITY_TYPE_DATASET, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertFalse(entity.canSample());
    }

    // browse dataset
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(dataset).build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertTrue(SUPPORTED_TYPES.contains(entity.getType()));
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // browse table
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(dataset + "/" + table).build());
    Assert.assertEquals(1, detail.getTotalCount());
    Assert.assertEquals(1, detail.getEntities().size());
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertTrue(SUPPORTED_TYPES.contains(entity.getType()));
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // invalid path
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder("a/b/c").build()));

    // not existing dataset
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder("/notexisting").build()));

    // not existing table
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder(dataset + "/notexisting").build()));
  }

  private void testTest(BigQueryConnector connector) {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    ValidationException validationException = context.getFailureCollector().getOrThrowException();
    Assert.assertTrue(validationException.getFailures().isEmpty());
  }
}
