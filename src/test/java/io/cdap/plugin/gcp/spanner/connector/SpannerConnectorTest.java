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

package io.cdap.plugin.gcp.spanner.connector;

import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.InstanceNotFoundException;
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
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.spanner.source.SpannerSource;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spanner Connector integration test.
 * This test will only be run when following properties are provided:
 * project.id -- the name of the project
 * instance.id -- the id of the spanner instance
 * database.name -- the name of the spanner database
 * table.name -- the name of the spanner table
 * service.account.file-- the path to the service account key file
 */
public class SpannerConnectorTest {
  private static String project;
  private static String instance;
  private static String database;
  private static String table;
  private static String serviceAccountFilePath;
  private static String serviceAccountKey;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured.";

    project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    Assume.assumeFalse(String.format(messageTemplate, "project id"), project == null);

    instance = System.getProperty("instance.id");
    Assume.assumeFalse(String.format(messageTemplate, "instance id"), instance == null);


    database = System.getProperty("database.name");
    Assume.assumeFalse(String.format(messageTemplate, "database name"), database == null);

    table = System.getProperty("table.name");
    Assume.assumeFalse(String.format(messageTemplate, "table name"), table == null);

    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);
  }
  @Test
  public void testServiceAccountPath() throws IOException {
    GCPConnectorConfig config =
      new GCPConnectorConfig(project, GCPConnectorConfig.SERVICE_ACCOUNT_FILE_PATH, serviceAccountFilePath, null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    GCPConnectorConfig config =
      new GCPConnectorConfig(project, GCPConnectorConfig.SERVICE_ACCOUNT_JSON, null, serviceAccountKey);
    test(config);
  }

  private void test(GCPConnectorConfig config) throws IOException {
    SpannerConnector connector = new SpannerConnector(config);
    testTest(connector);
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector);
  }

  private void testGenerateSpec(SpannerConnector connector) throws IOException {
    ConnectorSpec connectorSpec =
      connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                             ConnectorSpecRequest.builder().setPath(instance + "/" + database + "/" + table).build());
    Schema schema = connectorSpec.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(SpannerSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());

    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals(instance, properties.get("instance"));
    Assert.assertEquals(database, properties.get("database"));
    Assert.assertEquals(table, properties.get("table"));
  }

  private void testSample(SpannerConnector connector) throws IOException {
    List<StructuredRecord> sample = connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                                     SampleRequest.builder(1)
                                                       .setPath(instance + "/" + database + "/" + table).build());
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
                                               SampleRequest.builder(1).setPath("a/b/c/d").build()));

    //sample database
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                               SampleRequest.builder(1).setPath(database).build()));
  }

  private void testBrowse(SpannerConnector connector) throws IOException {
    // browse project
    BrowseDetail detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                           BrowseRequest.builder("/").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(SpannerConnector.ENTITY_TYPE_INSTANCE, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertFalse(entity.canSample());
    }

    // browse instance
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance).build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(SpannerConnector.ENTITY_TYPE_DATABASE, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertFalse(entity.canSample());
    }

    // browse database
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance + "/" + database).build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(SpannerConnector.ENTITY_TYPE_TABLE, entity.getType());
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }


    // browse table
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance + "/" + database + "/" + table).build());
    Assert.assertEquals(1, detail.getTotalCount());
    Assert.assertEquals(1, detail.getEntities().size());
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(SpannerConnector.ENTITY_TYPE_TABLE, entity.getType());
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // invalid path
    Assert.assertThrows(IllegalArgumentException.class, () ->
      connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                       BrowseRequest.builder("a/b/c/d").build()));

    // not existing instance
    Assert.assertThrows(InstanceNotFoundException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder("/notexisting").build()));

    // not existing database
    Assert.assertThrows(DatabaseNotFoundException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder(instance + "/notexisting").build()));

    // not existing table
    Assert.assertThrows(IllegalArgumentException.class,
                        () -> connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                               BrowseRequest.builder(instance + "/" + database + "/notexisting")
                                                 .build()));
  }

  private void testTest(SpannerConnector connector) {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    ValidationException validationException = context.getFailureCollector().getOrThrowException();
    Assert.assertTrue(validationException.getFailures().isEmpty());
  }
}
