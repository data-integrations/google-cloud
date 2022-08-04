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

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
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
import io.cdap.plugin.gcp.common.TestEnvironment;
import io.cdap.plugin.gcp.spanner.sink.SpannerSink;
import io.cdap.plugin.gcp.spanner.source.SpannerSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Spanner Connector integration test.
 *
 * The service account used to run this test needs Spanner admin permissions in the project.
 */
public class SpannerConnectorTest {
  private static String project;
  private static String instance;
  private static String database;
  private static String table;
  private static String serviceAccountFilePath;
  private static String serviceAccountKey;
  private static Spanner spanner;
  private static TestEnvironment testEnvironment;
  private static Schema expectedSchema;
  private static Set<StructuredRecord> tableData;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    testEnvironment = TestEnvironment.load();

    project = testEnvironment.getProject();
    serviceAccountFilePath = testEnvironment.getServiceAccountFilePath();
    serviceAccountKey = testEnvironment.getServiceAccountContent();

    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    instance = String.format("gcp-plugins-test-%d-%s", now, UUID.randomUUID());
    database = "connectors";
    table = "users";

    spanner = SpannerOptions.newBuilder()
      .setCredentials(testEnvironment.getCredentials())
      .setProjectId(project)
      .build()
      .getService();

    InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(project, instance))
      .setProcessingUnits(100)
      .setInstanceConfigId(InstanceConfigId.of(project, "regional-us-west1"))
      .setDisplayName("gcp-plugins-test-" + now)
      .build();
    spanner.getInstanceAdminClient().createInstance(instanceInfo).get();
    expectedSchema = Schema.recordOf("outputSchema",
                                     Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                     Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    String createStatement = String.format("CREATE TABLE %s (id INT64 NOT NULL, name STRING(256)) PRIMARY KEY (id)",
                                           table);
    spanner.getDatabaseAdminClient().createDatabase(instance, database, Collections.singleton(createStatement)).get();
    Mutation alice = Mutation.newInsertOrUpdateBuilder(table)
      .set("id").to(0L)
      .set("name").to("alice")
      .build();
    Mutation bob = Mutation.newInsertOrUpdateBuilder(table)
      .set("id").to(1L)
      .set("name").to("bob")
      .build();
    spanner.getDatabaseClient(DatabaseId.of(project, instance, database)).write(Arrays.asList(alice, bob));
    tableData = new HashSet<>();
    tableData.add(StructuredRecord.builder(expectedSchema).set("id", 0L).set("name", "alice").build());
    tableData.add(StructuredRecord.builder(expectedSchema).set("id", 1L).set("name", "bob").build());
  }

  @AfterClass
  public static void cleanupTestClass() {
    if (spanner == null) {
      return;
    }
    spanner.getInstanceAdminClient().deleteInstance(instance);
  }

  @Test
  public void testServiceAccountPath() throws IOException {
    GCPConnectorConfig config =
      new GCPConnectorConfig(project, GCPConnectorConfig.SERVICE_ACCOUNT_FILE_PATH, serviceAccountFilePath, null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    testEnvironment.skipIfNoServiceAccountGiven();
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
    Map<String, String> properties = new HashMap<>();
    properties.put("instance", instance);
    properties.put("database", database);
    properties.put("table", table);
    properties.put("connection", null);
    properties.put("useConnection", "true");
    properties.put("referenceName", String.format("%s.%s.%s", instance, database, table));
    ConnectorSpec expected = ConnectorSpec.builder()
      .setSchema(expectedSchema)
      .addRelatedPlugin(new PluginSpec(SpannerSource.NAME, BatchSource.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(SpannerSink.NAME, BatchSink.PLUGIN_TYPE, properties))
      .build();

    ConnectorSpec connectorSpec =
      connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                             ConnectorSpecRequest.builder().setPath(instance + "/" + database + "/" + table).build());
    Assert.assertEquals(expected, connectorSpec);
  }

  private void testSample(SpannerConnector connector) throws IOException {
    List<StructuredRecord> sample = connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                                     SampleRequest.builder(10)
                                                       .setPath(instance + "/" + database + "/" + table).build());

    Assert.assertEquals(tableData, new HashSet<>(sample));

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
    BrowseDetail expected = BrowseDetail.builder()
      .setTotalCount(1)
      .addEntity(BrowseEntity.builder(instance, "/" + instance, "instance")
                   .canBrowse(true)
                   .canSample(false)
                   .build())
      .build();
    Assert.assertEquals(expected, detail);

    // browse instance
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance).build());
    expected = BrowseDetail.builder()
      .setTotalCount(1)
      .addEntity(BrowseEntity.builder(database, String.format("/%s/%s", instance, database), "database")
                   .canBrowse(true)
                   .canSample(false)
                   .build())
      .build();
    Assert.assertEquals(expected, detail);

    // browse database
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance + "/" + database).build());
    expected = BrowseDetail.builder()
      .setTotalCount(1)
      .addEntity(BrowseEntity.builder(table, String.format("/%s/%s/%s", instance, database, table), "table")
                   .canBrowse(false)
                   .canSample(true)
                   .build())
      .build();
    Assert.assertEquals(expected, detail);

    // browse table
    detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                              BrowseRequest.builder(instance + "/" + database + "/" + table).build());
    Assert.assertEquals(expected, detail);

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
