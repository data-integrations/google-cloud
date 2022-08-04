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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryMultiSink;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySink;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLEngine;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.TestEnvironment;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * BigQuery Connector integration test.
 *
 * The service account used to run this test needs BigQuery admin permissions in the project.
 */
public class BigQueryConnectorTest {
  private static final Set<String> SUPPORTED_TYPES = new HashSet<>(Arrays.asList("table", "view"));
  private static TestEnvironment testEnvironment;
  private static String project;
  private static String datasetProject;
  private static String dataset;
  private static String table;
  private static BigQuery bigQuery;
  private static final Schema SCHEMA =
    Schema.recordOf("output",
                    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  private static final Set<StructuredRecord> TABLE_DATA = new HashSet<>(Arrays.asList(
    StructuredRecord.builder(SCHEMA).set("id", 0L).set("name", "alice").build(),
    StructuredRecord.builder(SCHEMA).set("id", 1L).set("name", "bob").build()));

  @BeforeClass
  public static void setupTestClass() throws Exception {
    testEnvironment = TestEnvironment.load();

    project = testEnvironment.getProject();
    datasetProject = testEnvironment.getProject();
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    dataset = String.format("bq_connector_test_%d_%s", now, UUID.randomUUID().toString().replaceAll("-", ""));
    table = "users";

    // create dataset, table, and populate table
    bigQuery = GCPUtils.getBigQuery(project, testEnvironment.getCredentials());
    bigQuery.create(DatasetInfo.of(DatasetId.of(project, dataset)));

    // TODO: (CDAP-19477) test one of the fields being required instead of nullable
    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
      Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableId tableId = TableId.of(project, dataset, table);
    bigQuery.create(TableInfo.of(tableId, tableDefinition));
    List<InsertAllRequest.RowToInsert> rows = TABLE_DATA.stream()
      .map(BigQueryConnectorTest::convert)
      .collect(Collectors.toList());

    bigQuery.insertAll(InsertAllRequest.of(tableId, rows));
  }

  private static InsertAllRequest.RowToInsert convert(StructuredRecord record) {
    Map<String, Object> row = new HashMap<>();
    for (Schema.Field field : record.getSchema().getFields()) {
      row.put(field.getName(), record.get(field.getName()));
    }
    return InsertAllRequest.RowToInsert.of(row);
  }

  @AfterClass
  public static void afterClass() {
    if (bigQuery == null) {
      return;
    }
    bigQuery.delete(TableId.of(project, dataset, table));
    bigQuery.delete(DatasetId.of(project, dataset));
  }

  @Test
  public void testServiceAccountPath() throws IOException {
    BigQueryConnectorConfig config =
      new BigQueryConnectorConfig(project, datasetProject, null, testEnvironment.getServiceAccountFilePath(), null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    testEnvironment.skipIfNoServiceAccountGiven();

    BigQueryConnectorConfig config =
      new BigQueryConnectorConfig(project, datasetProject, BigQueryConnectorConfig.SERVICE_ACCOUNT_JSON, null,
                                  testEnvironment.getServiceAccountContent());
    test(config);
  }

  private void test(BigQueryConnectorConfig config) throws IOException {
    BigQueryConnector connector = new BigQueryConnector(new BigQueryConnectorSpecificConfig(
      config.getProject(), config.getDatasetProject(), config.getServiceAccountType(),
      config.getServiceAccountFilePath(), config.getServiceAccountJson(), null));
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
    Assert.assertEquals(SCHEMA, schema);
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Set<PluginSpec> expectedRelatedPlugins = new HashSet<>();

    Map<String, String> pluginProperties = new HashMap<>();
    pluginProperties.put("useConnection", "true");
    pluginProperties.put("connection", "${conn(connection-id)}");
    pluginProperties.put("dataset", dataset);
    pluginProperties.put("table", table);
    pluginProperties.put("referenceName", dataset + "." + table);
    expectedRelatedPlugins.add(new PluginSpec(BigQueryMultiSink.NAME, BatchSink.PLUGIN_TYPE, pluginProperties));
    expectedRelatedPlugins.add(new PluginSpec(BigQuerySink.NAME, BatchSink.PLUGIN_TYPE, pluginProperties));
    expectedRelatedPlugins.add(new PluginSpec(BigQuerySource.NAME, BigQuerySource.PLUGIN_TYPE, pluginProperties));
    expectedRelatedPlugins.add(new PluginSpec(BigQuerySQLEngine.NAME, BatchSQLEngine.PLUGIN_TYPE, pluginProperties));
    Assert.assertEquals(expectedRelatedPlugins, relatedPlugins);
  }

  private void testSample(BigQueryConnector connector) throws IOException {
    SampleRequest sampleRequest = SampleRequest.builder(10).setPath(dataset + "/" + table).build();
    Set<StructuredRecord> sample = new HashSet<>(
      connector.sample(new MockConnectorContext(new MockConnectorConfigurer()), sampleRequest));

    Assert.assertEquals(TABLE_DATA, sample);


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
