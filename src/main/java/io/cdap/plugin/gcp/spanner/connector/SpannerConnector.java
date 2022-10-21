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

import com.google.api.gax.paging.Page;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import io.cdap.plugin.gcp.spanner.sink.SpannerSink;
import io.cdap.plugin.gcp.spanner.source.ResultSetToRecordTransformer;
import io.cdap.plugin.gcp.spanner.source.SpannerSource;
import io.cdap.plugin.gcp.spanner.source.SpannerSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spanner Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SpannerConnector.NAME)
@Category("Google Cloud Platform")
@Description("Connection to access data in Spanner databases and tables.")
public class SpannerConnector implements DirectConnector {
  public static final String NAME = "Spanner";
  public static final String ENTITY_TYPE_INSTANCE = "instance";
  public static final String ENTITY_TYPE_DATABASE = "database";
  public static final String ENTITY_TYPE_TABLE = "table";
  // Spanner queries for listing tables and listing schema of table are documented at
  // https://cloud.google.com/spanner/docs/information-schema
  private static final Statement LIST_TABLES_STATEMENT =
    Statement.of("SELECT t.table_name FROM " +
                   "information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''");
  private static final String TABLE_NAME = "TableName";
  private static final Statement.Builder SCHEMA_STATEMENT_BUILDER = Statement.newBuilder(
    String.format("SELECT t.column_name, t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE " +
                    "t.table_catalog = '' AND t.table_schema = '' AND t.table_name = @%s", TABLE_NAME));

  private GCPConnectorConfig config;

  SpannerConnector(GCPConnectorConfig config) {
    this.config = config;
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext context, SampleRequest sampleRequest) throws IOException {
    SpannerPath path = new SpannerPath(sampleRequest.getPath());
    String instance = path.getInstance();
    if (instance == null) {
      throw new IllegalArgumentException("Path should contain instance name.");
    }
    String database = path.getDatabase();
    if (database == null) {
      throw new IllegalArgumentException("Path should contain database name.");
    }
    String table = path.getTable();
    if (table == null) {
      throw new IllegalArgumentException("Path should contain table name.");
    }
    return getTableData(instance, database, table, sampleRequest.getLimit(), context.getFailureCollector());
  }

  @Override
  public void test(ConnectorContext context) throws ValidationException {
    String project = config.tryGetProject();
    FailureCollector failureCollector = context.getFailureCollector();
    if (project == null) {
      failureCollector
        .addFailure("Could not detect Google Cloud project id from the environment.", "Please specify a project id.");
    }
    // if either project  cannot be loaded , no need to continue
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    try (Spanner spanner = getSpanner()) {
      spanner.getInstanceAdminClient().listInstances();
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Could not connect to Spanner: %s", e.getMessage()),
                                  "Please specify correct connection properties.");
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext context, BrowseRequest browseRequest) throws IOException {
    SpannerPath path = new SpannerPath(browseRequest.getPath());
    try (Spanner spanner = getSpanner()) {
      if (path.isRoot()) {
        // browse project to list all instances
        return listInstances(spanner, browseRequest.getLimit());
      }
      String instance = path.getInstance();
      String database = path.getDatabase();
      String table = path.getTable();
      if (database == null) {
        return listDatabases(spanner, instance, browseRequest.getLimit());
      }
      if (table == null) {
        return listTables(spanner, instance, database, browseRequest.getLimit());
      }
      return getTableDetail(spanner, instance, database, table);
    }
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext context, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {
    SpannerPath path = new SpannerPath(connectorSpecRequest.getPath());
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> sourceProperties = new HashMap<>();
    Map<String, String> sinkProperties = new HashMap<>();
    sourceProperties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    sinkProperties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    sourceProperties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    sinkProperties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());

    String instanceName = path.getInstance();
    if (instanceName != null) {
      sourceProperties.put(SpannerSourceConfig.NAME_INSTANCE, instanceName);
      sinkProperties.put(SpannerSourceConfig.NAME_INSTANCE, instanceName);
    }
    String databaseName = path.getDatabase();
    if (databaseName != null) {
      sourceProperties.put(SpannerSourceConfig.NAME_DATABASE, databaseName);
      sinkProperties.put(SpannerSourceConfig.NAME_DATABASE, databaseName);
    }
    String tableName = path.getTable();
    if (tableName != null) {
      sourceProperties.put(SpannerSourceConfig.NAME_TABLE, tableName);
      sinkProperties.put(SpannerSourceConfig.NAME_TABLE, tableName);
      sourceProperties.put(Constants.Reference.REFERENCE_NAME,
                     ReferenceNames.cleanseReferenceName(instanceName + "." + databaseName + "." + tableName));
      Schema schema = getTableSchema(instanceName, databaseName, tableName, context.getFailureCollector());
      specBuilder.setSchema(schema);
    }
    return specBuilder
      .addRelatedPlugin(new PluginSpec(SpannerSource.NAME, BatchSource.PLUGIN_TYPE, sourceProperties))
      .addRelatedPlugin(new PluginSpec(SpannerSink.NAME, BatchSink.PLUGIN_TYPE, sinkProperties))
      .build();
  }

  private Schema getTableSchema(String instance, String database, String table, FailureCollector collector)
    throws IOException {
    try (Spanner spanner = getSpanner()) {
      return  SpannerUtil.getTableSchema(spanner, config.getProject(), instance, database, table, collector);
    }
  }

  private BrowseDetail listInstances(Spanner spanner, Integer limit) {
    Page<Instance> page = spanner.getInstanceAdminClient().listInstances();
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    for (Instance instance : page.iterateAll()) {
      if (count >= countLimit) {
        break;
      }
      String name = instance.getId().getInstance();
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_INSTANCE).canBrowse(true).build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail listDatabases(Spanner spanner, String instance, Integer limit) {
    Page<Database> page = spanner.getDatabaseAdminClient().listDatabases(instance);
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    String pathPrefix = "/" + instance + "/";
    for (Database database : page.iterateAll()) {
      if (count >= countLimit) {
        break;
      }
      String name = database.getId().getDatabase();
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, pathPrefix + name, ENTITY_TYPE_DATABASE).canBrowse(true).build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail listTables(Spanner spanner, String instance, String database, Integer limit) {
    ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(config.getProject(), instance, database)).
      singleUse().executeQuery(LIST_TABLES_STATEMENT);
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    String pathPrefix = "/" + instance + "/" + database + "/";
    while (resultSet.next()) {
      if (count >= countLimit) {
        break;
      }
      String name = resultSet.getString("table_name");
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, pathPrefix + name, ENTITY_TYPE_TABLE).canSample(true).build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail getTableDetail(Spanner spanner, String instance, String database, String table) {

    Statement getTableSchemaStatement = SCHEMA_STATEMENT_BUILDER.bind(TABLE_NAME).to(table).build();
    try (ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(config.getProject(), instance, database)).
      singleUse().executeQuery(getTableSchemaStatement)) {
      if (resultSet.next()) {
        String path = "/" + instance + "/" + database + "/" + table;
        return BrowseDetail.builder()
          .addEntity(BrowseEntity.builder(table, path, ENTITY_TYPE_TABLE).canSample(true).build())
          .setTotalCount(1)
          .build();
      } else {
        throw new IllegalArgumentException(String.format("Cannot find table: %s", table));
      }
    }
  }

  private List<StructuredRecord> getTableData(String instance, String database, String table, int limit,
                                              FailureCollector collector)
    throws IOException {
    List<StructuredRecord> records = new ArrayList<>();
    try (Spanner spanner = getSpanner()) {
      Schema schema = getTableSchema(instance, database, table, collector);
      List<String> columnNames = schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
      ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(config.getProject(), instance, database))
        .singleUse()
        .read(table, KeySet.all(), columnNames, Options.limit(limit));
      ResultSetToRecordTransformer transformer = new ResultSetToRecordTransformer(schema);
      while (resultSet.next()) {
        records.add(transformer.transform(resultSet));
      }
      return records;
    }
  }

  private Spanner getSpanner() throws IOException {
    return SpannerUtil.getSpannerService(config.getServiceAccount(), config.isServiceAccountFilePath(),
                                         config.getProject());
  }
}
