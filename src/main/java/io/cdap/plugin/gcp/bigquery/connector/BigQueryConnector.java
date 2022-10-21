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

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
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
import io.cdap.cdap.etl.api.connector.SampleType;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryMultiSink;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySink;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLEngine;
import io.cdap.plugin.gcp.bigquery.util.BigQueryDataParser;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * BigQuery Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(BigQueryConnector.NAME)
@Category("Google Cloud Platform")
@Description("Connection to access data in BigQuery datasets and tables.")
public final class BigQueryConnector implements DirectConnector {
  public static final String NAME = "BigQuery";
  public static final String ENTITY_TYPE_DATASET = "dataset";
  private static final int ERROR_CODE_NOT_FOUND = 404;
  private BigQueryConnectorSpecificConfig config;

  BigQueryConnector(BigQueryConnectorSpecificConfig config) {
    this.config = config;
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext context, SampleRequest sampleRequest) throws IOException {
    BigQueryPath path = new BigQueryPath(sampleRequest.getPath());
    String table = path.getTable();
    if (table == null) {
      throw new IllegalArgumentException("Path should contain both dataset and table name.");
    }
    String dataset = path.getDataset();
    String query = getTableQuery(String.format("`%s.%s.%s`", config.getDatasetProject(), dataset, table),
                                 sampleRequest.getLimit(),
                                 SampleType.fromString(sampleRequest.getProperties().get("sampleType")),
                                 sampleRequest.getProperties().get("strata"),
                                 UUID.randomUUID().toString().replace("-", "_"));
    String id = UUID.randomUUID().toString();
    return getQueryResult(waitForJob(getBigQuery(config.getProject()), query, sampleRequest.getTimeoutMs(), id), id);
  }

  @Override
  public void test(ConnectorContext context) throws ValidationException {
    FailureCollector failureCollector = context.getFailureCollector();
    // validate project ID
    String project = config.tryGetProject();
    if (project == null) {
      failureCollector
        .addFailure("Could not detect Google Cloud project id from the environment.", "Please specify a project id.");
    }

    GoogleCredentials credentials = null;
    //validate service account
    if (config.isServiceAccountJson() || config.getServiceAccountFilePath() != null) {
      try {
        credentials =
          GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
      } catch (Exception e) {
        failureCollector.addFailure(String.format("Service account key provided is not valid: %s", e.getMessage()),
                                    "Please provide a valid service account key.");
      }
    }
    // if either project or credentials cannot be loaded , no need to continue
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    try {
      BigQuery bigQuery = GCPUtils.getBigQuery(config.getDatasetProject(), credentials);
      bigQuery.listDatasets(BigQuery.DatasetListOption.pageSize(1));
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Could not connect to BigQuery: %s", e.getMessage()),
                                  "Please specify correct connection properties.");
    }
  }


  @Override
  public BrowseDetail browse(ConnectorContext context, BrowseRequest browseRequest) throws IOException {
    BigQueryPath path = new BigQueryPath(browseRequest.getPath());

    String dataset = path.getDataset();
    if (dataset == null) {
      // browse project to list all datasets
      return config.rootDataset == null ?
        listDatasets(getBigQuery(config.getDatasetProject()), browseRequest.getLimit()) :
        BrowseDetail.builder().setTotalCount(1).addEntity(
          BrowseEntity.builder(config.rootDataset, "/" + config.rootDataset, ENTITY_TYPE_DATASET)
            .canBrowse(true).build()).build();
    }
    String table = path.getTable();
    if (table == null) {
      return listTables(getBigQuery(config.getProject()), config.getDatasetProject(), dataset,
                        browseRequest.getLimit());
    }
    return getTableDetail(getBigQuery(config.getProject()), config.getDatasetProject(), dataset, table);
  }

  private BrowseDetail getTableDetail(BigQuery bigQuery, String datasetProject, String datasetName, String tableName) {
    Table table = getTable(bigQuery, datasetProject, datasetName, tableName);
    return BrowseDetail.builder().addEntity(
      BrowseEntity.builder(tableName, "/" + datasetName + "/" + tableName,
                           table.getDefinition().getType().name().toLowerCase())
        .canSample(true).build()).setTotalCount(1).build();
  }

  private Table getTable(BigQuery bigQuery, String datasetProject, String datasetName, String tableName) {
    Table table = bigQuery.getTable(TableId.of(datasetProject, datasetName, tableName));
    if (table == null) {
      throw new IllegalArgumentException(String.format("Cannot find tableName: %s.%s.", datasetName, tableName));
    }
    return table;
  }

  private BrowseDetail listTables(BigQuery bigQuery, String datasetProject, String dataset, @Nullable Integer limit) {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    DatasetId datasetId = DatasetId.of(datasetProject, dataset);
    Page<Table> tablePage = null;
    try {
      tablePage = bigQuery.listTables(datasetId);
    } catch (BigQueryException e) {
      if (e.getCode() == ERROR_CODE_NOT_FOUND) {
        throw new IllegalArgumentException(String.format("Cannot find dataset: %s.", dataset), e);
      }
      throw e;
    }
    String parentPath = "/" + dataset + "/";
    for (Table table : tablePage.iterateAll()) {
      if (count >= countLimit) {
        break;
      }
      String name = table.getTableId().getTable();
      browseDetailBuilder.addEntity(
        BrowseEntity.builder(name, parentPath + name, table.getDefinition().getType().name().toLowerCase())
          .canSample(true).build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail listDatasets(BigQuery bigQuery, Integer limit) {
    Page<Dataset> datasetPage = config.showHiddenDatasets() ?
      bigQuery.listDatasets(BigQuery.DatasetListOption.all()) : bigQuery.listDatasets();
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    for (Dataset dataset : datasetPage.iterateAll()) {
      if (count >= countLimit) {
        break;
      }
      String name = dataset.getDatasetId().getDataset();
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_DATASET).canBrowse(true).build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  /**
   * Get BigQuery client
   * @param project the GCP project where BQ dataset is listed and BQ job is run
   */
  private BigQuery getBigQuery(String project) throws IOException {
    GoogleCredentials credentials = null;
    //validate service account
    if (config.isServiceAccountJson() || config.getServiceAccountFilePath() != null) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
    }
    // Here project decides where the BQ job is run and under which the datasets is listed
    return GCPUtils.getBigQuery(project, credentials);
  }

  /**
   * Get the SQL query used to sample the table
   * @param tableName name of the table
   * @param limit limit on rows returned
   * @param sampleType sampling method
   * @param strata strata column (if given)
   * @param sessionID UUID
   * @return String
   * @throws IllegalArgumentException if no strata column is given for a stratified query
   */
  protected String getTableQuery(String tableName, int limit, SampleType sampleType, @Nullable String strata,
                               String sessionID) {
    switch (sampleType) {
      case RANDOM:
        return String.format("WITH table AS (\n" +
                                "  SELECT *, RAND() AS r_%s\n" +
                                "  FROM %s\n" +
                                "  WHERE RAND() < 2*%d/(SELECT COUNT(*) FROM %s)\n" +
                                ")\n" +
                                "SELECT * EXCEPT (r_%s)\n" +
                                "FROM table\n" +
                                "ORDER BY r_%s\n" +
                                "LIMIT %d",
                              sessionID, tableName, limit, tableName, sessionID, sessionID, limit);
      case STRATIFIED:
        if (strata == null) {
          throw new IllegalArgumentException("No strata column given.");
        }
        return String.format("SELECT * EXCEPT (`sqn_%s`, `c_%s`)\n" +
                                "FROM (\n" +
                                "SELECT *, row_number() OVER (ORDER BY %s, RAND()) AS sqn_%s,\n" +
                                "COUNT(*) OVER () as c_%s,\n" +
                                "FROM %s\n" +
                                ") %s\n" +
                                "WHERE MOD(sqn_%s, CAST(c_%s / %d AS INT64)) = 1\n" +
                                "ORDER BY %s\n" +
                                "LIMIT %d",
                              sessionID, sessionID, strata, sessionID, sessionID, tableName, tableName, sessionID,
                              sessionID, limit, strata, limit);
      default:
        return String.format("SELECT * FROM %s LIMIT %d", tableName, limit);
    }
  }

  /**
   * Wait for job to complete or time out (if timeout is given)
   * @param bigQuery BigQuery client
   * @param query SQL query
   * @param timeoutMs timeout (if given)
   * @param id job ID
   * @return Job
   * @throws IOException if the job is interrupted
   */
  private Job waitForJob(BigQuery bigQuery, String query, @Nullable Long timeoutMs, String id) throws IOException {

    // set up job
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    JobId jobId = JobId.of(id);
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // wait for job
    try {
      if (timeoutMs == null) {
        return queryJob.waitFor();
      } else {
        return queryJob.waitFor(RetryOption.totalTimeout(Duration.ofMillis(timeoutMs)));
      }
    } catch (InterruptedException e) {
      throw new IOException(String.format("Query job %s interrupted.", id), e);
    }
  }

  /**
   * Retrieve the results of a SQL query
   * @param queryJob query job after completion or timeout
   * @param id job ID
   * @return List of structured records
   * @throws IOException if query encounters an error or times out
   */
  protected List<StructuredRecord> getQueryResult(@Nullable Job queryJob, String id) throws IOException {

    // Check for errors
    if (queryJob == null) {
      throw new IOException(String.format("Job %s no longer exists.", id));
    } else if (!queryJob.isDone()) {
      queryJob.cancel();
      throw new IOException(String.format("Job %s timed out.", id));
    } else if (queryJob.getStatus().getError() != null) {
      throw new IOException(String.format("Failed to query table : %s", queryJob.getStatus().getError().toString()));
    }

    // Get the results
    TableResult result;
    try {
      result = queryJob.getQueryResults();
    } catch (InterruptedException e) {
      throw new IOException("Query results interrupted.", e);
    }
    return BigQueryDataParser.parse(result);
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext context,
                                    ConnectorSpecRequest connectorSpecRequest) throws IOException {
    BigQueryPath path = new BigQueryPath(connectorSpecRequest.getPath());
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String datasetName = path.getDataset();
    if (datasetName != null) {
      properties.put(BigQuerySourceConfig.NAME_DATASET, datasetName);
    }
    String tableName = path.getTable();
    if (tableName != null) {
      properties.put(BigQuerySourceConfig.NAME_TABLE, tableName);
      Table table = getTable(getBigQuery(config.getProject()), config.getDatasetProject(), datasetName, tableName);
      TableDefinition definition = table.getDefinition();
      Schema schema = BigQueryUtil.getTableSchema(definition.getSchema(), null);
      specBuilder.setSchema(schema);
      if (definition.getType() != TableDefinition.Type.TABLE) {
        properties.put(BigQuerySourceConfig.NAME_ENABLE_QUERYING_VIEWS, "true");
      }
    }
    return specBuilder
      .addRelatedPlugin(new PluginSpec(BigQuerySource.NAME, BatchSource.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(BigQuerySink.NAME, BatchSink.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(BigQueryMultiSink.NAME, BatchSink.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(BigQuerySQLEngine.NAME, BatchSQLEngine.PLUGIN_TYPE, properties))
      .addSupportedSampleType(SampleType.RANDOM)
      .addSupportedSampleType(SampleType.STRATIFIED)
      .build();
  }
}

