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
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryDataParser;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;

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
@Description("Connection to browse and sample data from BigQuery datasets and tables.")
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
    return getTableData(getBigQuery(config.getProject()), config.getDatasetProject(), dataset, table,
      sampleRequest.getLimit());
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
        failureCollector.addFailure(String.format("Service account key provided is not valid: %s.", e.getMessage()),
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
      BrowseEntity.builder(tableName, "/" + datasetName + "/" + tableName, table.getDefinition().getType().name())
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
    Page<Dataset> datasetPage = bigQuery.listDatasets(BigQuery.DatasetListOption.all());
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

  private List<StructuredRecord> getTableData(BigQuery bigQuery, String datasetProject, String dataset, String table,
    int limit)
    throws IOException {
    String query =
      String.format("SELECT * FROM `%s.%s.%s` LIMIT %d", datasetProject, dataset, table, limit);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    String id = UUID.randomUUID().toString();
    JobId jobId = JobId.of(id);
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    // Wait for the job to finish
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      throw new IOException(String.format("Query job %s interrupted.", id), e);
    }

    // check for errors
    if (queryJob == null) {
      throw new IOException(String.format("Job %s no longer exists.", id));
    } else if (queryJob.getStatus().getError() != null) {
      throw new IOException(String.format("Failed to query table : %s", queryJob.getStatus().getError().toString()));
    }

    // Get the results
    TableResult result = null;
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
      properties.put(Constants.Reference.REFERENCE_NAME,
                     ReferenceNames.cleanseReferenceName(datasetName + "." + tableName));
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(BigQuerySource.NAME, BatchSource.PLUGIN_TYPE, properties))
      .build();
  }
}
