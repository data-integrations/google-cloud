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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryOutputFormatProvider;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sink.Operation;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import io.cdap.plugin.gcp.bigquery.sqlengine.transform.PushTransform;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.List;

/**
 * SQL Push Dataset implementation for BigQuery backed datasets.
 */
public class BigQueryPushDataset extends BigQueryOutputFormatProvider
  implements SQLPushDataset<StructuredRecord, StructuredRecord, NullWritable>, BigQuerySQLDataset {

  private final String datasetName;
  private final BigQuery bigQuery;
  private final DatasetId bqDataset;
  private final String bqTable;
  private final String gcsPath;
  private final String jobId;
  private Long numRows;

  private BigQueryPushDataset(String datasetName,
                              Schema tableSchema,
                              Configuration configuration,
                              BigQuery bigQuery,
                              DatasetId bqDataset,
                              String bqTable,
                              String jobId,
                              String gcsPath) {
    super(configuration, tableSchema);
    this.datasetName = datasetName;
    this.bigQuery = bigQuery;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
    this.jobId = jobId;
    this.gcsPath = gcsPath;
  }

  protected static BigQueryPushDataset getInstance(SQLPushRequest pushRequest,
                                                   BigQuerySQLEngineConfig sqlEngineConfig,
                                                   Configuration baseConfiguration,
                                                   BigQuery bigQuery,
                                                   DatasetId dataset,
                                                   String bucket,
                                                   String runId) throws IOException {
    // Get new Job ID for this push operation
    String jobId = BigQuerySQLEngineUtils.newIdentifier();

    // Build new table name for this dataset
    String table = BigQuerySQLEngineUtils.getNewTableName(runId);

    // Clone configuration object
    Configuration configuration = new Configuration(baseConfiguration);

    // Set required configuration properties. Note that the table will be created before the upload takes place.
    configuration.set(BigQueryConstants.CONFIG_JOB_ID, jobId);
    configuration.set(BigQueryConstants.CONFIG_OPERATION, Operation.INSERT.name());
    configuration.setBoolean(BigQueryConstants.CONFIG_DESTINATION_TABLE_EXISTS, true);
    configuration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION, true);
    configuration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION_ON_EMPTY_OUTPUT, true);

    // Configure output.
    String gcsPath = BigQuerySQLEngineUtils.getGCSPath(bucket, runId, table);
    List<BigQueryTableFieldSchema> fields =
      BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(pushRequest.getDatasetSchema());
    BigQuerySinkUtils.configureOutput(configuration, dataset, table, gcsPath, fields);

    // Create empty table to store uploaded records.
    BigQuerySQLEngineUtils.createEmptyTable(sqlEngineConfig, bigQuery, dataset.getProject(), dataset.getDataset(),
                                            table);

    //Build new Instance
    return new BigQueryPushDataset(pushRequest.getDatasetName(),
                                   pushRequest.getDatasetSchema(),
                                   configuration,
                                   bigQuery,
                                   dataset,
                                   table,
                                   jobId,
                                   gcsPath);
  }

  @Override
  public Transform<StructuredRecord, KeyValue<StructuredRecord, NullWritable>> toKeyValue() {
    return new PushTransform();
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public Schema getSchema() {
    return tableSchema;
  }

  @Override
  public long getNumRows() {
    // Get the number of rows from BQ if not known at this time.
    if (numRows == null) {
      numRows = BigQuerySQLEngineUtils.getNumRows(bigQuery, bqDataset, bqTable);
    }

    return numRows;
  }

  @Override
  public String getBigQueryProject() {
    return bqDataset.getProject();
  }

  @Override
  public String getBigQueryDataset() {
    return bqDataset.getDataset();
  }

  @Override
  public String getBigQueryTable() {
    return bqTable;
  }

  @Override
  public String getGCSPath() {
    return gcsPath;
  }

  @Override
  public String getJobId() {
    return jobId;
  }
}
