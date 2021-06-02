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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.plugin.gcp.bigquery.source.BigQueryInputFormatProvider;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceUtils;
import io.cdap.plugin.gcp.bigquery.sqlengine.transform.PullTransform;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * SQL Pull Dataset implementation for BigQuery backed datasets.
 */
public class BigQueryPullDataset extends BigQueryInputFormatProvider
  implements SQLPullDataset<StructuredRecord, LongWritable, GenericData.Record>, BigQuerySQLDataset {

  private final BigQuery bigQuery;
  private final String datasetName;
  private final Schema schema;
  private final String project;
  private final String bqDataset;
  private final String bqTable;
  private final String gcsPath;
  private Long numRows;

  private BigQueryPullDataset(Configuration configuration,
                              String datasetName,
                              Schema schema,
                              BigQuery bigQuery,
                              String project,
                              String bqDataset,
                              String bqTable,
                              String gcsPath) {
    super(configuration);
    this.datasetName = datasetName;
    this.schema = schema;
    this.bigQuery = bigQuery;
    this.project = project;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
    this.gcsPath = gcsPath;
  }

  public static BigQueryPullDataset getInstance(SQLPullRequest pullRequest,
                                                Configuration baseConfiguration,
                                                BigQuery bigQuery,
                                                String project,
                                                String bqDataset,
                                                String bqTable,
                                                String bucket,
                                                String runId) throws IOException {

    // Clone configuration object
    Configuration configuration = new Configuration(baseConfiguration);

    // Configure BigQuery input format.
    String gcsPath = BigQuerySQLEngineUtils.getGCSPath(bucket, runId, bqTable);
    BigQuerySourceUtils.configureBigQueryInput(configuration, project, bqDataset, bqTable, gcsPath);

    return new BigQueryPullDataset(configuration,
                                   pullRequest.getDatasetName(),
                                   pullRequest.getDatasetSchema(),
                                   bigQuery,
                                   project,
                                   bqDataset,
                                   bqTable,
                                   gcsPath);
  }

  @Override
  public Transform<KeyValue<LongWritable, GenericData.Record>, StructuredRecord> fromKeyValue() {
    return new PullTransform(schema);
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public long getNumRows() {
    // Get the number of rows from BQ if not known at this time.
    if (numRows == null) {
      numRows = BigQuerySQLEngineUtils.getNumRows(bigQuery, project, bqDataset, bqTable);
    }

    return numRows;
  }

  @Override
  public String getBigQueryTableName() {
    return bqTable;
  }

  @Override
  @Nullable
  public String getJobId() {
    return null;
  }

  @Override
  public String getGCSPath() {
    return gcsPath;
  }
}
