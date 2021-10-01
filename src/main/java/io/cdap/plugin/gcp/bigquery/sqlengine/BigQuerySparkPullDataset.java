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
import io.cdap.cdap.etl.api.engine.sql.dataset.SparkPullDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.plugin.gcp.bigquery.sqlengine.transform.RowToStructuredRecordMapper;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;

/**
 * SQL Pull Dataset implementation for BigQuery backed datasets.
 */
public class BigQuerySparkPullDataset
  implements SparkPullDataset<StructuredRecord>, Serializable {

  private final BigQuery bigQuery;
  private final String datasetName;
  private final Schema schema;
  private final String project;
  private final String bqDataset;
  private final String bqTable;
  private Long numRows;

  private BigQuerySparkPullDataset(String datasetName,
                                   Schema schema,
                                   BigQuery bigQuery,
                                   String project,
                                   String bqDataset,
                                   String bqTable) {
    this.datasetName = datasetName;
    this.schema = schema;
    this.bigQuery = bigQuery;
    this.project = project;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
  }

  public static BigQuerySparkPullDataset getInstance(SQLPullRequest pullRequest,
                                                     BigQuery bigQuery,
                                                     String project,
                                                     String bqDataset,
                                                     String bqTable) throws IOException {

    return new BigQuerySparkPullDataset(pullRequest.getDatasetName(),
                                        pullRequest.getDatasetSchema(),
                                        bigQuery,
                                        project,
                                        bqDataset,
                                        bqTable);
  }

  @Override
  public JavaRDD<StructuredRecord> create(JavaSparkContext javaSparkContext) {
    String path = String.format("%s.%s.%s", project, bqDataset, bqTable);

    SparkSession spark = SparkSession.builder()
      .sparkContext(javaSparkContext.sc())
      .getOrCreate();

    //TODO: This should use the credentials from the BQ SQL engine config, right now it uses dataproc config.
    Dataset<Row> ds = spark.read()
      .format("bigquery")
      .load(path);

    // Adjust Integer and Float types to the right method signature.
    ds = adjustColumnTypes(ds, schema);

    Function<Row, StructuredRecord> mappingFunction = new RowToStructuredRecordMapper(schema);

    return ds.toJavaRDD().map(mappingFunction);
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

  /**
   * Used to correct differences between the Spark-BigQuery Connector data type mappings and CDAP schema types.
   * @param ds input dataset
   * @param schema input schema
   * @return dataset with updated column type mappings.
   */
  public Dataset<Row> adjustColumnTypes(Dataset<Row> ds, Schema schema) {
    if (schema.getFields() == null) {
      return ds;
    }

    for (Schema.Field field : schema.getFields()) {

      // Get underlying type from a field schema.
      Schema fieldSchema = field.getSchema();
      if (fieldSchema.isNullable()) {
        fieldSchema = fieldSchema.getNonNullable();
      }
      Schema.Type type = fieldSchema.getType();

      // Convert int types from long
      if (type == Schema.Type.INT) {
        ds = ds.withColumn(field.getName(), ds.col(field.getName()).cast("int"));
      }

      // Convert float types from double
      if (type == Schema.Type.FLOAT) {
        ds = ds.withColumn(field.getName(), ds.col(field.getName()).cast("float"));
      }
    }

    return ds;
  }
}
