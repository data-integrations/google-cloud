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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollectionImpl;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Nullable;

/**
 * Dataset Producer implementation which uses the Spark-BigQuery connector to extract records.
 */
public class BigQuerySparkDatasetProducer
  implements SQLDatasetProducer, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySparkDatasetProducer.class);

  private static final String FORMAT = "bigquery";
  private static final String CONFIG_CREDENTIALS_FILE = "credentialsFile";
  private static final String CONFIG_CREDENTIALS = "credentials";

  private BigQuerySQLEngineConfig config;
  private String project;
  private String bqDataset;
  private String bqTable;
  private Schema schema;


  public BigQuerySparkDatasetProducer(BigQuerySQLEngineConfig config,
                                      String project,
                                      String bqDataset,
                                      String bqTable,
                                      Schema schema) {
    this.config = config;
    this.project = project;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
    this.schema = schema;
  }

  @Override
  public SQLDatasetDescription getDescription() {
    return null;
  }

  @Override
  @Nullable
  public RecordCollection produce(SQLDataset sqlDataset) {
    // Define which table to load.
    String path = String.format("%s.%s.%s", project, bqDataset, bqTable);

    // Create Spark context to use for this operation.
    SparkContext sc = SparkContext.getOrCreate();
    SparkSession spark = SparkSession.builder()
      .appName("spark-bq-connector-reader")
      .sparkContext(sc)
      .getOrCreate();

    DataFrameReader bqReader = spark.read().format(FORMAT);

    // Set credential file path or base64-encoded credential from json.
    if (Boolean.TRUE.equals(config.isServiceAccountFilePath()) && config.getServiceAccountFilePath() != null) {
      bqReader.option(CONFIG_CREDENTIALS_FILE, config.getServiceAccountFilePath());
    } else if (Boolean.TRUE.equals(config.isServiceAccountJson()) && config.getServiceAccountJson() != null) {
      bqReader.option(CONFIG_CREDENTIALS, encodeBase64(config.getServiceAccountJson()));
    }

    // Load path into dataset.
    Dataset<Row> ds = bqReader.load(path);
    ds = convertFieldTypes(ds);

    return new SparkRecordCollectionImpl(ds);
  }


  private String encodeBase64(String serviceAccountJson) {
    return Base64.getEncoder().encodeToString(serviceAccountJson.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Adjust CDAP types for int and float fields.
   *
   * @param ds input dataframe
   * @return dataframe with updated schema.
   */
  private Dataset<Row> convertFieldTypes(Dataset<Row> ds) {
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();

      // For nullable types, check the underlying type.
      if (fieldSchema.isNullable()) {
        fieldSchema = fieldSchema.getNonNullable();
      }

      // Handle Int types
      if (fieldSchema.getType() == Schema.Type.INT) {
        LOG.trace("Converting field {} to Integer", fieldName);
        ds = ds.withColumn(fieldName, ds.col(fieldName).cast(DataTypes.IntegerType));
      }

      // Handle float types
      if (fieldSchema.getType() == Schema.Type.FLOAT) {
        LOG.trace("Converting field {} to Float", fieldName);
        ds = ds.withColumn(fieldName, ds.col(fieldName).cast(DataTypes.FloatType));
      }
    }

    return ds;
  }

}
