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

package io.cdap.plugin.gcp.dataplex.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBatchSinkConfig;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch Sink that writes data to Dataplex assets (Bigquery or GCS).
 *
 * {@code StructuredRecord} is the first parameter because that is what the
 * sink will take as an input.
 * NullWritable is the second parameter because that is the key used
 * by Hadoop's {@code TextOutputFormat}.
 * {@code StructuredRecord} is the third parameter because that is the value used by
 * Hadoop's {@code TextOutputFormat}. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DataplexBatchSink.NAME)
@Description("Ingests and processes data within Dataplex.")
public final class DataplexBatchSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  public static final String NAME = "Dataplex";
  public static final String BIGQUERY_DATASET_ASSET_TYPE = "BIGQUERY_DATASET";
  public static final String STORAGE_BUCKET_ASSET_TYPE = "STORAGE_BUCKET";
  // Usually, you will need a private variable to store the config that was passed to your class
  private final DataplexBatchSinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSink.class);

  public DataplexBatchSink(DataplexBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    if (!config.getConnection().canConnect() || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
      (config.tryGetProject() == null)) {
      return;
    }

    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    config.validateServiceAccount(collector);
    Schema inputSchema = configurer.getInputSchema();
    Schema configuredSchema = config.getSchema(collector);
    DataplexInterface dataplexInterface = new DataplexInterfaceImpl();
    config.validateAssetConfiguration(collector, dataplexInterface);
    if (config.getAssetType().equals(BIGQUERY_DATASET_ASSET_TYPE)) {
      config.validateBigQueryDataset(inputSchema, configuredSchema, collector, dataplexInterface);
    } else if (config.getAssetType().equals(STORAGE_BUCKET_ASSET_TYPE)) {
      config.validateStorageBucket(pipelineConfigurer, collector);
    }


    // validate schema with underlying table
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
  //no-op
  }
}
