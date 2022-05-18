/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink plugins.
 */
public abstract class AbstractBigQuerySink extends BatchSink<StructuredRecord, StructuredRecord, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQuerySink.class);

  private static final String gcsPathFormat = "gs://%s/%s";
  public static final String RECORDS_UPDATED_METRIC = "records.updated";

  // UUID for the run. Will be used as bucket name if bucket is not provided.
  // UUID is used since GCS bucket names must be globally unique.
  private final UUID runUUID = UUID.randomUUID();
  protected Configuration baseConfiguration;
  protected BigQuery bigQuery;

  /**
   * Executes main prepare run logic. Child classes cannot override this method,
   * instead they should implement two methods {@link #prepareRunValidation(BatchSinkContext)}
   * and {@link #prepareRunInternal(BatchSinkContext, BigQuery, String)} in order to add custom logic.
   *
   * @param context batch sink context
   */
  @Override
  public final void prepareRun(BatchSinkContext context) throws Exception {
    prepareRunValidation(context);

    AbstractBigQuerySinkConfig config = getConfig();
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = serviceAccount == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccount, config.isServiceAccountFilePath());
    String project = config.getProject();
    bigQuery = GCPUtils.getBigQuery(project, credentials);
    CryptoKeyName cmekKeyName = config.getCmekKey(context.getArguments(), context.getFailureCollector());
    context.getFailureCollector().getOrThrowException();
    baseConfiguration = getBaseConfiguration(cmekKeyName);
    String bucket = BigQuerySinkUtils.configureBucket(baseConfiguration, config.getBucket(), runUUID.toString());
    if (!context.isPreviewEnabled()) {
      BigQuerySinkUtils.createResources(bigQuery, GCPUtils.getStorage(project, credentials),
                                        DatasetId.of(config.getDatasetProject(), config.getDataset()),
                                        bucket, config.getLocation(), cmekKeyName);
    }

    prepareRunInternal(context, bigQuery, bucket);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    String gcsPath;
    String bucket = getConfig().getBucket();
    if (bucket == null) {
      gcsPath = String.format("gs://%s", runUUID.toString());
    } else {
      gcsPath = String.format(gcsPathFormat, bucket, runUUID.toString());
    }
    try {
      BigQueryUtil.deleteTemporaryDirectory(baseConfiguration, gcsPath);
    } catch (IOException e) {
      LOG.warn("Failed to delete temporary directory '{}': {}", gcsPath, e.getMessage());
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<StructuredRecord, NullWritable>> emitter) {
    emitter.emit(new KeyValue<>(input, NullWritable.get()));
  }

  /**
   * Initializes output along with lineage recording for given table and its schema.
   *
   * @param context batch sink context
   * @param bigQuery big query client for the configured project
   * @param outputName output name
   * @param tableName table name
   * @param tableSchema table schema
   * @param bucket bucket name
   */
  protected final void initOutput(BatchSinkContext context, BigQuery bigQuery, String outputName, String tableName,
                                  @Nullable Schema tableSchema, String bucket,
                                  FailureCollector collector) throws IOException {
    LOG.debug("Init output for table '{}' with schema: {}", tableName, tableSchema);

    List<BigQueryTableFieldSchema> fields = BigQuerySinkUtils.getBigQueryTableFields(bigQuery, tableName, tableSchema,
      getConfig().isAllowSchemaRelaxation(), getConfig().getDatasetProject(),
      getConfig().getDataset(), getConfig().isTruncateTableSet(), collector);

    Configuration configuration = new Configuration(baseConfiguration);

    // Build GCS storage path for this bucket output.
    String temporaryGcsPath = BigQuerySinkUtils.getTemporaryGcsPath(bucket, runUUID.toString(), tableName);
    BigQuerySinkUtils.configureOutput(configuration,
                                      DatasetId.of(getConfig().getDatasetProject(), getConfig().getDataset()),
                                      tableName,
                                      temporaryGcsPath,
                                      fields);
    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exist.
    // We call emitLineage before since it creates the dataset with schema which is used.
    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());
    BigQuerySinkUtils.recordLineage(context, outputName, tableSchema, fieldNames);
    context.addOutput(Output.of(outputName, getOutputFormatProvider(configuration, tableName, tableSchema)));
  }

  /**
   * Child classes must provide configuration based on {@link AbstractBigQuerySinkConfig}.
   *
   * @return config instance
   */
  protected abstract AbstractBigQuerySinkConfig getConfig();

  /**
   * Child classes must override this method to provide specific validation logic to executed before
   * actual {@link #prepareRun(BatchSinkContext)} method execution.
   * For example, Batch Sink plugin can validate schema right away,
   * Batch Multi Sink does not have information at this point to do the validation.
   *
   * @param context batch sink context
   */
  protected abstract void prepareRunValidation(BatchSinkContext context);

  /**
   * Executes main prepare run logic, i.e. prepares output for given table (for Batch Sink plugin)
   * or for a number of tables (for Batch Multi Sink plugin).
   *
   * @param context batch sink context
   * @param bigQuery a big query client for the configured project
   * @param bucket bucket name
   */
  protected abstract void prepareRunInternal(BatchSinkContext context, BigQuery bigQuery,
                                             String bucket) throws IOException;

  /**
   * Returns output format provider instance specific to the child classes that extend this class.
   *
   * @param configuration Hadoop configuration
   * @param tableName table name
   * @param tableSchema table schema
   * @return output format provider
   */
  protected abstract OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                                  String tableName,
                                                                  Schema tableSchema);

  /**
   * Initialized base configuration needed to load data into BigQuery table.
   *
   * @return base configuration
   */
  private Configuration getBaseConfiguration(@Nullable CryptoKeyName cmekKeyName) throws IOException {
    AbstractBigQuerySinkConfig config = getConfig();
    Configuration baseConfiguration = BigQueryUtil.getBigQueryConfig(config.getServiceAccount(), config.getProject(),
                                                                     cmekKeyName, config.getServiceAccountType());
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION,
                                 config.isAllowSchemaRelaxation());
    baseConfiguration.setStrings(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
                                 config.getWriteDisposition().name());
    // this setting is needed because gcs has default chunk size of 64MB. This is large default chunk size which can
    // cause OOM issue if there are many tables being written. See this - CDAP-16670
    String gcsChunkSize = "8388608";
    if (!Strings.isNullOrEmpty(config.getGcsChunkSize())) {
      gcsChunkSize = config.getGcsChunkSize();
    }
    baseConfiguration.set("fs.gs.outputstream.upload.chunk.size", gcsChunkSize);
    return baseConfiguration;
  }

  /**
   * Creates Hadoop configuration instance
   *
   * @return Hadoop configuration
   */
  protected Configuration getOutputConfiguration() throws IOException {
    Configuration configuration = new Configuration(baseConfiguration);
    return configuration;
  }

}
