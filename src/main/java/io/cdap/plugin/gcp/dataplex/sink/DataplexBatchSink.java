/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.api.gax.rpc.ApiException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.CreateEntityRequest;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.EntityName;
import com.google.cloud.dataplex.v1.GetEntityRequest;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.StorageFormat;
import com.google.cloud.dataplex.v1.StorageSystem;
import com.google.cloud.dataplex.v1.UpdateEntityRequest;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.bigquery.sink.AbstractBigQuerySink;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sink.PartitionType;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBatchSinkConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;
import io.cdap.plugin.gcp.gcs.sink.GCSBatchSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Batch Sink that writes data to Dataplex assets (BigQuery or Cloud Storage).
 * <p>
 * {@code StructuredRecord} is the first parameter because that is what the
 * sink will take as an input.
 * Object is the second parameter because that is the key used
 * by Hadoop's {@code TextOutputFormat}.
 * {@code Object} is the third parameter because that is the value used by
 * Hadoop's {@code TextOutputFormat}. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DataplexBatchSink.NAME)
@Description("Ingests and processes data within Dataplex.")
public final class DataplexBatchSink extends BatchSink<StructuredRecord, Object, Object> {
  public static final String NAME = "Dataplex";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSink.class);
  private static final String RECORDS_UPDATED_METRIC = "records.updated";
  // Usually, you will need a private variable to store the config that was passed to your class
  private final DataplexBatchSinkConfig config;
  // UUID for the run. Will be used as bucket name for BigQuery assets.
  // UUID is used since GCS bucket names must be globally unique.
  private final UUID runUUID = UUID.randomUUID();
  protected Configuration baseConfiguration;
  protected BigQuery bigQuery;
  private String outputPath;
  private Asset asset;
  private Entity entityBean = null;

  public DataplexBatchSink(DataplexBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    GoogleCredentials credentials = config.validateAndGetServiceAccountCredentials(collector);
    try (DataplexServiceClient dataplexServiceClient = DataplexUtil.getDataplexServiceClient(credentials)) {
      if (!config.getConnection().canConnect() || config.getServiceAccountType() == null ||
        (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
        (config.tryGetProject() == null)) {
        return;
      }
      Schema inputSchema = configurer.getInputSchema();
      Schema configuredSchema = config.getSchema(collector);
      config.validateAssetConfiguration(collector, dataplexServiceClient);
      if (config.getAssetType().equals(DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE)) {
        config.validateBigQueryDataset(inputSchema, configuredSchema, collector, dataplexServiceClient);
        return;
      }
      if (config.getAssetType().equals(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE)) {
        config.validateStorageBucket(collector);
        config.validateFormatForStorageBucket(pipelineConfigurer, collector);
        if (config.isUpdateDataplexMetadata()) {
          prepareDataplexMetadataUpdate(collector, configuredSchema);
        }
        return;
      }
    } catch (IOException e) {
      collector.addFailure(e.getMessage(), null);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    GoogleCredentials credentials = config.validateAndGetServiceAccountCredentials(collector);
    try (DataplexServiceClient dataplexServiceClient =
           DataplexUtil.getDataplexServiceClient(credentials)) {
      config.validateAssetConfiguration(collector, dataplexServiceClient);

      asset =
        dataplexServiceClient.getAsset(AssetName.newBuilder().setProject(config.tryGetProject())
          .setLocation(config.getLocation())
          .setLake(config.getLake()).setZone(config.getZone()).setAsset(config.getAsset()).build());

      if (config.getAssetType().equals(DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE)) {
        config.validateBigQueryDataset(context.getInputSchema(), context.getOutputSchema(), collector,
          dataplexServiceClient);
        prepareRunBigQueryDataset(context);
      }
      if (config.getAssetType().equals(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE)) {
        config.validateStorageBucket(collector);
        if (config.isUpdateDataplexMetadata()) {
          prepareDataplexMetadataUpdate(collector, config.getSchema(collector));
        }
        prepareRunStorageBucket(context);
      }
    }
  }

  /**
   * O/P Template parameters will get changed based on asset type. E.g. <StructuredRecord, NullWritable> for BQ Dataset
   *
   * @param input
   * @param emitter
   */

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter) {
    if (this.config.getAssetType().equalsIgnoreCase(DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE)) {
      emitter.emit(new KeyValue<>(input, NullWritable.get()));
    } else {
      emitter.emit(new KeyValue<>(NullWritable.get(), input));
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (this.config.getAssetType().equalsIgnoreCase(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE)) {
      emitMetricsForStorageBucket(succeeded, context);
      // Create entity when pipeline run has succeeded to enable manual discovery in dataplex.
      // CreateEntity API only allows CRUD operations for GCS assets
      if (succeeded && config.isUpdateDataplexMetadata()) {
        FailureCollector collector = context.getFailureCollector();
        GoogleCredentials googleCredentials = config.validateAndGetServiceAccountCredentials(collector);
        Schema schema = config.getSchema(collector);
        if (schema == null) {
          schema = context.getInputSchema();
        }
        String bucketName = "";
        try {
          bucketName = asset.getResourceSpec().getName();
        } catch (StorageException e) {
          throw new RuntimeException(
            "Unable to read bucket name. See error details for more information ", e);
        }
        try (
          DataplexServiceClient dataplexServiceClient =
            DataplexUtil.getDataplexServiceClient(googleCredentials)
        ) {

          String assetFullPath = DataplexConstants.STORAGE_BUCKET_PATH_PREFIX + bucketName +
            "/" + config.getTable();
          configureDataplexMetadataUpdate(googleCredentials, assetFullPath,
                                                       StorageSystem.CLOUD_STORAGE, schema);
        } catch (ApiException | IOException e) {
          throw new RuntimeException(
            String.format("Unable create entity for bucket %s. ", bucketName)
              + "See error details for more information.", e);
        }
      }
      return;
    }

    Path gcsPath = new Path(DataplexConstants.STORAGE_BUCKET_PATH_PREFIX + runUUID);
    try {
      FileSystem fs = gcsPath.getFileSystem(baseConfiguration);
      if (fs.exists(gcsPath)) {
        fs.delete(gcsPath, true);
        LOG.debug("Deleted temporary directory '{}'", gcsPath);
      }
      emitMetricsForBigQueryDataset(succeeded, context);
    } catch (IOException e) {
      LOG.warn("Failed to delete temporary directory '{}': {}", gcsPath, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Exception while trying to emit metric. No metric will be emitted for the number of affected rows.",
        exception);
    }
  }

  /**
   * prepare Run for BigQuery Dataset Asset. It will create necessary resources, and configuration methods
   *
   * @param context
   * @throws Exception
   */
  private void prepareRunBigQueryDataset(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    Credentials credentials = config.getCredentials(collector);
    String project = config.getProject();
    String cmekKey = context.getArguments().get(CmekUtils.CMEK_KEY);
    CryptoKeyName cmekKeyName = null;
    if (!Strings.isNullOrEmpty(cmekKey)) {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    }
    baseConfiguration = getBaseConfiguration(cmekKeyName);
    // asset.getResourceSpec().getName() will be of format 'projects/datasetProjectName/datasets/datasetName'
    String[] assetValues = asset.getResourceSpec().getName().split("/");
    String datasetName = assetValues[assetValues.length - 1];
    String datasetProject = assetValues[assetValues.length - 3];
    bigQuery = GCPUtils.getBigQuery(datasetProject, credentials);
    // Get required dataset ID and dataset instance (if it exists)
    DatasetId datasetId = DatasetId.of(datasetProject, datasetName);
    Dataset dataset = bigQuery.getDataset(datasetId);
    String bucket = BigQueryUtil.getStagingBucketName(context.getArguments().asMap(), config.getLocation(),
                                                      dataset, null);
    String fallbackBucketName = "dataplex-" + runUUID;
    bucket = BigQuerySinkUtils.configureBucket(baseConfiguration, bucket, fallbackBucketName);
    if (!context.isPreviewEnabled()) {
      BigQuerySinkUtils.createResources(bigQuery, GCPUtils.getStorage(project, credentials),
        DatasetId.of(datasetProject, datasetName),
        bucket, config.getLocation(), cmekKeyName);
    }

    Schema configSchema = config.getSchema(collector);
    Schema outputSchema = configSchema == null ? context.getInputSchema() : configSchema;
    configureTable(outputSchema, datasetName, datasetProject, collector);
    configureBigQuerySink();
    initOutput(context, bigQuery,
               config.getReferenceName(BigQueryUtil.getFQN(datasetProject, datasetName, config.getTable())),
               config.getTable(), outputSchema, bucket, collector, datasetName, datasetProject);
  }


  /**
   * Sets addition configuration for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureBigQuerySink() {
    baseConfiguration.set(BigQueryConstants.CONFIG_JOB_ID, runUUID.toString());
    if (config.getPartitionByField() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_PARTITION_BY_FIELD, config.getPartitionByField());
    }
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_REQUIRE_PARTITION_FILTER,
      config.isRequirePartitionField());
    if (config.getClusteringOrder() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_CLUSTERING_ORDER, config.getClusteringOrder());
    }
    baseConfiguration.set(BigQueryConstants.CONFIG_OPERATION, config.getOperation().name());
    if (config.getTableKey() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_TABLE_KEY, config.getTableKey());
    }
    if (config.getDedupeBy() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_DEDUPE_BY, config.getDedupeBy());
    }
    if (config.getPartitionFilter() != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_PARTITION_FILTER, config.getPartitionFilter());
    }

    PartitionType partitioningType = config.getPartitioningType();
    baseConfiguration.setEnum(BigQueryConstants.CONFIG_PARTITION_TYPE, partitioningType);

    if (config.getRangeStart() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_START, config.getRangeStart());
    }
    if (config.getRangeEnd() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_END, config.getRangeEnd());
    }
    if (config.getRangeInterval() != null) {
      baseConfiguration.setLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_INTERVAL, config.getRangeInterval());
    }
  }

  /**
   * Sets the output table for the AbstractBigQuerySink's Hadoop configuration
   */
  private void configureTable(Schema schema, String dataset, String datasetProject, FailureCollector collector) {
    Table table = BigQueryUtil.getBigQueryTable(datasetProject, dataset,
      config.getTable(),
      config.getServiceAccount(),
      config.isServiceAccountFilePath(),
      collector);
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_DESTINATION_TABLE_EXISTS, table != null);
    List<String> tableFieldsNames = null;
    if (table != null) {
      tableFieldsNames = Objects.requireNonNull(table.getDefinition().getSchema()).getFields().stream()
        .map(Field::getName).collect(Collectors.toList());
    } else if (schema != null) {
      tableFieldsNames = schema.getFields().stream()
        .map(Schema.Field::getName).collect(Collectors.toList());
    }
    if (tableFieldsNames != null) {
      baseConfiguration.set(BigQueryConstants.CONFIG_TABLE_FIELDS, String.join(",", tableFieldsNames));
    }
  }

  /**
   * Initialized base configuration needed to load data into BigQuery table.
   *
   * @return base configuration
   */
  private Configuration getBaseConfiguration(@Nullable CryptoKeyName cmekKey) throws IOException {
    Configuration baseConfiguration = BigQueryUtil.getBigQueryConfig(config.getServiceAccount(), config.getProject(),
      cmekKey, config.getServiceAccountType());
    baseConfiguration.setBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION,
      config.isUpdateTableSchema());
    baseConfiguration.setStrings(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION.getKey(),
      config.getWriteDisposition().name());
    // this setting is needed because gcs has default chunk size of 64MB. This is large default chunk size which can
    // cause OOM issue if there are many tables being written. See this - CDAP-16670
    String gcsChunkSize = "8388608";
    baseConfiguration.set("fs.gs.outputstream.upload.chunk.size", gcsChunkSize);
    return baseConfiguration;
  }


  /**
   * Initializes output along with lineage recording for given table and its schema.
   *
   * @param context     batch sink context
   * @param bigQuery    big query client for the configured project
   * @param outputName  output name
   * @param tableName   table name
   * @param tableSchema table schema
   * @param bucket      bucket name
   */
  protected void initOutput(BatchSinkContext context, BigQuery bigQuery, String outputName, String tableName,
                            @Nullable Schema tableSchema, String bucket,
                            FailureCollector collector, String dataset, String datasetProject) throws IOException {
    LOG.debug("Init output for table '{}' with schema: {}", tableName, tableSchema);

    List<BigQueryTableFieldSchema> fields = BigQuerySinkUtils.getBigQueryTableFields(bigQuery, tableName, tableSchema,
      this.config.isUpdateTableSchema(), datasetProject, dataset, this.config.isTruncateTable(), collector);

    Configuration configuration = new Configuration(baseConfiguration);

    // Build GCS storage path for this bucket output.
    DatasetId datasetId = DatasetId.of(datasetProject, dataset);
    String temporaryGcsPath = BigQuerySinkUtils.getTemporaryGcsPath(bucket, runUUID.toString(), tableName);
    BigQuerySinkUtils.configureOutput(configuration, datasetId, tableName, temporaryGcsPath, fields);
    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exist.
    // We call emitLineage before since it creates the dataset with schema which is used.
    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());
    String fqn = BigQueryUtil.getFQN(datasetProject, dataset, config.getTable());
    String location = bigQuery.getDataset(datasetId).getLocation();
    io.cdap.plugin.common.Asset lineageAsset = io.cdap.plugin.common.Asset.builder(
      config.getReferenceName(fqn))
      .setFqn(fqn).setLocation(location).build();
    BigQuerySinkUtils.recordLineage(context, lineageAsset, tableSchema, fieldNames, null);
    configuration.set(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE, DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE);
    context.addOutput(Output.of(outputName, new DataplexOutputFormatProvider(configuration, tableSchema, null)));
  }

  void emitMetricsForBigQueryDataset(boolean succeeded, BatchSinkContext context) {
    if (!succeeded) {
      return;
    }
    Job queryJob = bigQuery.getJob(getJobId());
    if (queryJob == null) {
      LOG.warn("Unable to find BigQuery job. No metric will be emitted for the number of affected rows.");
      return;
    }
    long totalRows = getTotalRows(queryJob);
    LOG.info("Job {} affected {} rows", queryJob.getJobId(), totalRows);
    //work around since StageMetrics count() only takes int as of now
    int cap = 10000; // so the loop will not cause significant delays
    long count = totalRows / Integer.MAX_VALUE;
    if (count > cap) {
      LOG.warn("Total record count is too high! Metric for the number of affected rows may not be updated correctly");
    }
    count = count < cap ? count : cap;
    for (int i = 0; i <= count && totalRows > 0; i++) {
      int rowCount = totalRows < Integer.MAX_VALUE ? (int) totalRows : Integer.MAX_VALUE;
      context.getMetrics().count(AbstractBigQuerySink.RECORDS_UPDATED_METRIC, rowCount);
      totalRows -= rowCount;
    }
  }

  private JobId getJobId() {
    return JobId.newBuilder().setLocation(config.getLocation()).setJob(runUUID.toString()).build();
  }

  private long getTotalRows(Job queryJob) {
    JobConfiguration.Type type = queryJob.getConfiguration().getType();
    if (type == JobConfiguration.Type.LOAD) {
      return ((JobStatistics.LoadStatistics) queryJob.getStatistics()).getOutputRows();
    } else if (type == JobConfiguration.Type.QUERY) {
      return ((JobStatistics.QueryStatistics) queryJob.getStatistics()).getNumDmlAffectedRows();
    }
    LOG.warn("Unable to identify BigQuery job type. No metric will be emitted for the number of affected rows.");
    return 0;
  }

  /**
   * This method will perform prepareRun tasks for Storage Bucket Asset.
   *
   * @param context
   * @throws Exception
   */
  private void prepareRunStorageBucket(BatchSinkContext context) throws Exception {
    ValidatingOutputFormat validatingOutputFormat = validateOutputFormatForRun(context);
    FailureCollector collector = context.getFailureCollector();
    String cmekKey = context.getArguments().get(CmekUtils.CMEK_KEY);
    CryptoKeyName cmekKeyName = null;
    if (!Strings.isNullOrEmpty(cmekKey)) {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    }
    Credentials credentials = config.getCredentials(collector);
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    Bucket bucket;
    String bucketName = "";
    try {
      bucketName = asset.getResourceSpec().getName();
      bucket = storage.get(bucketName);
    } catch (StorageException e) {
      throw new RuntimeException(
        String.format("Unable to access or create bucket %s. ", bucketName)
          + "Ensure you entered the correct bucket path and have permissions for it.", e);
    }
    if (bucket == null) {
      GCPUtils.createBucket(storage, bucketName, config.getLocation(), cmekKeyName);
    }
    String outputDir = getOutputDir(context.getLogicalStartTime());
    Map<String, String> outputProperties = getStorageBucketOutputProperties(validatingOutputFormat, outputDir);
    
    // record field level lineage information
    // needs to happen before context.addOutput(), otherwise an external dataset without schema will be created.
    Schema schema = config.getSchema(collector);
    if (schema == null) {
      schema = context.getInputSchema();
    }
    io.cdap.plugin.common.Asset asset = io.cdap.plugin.common.Asset.builder(
      config.getReferenceName(outputDir))
      .setFqn(outputDir).setLocation(config.getLocation()).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);
    if (schema != null && schema.getFields() != null && !schema.getFields().isEmpty()) {
      recordLineage(lineageRecorder,
                    schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
    context.addOutput(Output.of(config.getReferenceName(outputDir),
      new SinkOutputFormatProvider(validatingOutputFormat.getOutputFormatClassName(), outputProperties)));
  }

  /**
   * Validates output format for run  and return ValidatingOutputFormat
   *
   * @param context
   * @throws Exception
   */
  private ValidatingOutputFormat validateOutputFormatForRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    String format = config.getFormat().toString().toLowerCase(Locale.ROOT);
    ValidatingOutputFormat validatingOutputFormat = getOutputFormatForRun(context);
    FormatContext formatContext = new FormatContext(collector, context.getInputSchema());
    config.validateOutputFormatProvider(formatContext, format, validatingOutputFormat);
    collector.getOrThrowException();
    return validatingOutputFormat;
  }

  protected Map<String, String> getFileSystemProperties() {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(config.getConnection(),
      outputPath, new HashMap<>());
    properties.put(GCSBatchSink.CONTENT_TYPE, config.getContentType(config.getFormat().toString()));
    return properties;
  }

  protected Map<String, String> getStorageBucketOutputProperties(ValidatingOutputFormat validatingOutputFormat,
                                                                 String outputDir) {
    Map<String, String> outputProperties = new HashMap<>(validatingOutputFormat.getOutputFormatConfiguration());
    outputProperties.put(FileOutputFormat.OUTDIR, outputDir);
    outputProperties.put(DataplexOutputFormatProvider.DATAPLEX_OUTPUT_BASE_DIR, outputDir);
    outputProperties.put(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE, config.getAssetType());
    outputProperties.putAll(getFileSystemProperties());
    // Added to not create _SUCCESS file in the output directory.
    outputProperties.put("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    // Added to not write metadata files for parquet format in the output directory.
    if (config.getFormat().equals(FileFormat.PARQUET)) {
      outputProperties.put("parquet.enable.summary-metadata", "false");
    }
    return outputProperties;
  }

  /**
   * It will instantiate and return the ValidatingOutputFormat object based on file Format selected by user
   *
   * @param context
   * @return
   * @throws InstantiationException
   */
  protected ValidatingOutputFormat getOutputFormatForRun(BatchSinkContext context) throws InstantiationException {
    String fileFormat = config.getFormat().toString().toLowerCase();
    try {
      ValidatingOutputFormat validatingOutputFormat = context.newPluginInstance(fileFormat);
      return new DataplexOutputFormatProvider(null, null, validatingOutputFormat);
    } catch (InvalidPluginConfigException e) {
      Set<String> properties = new HashSet<>(e.getMissingProperties());
      for (InvalidPluginProperty invalidProperty : e.getInvalidProperties()) {
        properties.add(invalidProperty.getName());
      }
      String errorMessage = String.format("Format '%s' cannot be used because properties %s were not provided or " +
          "were invalid when the pipeline was deployed. Set the format to a " +
          "different value, or re-create the pipeline with all required properties.",
        fileFormat, properties);
      throw new IllegalArgumentException(errorMessage, e);
    }
  }

  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Storage.", outputFields);
  }

  /**
   * it will return the output directory path in format gs://bucket/folder
   *
   * @param logicalStartTime
   * @return
   */
  protected String getOutputDir(long logicalStartTime) {
    String suffix = config.getSuffix();
    String defaultTimestampFormat = "yyyy-MM-dd-HH-mm";
    String tableName = config.getTable();
    suffix = Strings.isNullOrEmpty(suffix) ? defaultTimestampFormat : suffix;
    String timeSuffix = String.format("%s=%s",
      DataplexConstants.STORAGE_BUCKET_PARTITION_KEY,
      new SimpleDateFormat(suffix).format(logicalStartTime)
    );
    String configPath = GCSPath.SCHEME + asset.getResourceSpec().getName();
    String finalPath = String.format("%s/%s/%s/", configPath, tableName, timeSuffix);
    this.outputPath = finalPath;
    return finalPath;
  }

  private void emitMetricsForStorageBucket(boolean succeeded, BatchSinkContext context) {
    if (!succeeded) {
      return;
    }
    try {
      StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccount(),
        config.isServiceAccountFilePath());
      storageClient.mapMetaDataForAllBlobs(outputPath,
        new MetricsEmitter(context.getMetrics())::emitMetrics);
    } catch (Exception e) {
      LOG.warn("Metrics for the number of affected rows in GCS Sink maybe incorrect.", e);
    }
  }

  /**
   * Prepares metadata update in Dataplex if update dataplex metadata is enabled
   *
   * @param collector
   * @param schema
   * @throws IOException
   */
  private void prepareDataplexMetadataUpdate(FailureCollector collector, Schema schema)
    throws IOException {
    Optional<Schema.Field> partitionKey = Objects.requireNonNull(schema.getFields()).stream()
      .filter(avroField -> avroField.getName().equals(DataplexConstants.STORAGE_BUCKET_PARTITION_KEY)).findAny();

    if (partitionKey.isPresent()) {
      collector.addFailure(
        String.format("Field '%s' is used by dataplex sink to create time partitioned layout on GCS." +
          " To avoid conflict, presence of a column with the name '%s' on the input schema is not allowed.",
                      DataplexConstants.STORAGE_BUCKET_PARTITION_KEY, DataplexConstants.STORAGE_BUCKET_PARTITION_KEY),
        String.format(
          "Remove '%s' field from the output schema or rename the '%s' field in the input schema by adding" +
            " a transform step.",
          DataplexConstants.STORAGE_BUCKET_PARTITION_KEY, DataplexConstants.STORAGE_BUCKET_PARTITION_KEY
        )
      );
    }

    String entityID = config.getTable().replaceAll("[^a-zA-Z0-9_]", "_");
    try (MetadataServiceClient metadataServiceClient =
           DataplexUtil.getMetadataServiceClient(config.getCredentials(collector))) {
      entityBean =
        metadataServiceClient.getEntity(GetEntityRequest.newBuilder().setName(EntityName.of(
            config.tryGetProject(), config.getLocation(), config.getLake(), config.getZone(), entityID).toString())
                                          .setView(GetEntityRequest.EntityView.FULL)
                                          .build());
    } catch (ApiException e) {
      int statusCode = e.getStatusCode().getCode().getHttpStatusCode();
      if (statusCode != 404) {
        collector.addFailure("Unable to fetch entity information.", null);
      }
    }

    if (entityBean != null && entityBean.getSchema().getUserManaged() == false) {
      collector.addFailure("Entity already exists, but the schema is not user-managed.", null);
    }
  }

  /**
   * Configures metadata update in Dataplex if update dataplex metadata is enabled
   *
   * @param credentials
   * @param assetFullPath
   * @param storageSystem
   * @param schema
   * @throws IOException
   */
  private void configureDataplexMetadataUpdate(
    GoogleCredentials credentials, String assetFullPath, StorageSystem storageSystem, Schema schema
  ) throws IOException {
    String entityID = config.getTable().replaceAll("[^a-zA-Z0-9_]", "_");
    try (MetadataServiceClient metadataServiceClient =
           DataplexUtil.getMetadataServiceClient(credentials)) {
      com.google.cloud.dataplex.v1.Schema dataplexSchema = DataplexUtil.getDataplexSchema(schema);
      Entity.Builder entityBuilder = Entity.newBuilder()
        .setId(entityID)
        .setAsset(config.getAsset())
        .setDataPath(assetFullPath)
        .setType(Entity.Type.TABLE)
        .setSystem(storageSystem)
        .setSchema(dataplexSchema)
        .setFormat(StorageFormat
                     .newBuilder()
                     .setMimeType(DataplexUtil.getStorageFormatForEntity(config.getFormatStr()))
                     .build()
        );

      if (entityBean != null) {
        try {
          entityBean = metadataServiceClient.updateEntity(
            UpdateEntityRequest.newBuilder()
              .setEntity(entityBuilder
                           .setName(entityBean.getName())
                           .setEtag(entityBean.getEtag())
                           .build()
              )
              .build()
          );
        } catch (ApiException e) {
          throw new RuntimeException(
            String.format("%s: %s", "There was a problem updating the entity for metadata updates.", e.getMessage()));
        }
      } else {
        try {
          String entityParent = "projects/" + config.tryGetProject() +
            "/locations/" + config.getLocation() +
            "/lakes/" + config.getLake() +
            "/zones/" + config.getZone();
          entityBean = metadataServiceClient.createEntity(
            CreateEntityRequest.newBuilder()
              .setParent(entityParent)
              .setEntity(entityBuilder.build())
              .build()
          );
        } catch (ApiException e) {
          throw new RuntimeException(
            String.format("%s: %s", "There was a problem creating the entity for metadata updates.", e.getMessage()));
        }
      }
      try {
        DataplexUtil.addPartitionInfo(entityBean, credentials,
                                    asset.getResourceSpec().getName(), config.getTable(), config.getProject());
      } catch (ApiException e) {
        // extract last 14 chars of the error message to make sure it's "already exists" and safe to ignore
        String errMessage = e.getMessage().substring(e.getMessage().length() - 14);
        if (!errMessage.equals("already exists")) {
          throw new RuntimeException(String.format("Unable to create add partition information for %s. ", entityID));
        }
      }
    }
  }

  private static class MetricsEmitter {
    private final StageMetrics stageMetrics;

    private MetricsEmitter(StageMetrics stageMetrics) {
      this.stageMetrics = stageMetrics;
    }

    public void emitMetrics(Map<String, String> metaData) {
      long totalRows = extractRecordCount(metaData);
      if (totalRows == 0) {
        return;
      }

      // work around since StageMetrics count() only takes int as of now
      int cap = 10000; // so the loop will not cause significant delays
      long count = totalRows / Integer.MAX_VALUE;
      if (count > cap) {
        LOG.warn("Total record count is too high! Metric for the number of affected rows may not be updated correctly");
      }
      count = count < cap ? count : cap;
      for (int i = 0; i <= count && totalRows > 0; i++) {
        int rowCount = totalRows < Integer.MAX_VALUE ? (int) totalRows : Integer.MAX_VALUE;
        stageMetrics.count(RECORDS_UPDATED_METRIC, rowCount);
        totalRows -= rowCount;
      }
    }

    private long extractRecordCount(Map<String, String> metadata) {
      String value = metadata.get(GCSBatchSink.RECORD_COUNT);
      return value == null ? 0L : Long.parseLong(value);
    }
  }

}
