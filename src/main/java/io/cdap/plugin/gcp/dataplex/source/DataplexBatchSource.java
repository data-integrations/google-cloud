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

package io.cdap.plugin.gcp.dataplex.source;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.EntityName;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.StorageSystem;
import com.google.cloud.dataplex.v1.Task;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.source.BigQueryAvroToStructuredTransformer;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBatchSinkConfig;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Batch Source that reads data from Dataplex entities (BigQuery or Cloud Storage).
 * <p>
 * {@code Object} is the first parameter because that is the key used
 * by Hadoop's {@code TextInputFormat}.
 * Object is the second parameter because that is the value used
 * by Hadoop's {@code TextInputFormat}.
 * {@code StructuredRecord} is the third parameter because that is the output given by
 * Hadoop's {@code TextInputFormat}. All the plugins included with Hydrator operate on
 * StructuredRecord.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DataplexBatchSource.NAME)
@Description("Dataplex Source")
public class DataplexBatchSource extends BatchSource<Object, Object, StructuredRecord> {
  public static final String NAME = "Dataplex";
  private static final String BQ_TEMP_BUCKET_NAME_PREFIX = "dataplex-bq-source-bucket-";
  private static final String BQ_TEMP_BUCKET_NAME_TEMPLATE = BQ_TEMP_BUCKET_NAME_PREFIX + "%s";
  private static final String CONFIG_TEMPORARY_TABLE_NAME = "cdap.bq.source.temporary.table.name";
  private static final String GCS_TEMP_BUCKET_NAME = "dataplex-cdf-" + UUID.randomUUID();
  private static final String DATAPLEX_TASK_ARGS = "TASK_ARGS";

  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSource.class);
  private static Entity entity;
  private static String dataset;
  private static String datasetProject;
  private static Schema outputSchema;
  private static String tableId;
  private final BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
  private final DataplexBatchSourceConfig config;
  private Configuration configuration;
  private String bucketPath;

  public DataplexBatchSource(DataplexBatchSourceConfig dataplexBatchSourceConfig) {
    this.config = dataplexBatchSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    if (!config.getConnection().canConnect() || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
      (config.tryGetProject() == null)) {
      // ValidatingInputFormat plugin setup is mandatory.Otherwise pipeline will fail at runtime in case of GCS entity.
      config.setupValidatingInputFormat(pipelineConfigurer, collector, null);
      return;
    }
    GoogleCredentials credentials = config.validateAndGetServiceAccountCredentials(collector);
    collector.getOrThrowException();
    try {
      entity = config.getAndValidateEntityConfiguration(collector, credentials);
    } catch (IOException e) {
      collector.addFailure(e.getCause().getMessage(), "Please check credentials");
      return;
    }
    if (entity == null) {
      config.setupValidatingInputFormat(pipelineConfigurer, collector, null);
      return;
    }
    if (entity.getSystem().equals(StorageSystem.BIGQUERY)) {
      getEntityValuesFromDataPathForBQEntities(entity.getDataPath());
      config.validateBigQueryDataset(collector, datasetProject, dataset, tableId);
      if (config.getSchema(collector) == null) {
        Schema configuredSchema = DataplexUtil.getTableSchema(entity.getSchema(), collector);
        configurer.setOutputSchema(configuredSchema);
      }
      return;
    }
    // for Cloud Storage Entities
    config.checkMetastoreForGCSEntity(collector, credentials);
    config.setupValidatingInputFormat(pipelineConfigurer, collector, entity);

  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    GoogleCredentials credentials = config.validateAndGetServiceAccountCredentials(collector);
    collector.getOrThrowException();
    entity = config.getAndValidateEntityConfiguration(collector, credentials);
    if (entity == null) {
      throw new IOException(String.format("Pipeline failed. Entity %s does not exist", config.getEntity()));
    }
    if (entity.getSystem().equals(StorageSystem.BIGQUERY)) {
      getEntityValuesFromDataPathForBQEntities(entity.getDataPath());
      config.validateBigQueryDataset(collector, datasetProject, dataset, tableId);
      prepareRunBigQueryDataset(context);
    } else {
      config.checkMetastoreForGCSEntity(collector, credentials);
      prepareRunStorageBucket(context);
    }
  }

  private void getEntityValuesFromDataPathForBQEntities(String dataPath) {
    // dataPath will be in format 'projects/projectName/datasets/datasetName/tables/tableName'
    String[] entityValues = dataPath.split("/");
    if (entityValues.length >= 3) {
      dataset = entityValues[entityValues.length - 3];
      datasetProject = entityValues[1];
      tableId = entityValues[entityValues.length - 1];
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputSchema = config.getSchema(context.getFailureCollector());
    try (MetadataServiceClient metadataServiceClient =
           DataplexUtil.getMetadataServiceClient(config.getCredentials(context.getFailureCollector()))) {
      // entity will be required while calling transform method
      entity =
        metadataServiceClient.getEntity(EntityName.newBuilder().setProject(config.tryGetProject()).
          setLocation(config.getLocation()).setLake(config.getLake()).setZone(config.getZone()).
          setEntity(config.getEntity()).build());
    }
  }

  /**
   * this method will set up all the configuration to run the job in case of bigQuery entities.
   *
   * @param context
   * @throws Exception
   */
  private void prepareRunBigQueryDataset(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    outputSchema = DataplexUtil.getTableSchema(entity.getSchema(), collector);
    // Create BigQuery client
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = config.getCredentials(collector);
    BigQuery bigQuery = GCPUtils.getBigQuery(datasetProject, credentials);

    // Temporary bucket path without BQ template
    bucketPath = UUID.randomUUID().toString();

    configuration = BigQueryUtil.getBigQueryConfig(serviceAccount, config.getProject(), null,
      config.getServiceAccountType());

    // Configure temporay GCS Bucket to use
    String bucketName = BigQueryUtil.getStagingBucketName(context.getArguments().asMap(), config.getLocation(),
                                                          bigQuery.getDataset(DatasetId.of(datasetProject, dataset)),
                                                          null);
    String bucket = createBucket(configuration, config.getProject(), bigQuery, credentials, bucketName, bucketPath);

    // Configure Service account credentials
    configureServiceAccount(configuration, config.getConnection());

    // Configure BQ Source
    configureBigQuerySource();

    // Configure BigQuery input format.
    String temporaryGcsPath = BigQuerySourceUtils.getTemporaryGcsPath(bucket, bucketPath, bucketPath);
    BigQuerySourceUtils.configureBigQueryInput(configuration, DatasetId.of(datasetProject, dataset), tableId,
      temporaryGcsPath);
    configuration.set(DataplexConstants.DATAPLEX_ENTITY_TYPE, entity.getSystem().toString());
    TableDefinition.Type sourceTableType = config.getSourceTableType(datasetProject, dataset, tableId);
    emitLineage(context, outputSchema, sourceTableType);
    context.setInput(
      Input.of(config.getReferenceName(BigQueryUtil.getFQN(datasetProject, dataset, tableId)),
               new DataplexInputFormatProvider(configuration)));
  }

  /**
   * Set bigquery filter and partition values for filtered data.
   */
  private void configureBigQuerySource() {
    if (config.getPartitionFrom() != null) {
      configuration.set(BigQueryConstants.CONFIG_PARTITION_FROM_DATE, config.getPartitionFrom());
    }
    if (config.getPartitionTo() != null) {
      configuration.set(BigQueryConstants.CONFIG_PARTITION_TO_DATE, config.getPartitionTo());
    }
    if (config.getFilter() != null) {
      configuration.set(BigQueryConstants.CONFIG_FILTER, config.getFilter());
    }
  }


  private void emitLineage(BatchSourceContext context, Schema schema, TableDefinition.Type sourceTableType) {
    getEntityValuesFromDataPathForBQEntities(entity.getDataPath());
    String fqn = BigQueryUtil.getFQN(datasetProject, dataset, tableId);
    Asset asset = Asset.builder(
      config.getReferenceName(fqn)).setFqn(fqn).setLocation(config.getLocation()).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);

    String type = "table";
    if (TableDefinition.Type.VIEW == sourceTableType) {
      type = "view";
    } else if (TableDefinition.Type.MATERIALIZED_VIEW == sourceTableType) {
      type = "materialized view";
    }

    if (schema.getFields() != null) {
      this.recordLineage(lineageRecorder,
        schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()),
        String.format("Read from BigQuery Entity %s '%s' from Dataplex.",
          type, tableId));
    }
  }

  /**
   * this method will set up all the configuration to run the job in case of Cloud Storage entities.
   *
   * @param context BatchSourceContext
   * @throws InstantiationException
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void prepareRunStorageBucket(BatchSourceContext context)
    throws InstantiationException, IOException, ExecutionException, InterruptedException {
    FailureCollector collector = context.getFailureCollector();
    Job job = JobUtils.createInstance();
    configuration = job.getConfiguration();

    // Get storage and create temporary bucket to store task execution data from.
    Storage storage = GCPUtils.getStorage(config.getProject(), config.getCredentials(collector));
    createBucket(configuration, storage, config.getLocation(),
      GCS_TEMP_BUCKET_NAME);
    String outputLocation = GCSPath.SCHEME + GCS_TEMP_BUCKET_NAME;
    String query = formatQuery(entity, context.isPreviewEnabled());

    // Create Dataplex task to fetch filtered data. And set in configuration
    String taskId = createTask(outputLocation, query, collector);
    setConfigurationForDataplex(taskId);

    // Set Validating Input Format
    ValidatingInputFormat validatingInputFormat = config.getValidatingInputFormat(context);
    FileInputFormat.setInputDirRecursive(job, true);

    Schema schema = DataplexUtil.getTableSchema(entity.getSchema(), collector);
    io.cdap.plugin.common.Asset asset = io.cdap.plugin.common.Asset.builder(
      config.getReferenceName(entity.getDataPath()))
      .setFqn(entity.getDataPath()).setLocation(config.getLocation()).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);

    if (schema != null && schema.getFields() != null) {
      this.recordLineage(lineageRecorder, schema.getFields().stream().map(Schema.Field::getName).collect(
        Collectors.toList()), "Read from GCS entity in Dataplex.");
    }

    Iterator<Map.Entry<String, String>> propertiesIterator = config.getFileSystemProperties(outputLocation)
      .entrySet().iterator();
    while (propertiesIterator.hasNext()) {
      Map.Entry<String, String> entry = propertiesIterator.next();
      configuration.set(entry.getKey(), entry.getValue());
    }
    Path path = new Path(outputLocation);
    FileSystem pathFileSystem = FileSystem.get(path.toUri(), configuration);
    FileStatus[] fileStatus = pathFileSystem.globStatus(path);
    if (fileStatus == null) {
      throw new IOException(String.format("Input path %s does not exist", path));
    } else {
      FileInputFormat.addInputPath(job, path);
      Map<String, String> inputFormatConfiguration = validatingInputFormat.getInputFormatConfiguration();
      Iterator<Map.Entry<String, String>> inputFormatIterator = inputFormatConfiguration.entrySet().iterator();
      while (inputFormatIterator.hasNext()) {
        Map.Entry<String, String> propertyEntry = inputFormatIterator.next();
        configuration.set(propertyEntry.getKey(), propertyEntry.getValue());
      }
    }
    configuration.set(DataplexConstants.DATAPLEX_ENTITY_TYPE, entity.getSystem().toString());
    context.setInput(Input.of(config.getReferenceName(entity.getDataPath()),
                              new DataplexInputFormatProvider(configuration)));
  }

  /**
   * Set Dataplex properties in configuration
   *
   * @param taskId
   */
  private void setConfigurationForDataplex(String taskId) {
    configuration.set(DataplexConstants.DATAPLEX_TASK_ID, taskId);
    configuration.set(DataplexConstants.DATAPLEX_PROJECT_ID, config.tryGetProject());
    configuration.set(DataplexConstants.DATAPLEX_LOCATION, config.getLocation());
    configuration.set(DataplexConstants.DATAPLEX_LAKE, config.getLake());
    configuration.set(DataplexConstants.SERVICE_ACCOUNT_TYPE, config.getServiceAccountType());
    String serviceAccountFilePath = config.getServiceAccountFilePath() != null ? config.getServiceAccountFilePath()
      : DataplexConstants.NONE;
    configuration.set(DataplexConstants.SERVICE_ACCOUNT_FILEPATH, serviceAccountFilePath);
  }

  private void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields, String description) {
    lineageRecorder.recordRead("Read", description,
      outputFields);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (entity.getSystem().equals(StorageSystem.BIGQUERY)) {
      BigQuerySourceUtils.deleteGcsTemporaryDirectory(configuration, null, bucketPath);
      String temporaryTable = configuration.get(CONFIG_TEMPORARY_TABLE_NAME);
      Credentials credentials = config.getCredentials(context.getFailureCollector());
      BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
      bigQuery.delete(TableId.of(datasetProject, dataset, temporaryTable));
      LOG.debug("Deleted temporary table '{}'", temporaryTable);
    } else {
      Storage storage = GCPUtils.getStorage(config.tryGetProject(),
        config.getCredentials(context.getFailureCollector()));
      // Delete the directory first and then delete empty storage bucket
      BigQuerySourceUtils.deleteGcsTemporaryDirectory(configuration, GCS_TEMP_BUCKET_NAME, "projects");
      storage.delete(GCS_TEMP_BUCKET_NAME);
      LOG.debug("Deleted temporary bucket '{}'.", GCS_TEMP_BUCKET_NAME);
    }
  }

  /**
   * Create task object
   *
   * @param outputLocation GCS output path
   * @param query          Query to run on spark while calling dataplex task.
   * @param collector      FailureCollector
   * @return
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private String createTask(String outputLocation, String query, FailureCollector collector)
    throws IOException, ExecutionException, InterruptedException {
    String taskArgs = "--output_location,%s, --output_format, %s";
    Task.TriggerSpec triggerSpec = Task.TriggerSpec.newBuilder().setType(Task.TriggerSpec.Type.ON_DEMAND).build();
    Task.ExecutionSpec executionSpec = Task.ExecutionSpec.newBuilder()
      .setServiceAccount(config.getServiceAccountEmail()).putArgs(DATAPLEX_TASK_ARGS, String.format(taskArgs,
        outputLocation, DataplexBatchSourceConfig.INPUT_FORMAT)).build();
    Task.SparkTaskConfig spark = Task.SparkTaskConfig.newBuilder().setSqlScript(query).build();
    Task task = Task.newBuilder().setTriggerSpec(triggerSpec).setDescription("task-" + UUID.randomUUID()).
      setExecutionSpec(executionSpec).setSpark(spark).build();

    try (DataplexServiceClient dataplexServiceClient =
           DataplexUtil.getDataplexServiceClient(config.getCredentials(collector))) {
      task = dataplexServiceClient.createTaskAsync(LakeName.newBuilder().setLake(config.getLake()).
          setProject(config.tryGetProject()).setLocation(config.getLocation()).build(), task, task.getDescription())
        .get();
    }
    return task.getDescription();
  }

  /**
   * Format query for Cloud storage entities.
   *
   * @param entity           Dataplex entity
   * @param isPreviewEnabled
   * @return
   */
  private String formatQuery(Entity entity, boolean isPreviewEnabled) {
    String queryTemplate = "select * from %s.%s %s";
    StringBuilder condition = new StringBuilder();
    if (!Strings.isNullOrEmpty(config.getFilter())) {
      condition.append("where ").append(config.getFilter());
    }

    // In case of preview limit the job to 1000 records
    condition.append(isPreviewEnabled ? " LIMIT 1000;" : ";");

    return String.format(queryTemplate, config.getZone(), entity.getId(), condition);
  }

  @Override
  public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter)
    throws IOException {
    if (entity.getSystem().equals(StorageSystem.BIGQUERY)) {
      StructuredRecord transformed = outputSchema == null ?
        transformer.transform((GenericData.Record) input.getValue()) :
        transformer.transform((GenericData.Record) input.getValue(), outputSchema);
      emitter.emit(transformed);
    } else {
      emitter.emit((StructuredRecord) input.getValue());
    }
  }

  /**
   * Gets bucket from supplied configuration.
   * <p>
   * If the supplied configuration doesn't specify a bucket, a bucket will get auto created and configuration modified
   * to auto-delete this bucket on completion.
   *
   * @param configuration Hadoop configuration instance.
   * @param project       GCP projectId
   * @param bigQuery      bigquery client
   * @param credentials   GCP credentials
   * @param bucket        bucket name
   * @param bucketPath    bucket path to use. Will be used as a bucket name if needed..
   * @return Bucket name.
   */
  private String createBucket(Configuration configuration,
                              String project,
                              BigQuery bigQuery,
                              Credentials credentials,
                              @Nullable String bucket,
                              String bucketPath) throws IOException {
    if (bucket == null) {
      bucket = String.format(BQ_TEMP_BUCKET_NAME_TEMPLATE, bucketPath);
      // By default, this option is false, meaning the job can not delete the bucket.
      configuration.setBoolean("fs.gs.bucket.delete.enable", true);
    }
    // the dataset existence is validated before, so this cannot be null
    Dataset bigQueryDataset = bigQuery.getDataset(DatasetId.of(datasetProject, dataset));
    createBucket(configuration, GCPUtils.getStorage(project, credentials),
      bigQueryDataset.getLocation(), bucket);
    return bucket;
  }

  /**
   * Sets up service account credentials into supplied Hadoop configuration.
   *
   * @param configuration Hadoop Configuration instance.
   * @param config        BigQuery connection configuration.
   */
  private void configureServiceAccount(Configuration configuration, GCPConnectorConfig config) {
    if (config.getServiceAccount() != null) {
      configuration.set(BigQueryConstants.CONFIG_SERVICE_ACCOUNT, config.getServiceAccount());
      configuration.setBoolean(BigQueryConstants.CONFIG_SERVICE_ACCOUNT_IS_FILE, config.isServiceAccountFilePath());
    }
  }

  /**
   * Gets bucket from supplied configuration.
   * <p>
   * If the supplied configuration doesn't specify a bucket, a bucket will get auto created and configuration modified
   * to auto-delete this bucket on completion.
   *
   * @param configuration Hadoop configuration instance.
   * @param storage       GCS Storage client
   * @param bucket        GCS bucket
   * @param location      location
   * @return Bucket name.
   */
  private String createBucket(Configuration configuration,
                              Storage storage, String location,
                              @Nullable String bucket) throws IOException {
    // Create a new bucket if needed
    if (storage != null && storage.get(bucket) == null) {
      try {
        configuration.setBoolean("fs.gs.bucket.delete.enable", true);
        GCPUtils.createBucket(storage, bucket, location, null);

      } catch (StorageException e) {
        if (e.getCode() == 409) {
          // A conflict means the bucket already exists
          // This most likely means multiple stages in the same pipeline are trying to create the same bucket.
          // Ignore this and move on, since all that matters is that the bucket exists.
          return bucket;
        }
        throw new IOException(String.format("Unable to create Cloud Storage bucket '%s'. ",
          bucket), e);
      }
    }
    return bucket;
  }

}
