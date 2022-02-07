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

package io.cdap.plugin.gcp.dataplex.source;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.gcp.bigquery.source.BigQueryAvroToStructuredTransformer;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.common.connection.impl.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.common.exception.DataplexException;
import io.cdap.plugin.gcp.dataplex.common.model.Entity;
import io.cdap.plugin.gcp.dataplex.common.model.Task;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
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
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Dataplex Batch Source Plugin
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DataplexBatchSource.NAME)
@Description("Dataplex Source")
public class DataplexBatchSource extends BatchSource<Object, Object, StructuredRecord> {
  public static final String NAME = "Dataplex";

  private static final String BQ_TEMP_BUCKET_NAME_PREFIX = "bq-source-bucket-";
  private static final String BQ_TEMP_BUCKET_NAME_TEMPLATE = BQ_TEMP_BUCKET_NAME_PREFIX + "%s";
  private static final String CONFIG_TEMPORARY_TABLE_NAME = "cdap.bq.source.temporary.table.name";
  private static final String GCS_TEMP_BUCKET_NAME = "dp-cdf-" + UUID.randomUUID();

  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSource.class);
  private static Entity entityBean;
  private static String dataset;
  private static String datasetProject;
  private static Schema outputSchema;
  private static String tableId;
  private final BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
  private final DataplexBatchSourceConfig config;
  DataplexInterface dataplexInterface = new DataplexInterfaceImpl();
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
    config.validateServiceAccount(collector);
    entityBean = config.getAndValidateEntityConfiguration(collector, dataplexInterface);
    if (entityBean == null) {
      config.setupValidatingInputFormat(pipelineConfigurer, collector, null);
      return;
    }
    if (entityBean.getSystem().equals(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
      getEntityValuesFromDataPathForBQEntities(entityBean.getDataPath());
      config.validateBigQueryDataset(collector, datasetProject, dataset, tableId);
      config.validateTable(collector, datasetProject, dataset, tableId);
      Schema configuredSchema = entityBean.getSchema(collector);
      configurer.setOutputSchema(configuredSchema);
    } else {
      config.checkMetastoreForGCSEntity(dataplexInterface, collector);
      config.setupValidatingInputFormat(pipelineConfigurer, collector, entityBean);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    entityBean = config.getAndValidateEntityConfiguration(collector, dataplexInterface);
    config.validateServiceAccount(collector);
    if (entityBean.getSystem().equals(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
      getEntityValuesFromDataPathForBQEntities(entityBean.getDataPath());
      config.validateBigQueryDataset(collector, datasetProject, dataset, tableId);
      config.validateTable(collector, datasetProject, dataset, tableId);
      prepareRunBigQueryDataset(context);
    } else {
      config.checkMetastoreForGCSEntity(dataplexInterface, collector);
      prepareRunStorageBucket(context);
    }
  }

  private void getEntityValuesFromDataPathForBQEntities(String datapath) {
    String[] entityValues = datapath.split("/");
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
    entityBean = dataplexInterface.getEntity(config.getCredentials(), config.tryGetProject(), config.getLocation(),
      config.getLake(), config.getZone(), config.getEntity());
  }

  @Override
  public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter)
    throws IOException {
    if (entityBean.getSystem().equalsIgnoreCase(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
      StructuredRecord transformed = outputSchema == null ?
        transformer.transform((GenericData.Record) input.getValue()) :
        transformer.transform((GenericData.Record) input.getValue(), outputSchema);
      emitter.emit(transformed);
    } else {
      emitter.emit((StructuredRecord) input.getValue());
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (entityBean.getSystem().equalsIgnoreCase(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
      BigQuerySourceUtils.deleteGcsTemporaryDirectory(configuration, null, bucketPath);
      String temporaryTable = configuration.get(CONFIG_TEMPORARY_TABLE_NAME);
      Credentials credentials = config.getCredentials();
      BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
      bigQuery.delete(TableId.of(datasetProject, dataset, temporaryTable));
      LOG.debug("Deleted temporary table '{}'", temporaryTable);
    } else {
      Storage storage = GCPUtils.getStorage(config.tryGetProject(), config.getCredentials());
      BigQuerySourceUtils.deleteGcsTemporaryDirectory(configuration, GCS_TEMP_BUCKET_NAME, "projects");
      storage.delete(GCS_TEMP_BUCKET_NAME);
      LOG.debug("Deleted temporary bucket '{}'.", GCS_TEMP_BUCKET_NAME);
    }
  }

  private void prepareRunBigQueryDataset(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    outputSchema = entityBean.getSchema(collector);
    // Create BigQuery client
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = config.getCredentials();
    BigQuery bigQuery = GCPUtils.getBigQuery(datasetProject, credentials);

    // Get Configuration for this run
    bucketPath = UUID.randomUUID().toString();
    configuration = BigQueryUtil.getBigQueryConfig(serviceAccount, config.getProject(), null,
      config.getServiceAccountType());

    // Configure GCS Bucket to use
    String bucket = getOrCreateBucket(configuration,
      config.getProject(), bigQuery, credentials, bucketPath);

    // Configure Service account credentials
    configureServiceAccount(configuration, config.getConnection());

    // Configure BQ Source
    configureBigQuerySource();

    // Configure BigQuery input format.
    String temporaryGcsPath = BigQuerySourceUtils.getTemporaryGcsPath(bucket, bucketPath, bucketPath);
    BigQuerySourceUtils.configureBigQueryInput(configuration, DatasetId.of(datasetProject, dataset), tableId,
      temporaryGcsPath);
    configuration.set(DataplexConstants.DATAPLEX_ENTITY_TYPE, entityBean.getSystem());
    TableDefinition.Type sourceTableType = config.getSourceTableType(datasetProject, dataset, tableId, collector);
    emitLineage(context, outputSchema, sourceTableType, tableId);
    context.setInput(
      Input.of(config.getReferenceName(), new DataplexInputFormatProvider(configuration)));
  }

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

  private void prepareRunStorageBucket(BatchSourceContext context)
    throws InstantiationException, IOException, DataplexException {
    FailureCollector collector = context.getFailureCollector();
    Job job = JobUtils.createInstance();
    configuration = job.getConfiguration();
    Storage storage = GCPUtils.getStorage(config.getProject(), config.getCredentials());
    getOrCreateBucket(configuration, storage, config.getLocation(),
      GCS_TEMP_BUCKET_NAME);
    config.checkMetastoreForGCSEntity(dataplexInterface, collector);
    String outputLocation = GCSPath.SCHEME + GCS_TEMP_BUCKET_NAME;
    String query = formatQuery(entityBean, context.isPreviewEnabled());
    String taskId = createTask(config.getLocation(), config.getLake(), outputLocation, query);
    setConfigurationForDataplex(taskId);
    ValidatingInputFormat validatingInputFormat = config.validateRunStorageBucket(context, entityBean);
    FileInputFormat.setInputDirRecursive(job, true);
    Schema schema = entityBean.getSchema(collector);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    if (schema != null && schema.getFields() != null) {
      this.recordLineage(lineageRecorder, schema.getFields().stream().map(Schema.Field::getName).collect(
        Collectors.toList()), "Read from GCS entity in Dataplex.");
    }

    Iterator propertiesIterator = config.getFileSystemProperties(outputLocation).entrySet().iterator();
    while (propertiesIterator.hasNext()) {
      Map.Entry<String, String> entry = (Map.Entry) propertiesIterator.next();
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
      Iterator inputFormatIterator = inputFormatConfiguration.entrySet().iterator();
      while (inputFormatIterator.hasNext()) {
        Map.Entry<String, String> propertyEntry = (Map.Entry) inputFormatIterator.next();
        configuration.set(propertyEntry.getKey(), propertyEntry.getValue());
      }
    }
    configuration.set(DataplexConstants.DATAPLEX_ENTITY_TYPE, entityBean.getSystem());
    context.setInput(Input.of(config.getReferenceName(), new DataplexInputFormatProvider(configuration)));
  }

  private void setConfigurationForDataplex(String taskId) {
    configuration.set(DataplexConstants.DATAPLEX_TASK_ID, taskId);
    configuration.set(DataplexConstants.DATAPLEX_PROJECT_ID, config.tryGetProject());
    configuration.set(DataplexConstants.DATAPLEX_LOCATION, config.getLocation());
    configuration.set(DataplexConstants.DATAPLEX_LAKE, config.getLake());
    configuration.set("cdap.gcs.auth.service.account.type.flag", config.getServiceAccountType());
    String serviceAccountFilePath = config.getServiceAccountFilePath() != null ? config.getServiceAccountFilePath()
      : "none";
    configuration.set("cdap.gcs.auth.service.account.type.filepath", serviceAccountFilePath);
  }

  private void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields, String description) {
    lineageRecorder.recordRead("Read", description,
      outputFields);
  }

  private void emitLineage(BatchSourceContext context, Schema schema, TableDefinition.Type sourceTableType,
                           String table) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
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
          type, table));
    }
  }

  private String createTask(String locationId, String lakeId, String outputLocation, String query)
    throws IOException, DataplexException {
    String taskArgs = "--output_location,%s, --output_format, %s";
    Task task = new Task();
    Task.TriggerSpec triggerSpec = new Task.TriggerSpec();
    triggerSpec.setType("ON_DEMAND");

    Task.ExecutionSpec executionSpec = new Task.ExecutionSpec();
    Task.Args args = new Task.Args();
    executionSpec.setServiceAccount(config.getServiceAccountEmail());
    args.setTaskArgs(String.format(taskArgs, outputLocation, DataplexBatchSourceConfig.INPUT_FORMAT));
    executionSpec.setArgs(args);

    Task.Spark spark = new Task.Spark();
    spark.setSqlScript(query);
    task.setTriggerSpec(triggerSpec);
    task.setExecutionSpec(executionSpec);
    task.setSpark(spark);
    task.setDescription("task-" + UUID.randomUUID());
    dataplexInterface.createTask(config.getCredentials(),
      config.tryGetProject(), locationId, lakeId, task);
    return task.getDescription();
  }

  private String formatQuery(Entity entityBean, boolean isPreviewEnabled) {
    String queryTemplate = "select * from %s.%s %s";
    StringBuilder condition = new StringBuilder();
    if (!Strings.isNullOrEmpty(config.getFilter())) {
      condition.append("where ").append(config.getFilter());
    }
    if (isPreviewEnabled) {
      condition.append(" LIMIT 1000;");
    }
    return String.format(queryTemplate, config.getZone(), entityBean.getId(), condition);
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
   * @param bucketName    bucket Name
   * @return Bucket name.
   */
  private String getOrCreateBucket(Configuration configuration,
                                   String project,
                                   BigQuery bigQuery,
                                   Credentials credentials,
                                   String bucketName) {
    String bucket = String.format(BQ_TEMP_BUCKET_NAME_TEMPLATE, bucketName);
    configuration.setBoolean("fs.gs.bucket.delete.enable", true);

    // the dataset existence is validated before, so this cannot be null
    Dataset bigQueryDataset = bigQuery.getDataset(DatasetId.of(datasetProject, dataset));
    GCPUtils.createBucket(GCPUtils.getStorage(project, credentials),
      bucket, bigQueryDataset.getLocation(), null);
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
  private String getOrCreateBucket(Configuration configuration,
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
        throw new IOException(String.format("Unable to create Cloud Storage bucket '%s' in the same ",
          bucket), e);
      }
    }
    return bucket;
  }
}
