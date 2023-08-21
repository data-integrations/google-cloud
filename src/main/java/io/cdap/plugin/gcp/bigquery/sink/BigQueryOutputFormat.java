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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.RangePartitioning;
import com.google.api.services.bigquery.model.RangePartitioning.Range;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.ForwardingBigQueryFileOutputCommitter;
import com.google.cloud.hadoop.io.bigquery.output.ForwardingBigQueryFileOutputFormat;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.gcp.bigquery.source.BigQueryFactoryWithScopes;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
/**
 * An Output Format which extends {@link ForwardingBigQueryFileOutputFormat}.
 * This is added to override BigQueryUtils.waitForJobCompletion error message with more useful error message.
 * See CDAP-15289 for more information.
 */
public class BigQueryOutputFormat extends ForwardingBigQueryFileOutputFormat<StructuredRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryOutputFormat.class);

  @Override
  public RecordWriter<StructuredRecord, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    return getRecordWriter(taskAttemptContext,
                           getOutputSchema(configuration));
  }

  /**
   * Get a Record Writer instance which uses a supplied schema to write output records.
   *
   * @param taskAttemptContext the execution context
   * @param schema             output schema
   * @return Record Writer Instance
   */
  public RecordWriter<StructuredRecord, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext,
                                                                      io.cdap.cdap.api.data.schema.Schema schema)
    throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    return new BigQueryRecordWriter(getDelegate(configuration).getRecordWriter(taskAttemptContext),
                                    BigQueryOutputConfiguration.getFileFormat(configuration),
                                    schema);
  }

  private io.cdap.cdap.api.data.schema.Schema getOutputSchema(Configuration configuration) throws IOException {
    String schemaJson = configuration.get(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA);
    if (schemaJson == null) {
      return null;
    }
    return io.cdap.cdap.api.data.schema.Schema.parseJson(schemaJson);
  }

  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    return new BigQueryOutputCommitter(context, delegateCommitter);
  }

  /**
   * This method is copied from
   * {@link ForwardingBigQueryFileOutputFormat#checkOutputSpecs(JobContext)} to override
   * {@link BigQueryFactory} with {@link BigQueryFactoryWithScopes}.
   */
  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    Configuration conf = job.getConfiguration();

    // Validate the output configuration.
    BigQueryOutputConfiguration.validateConfiguration(conf);

    // Get the output path.
    Path outputPath = BigQueryOutputConfiguration.getGcsOutputPath(conf);
    LOG.info("Using output path '%s'.", outputPath);

    // Error if the output path already exists.
    FileSystem outputFileSystem = outputPath.getFileSystem(conf);
    if (outputFileSystem.exists(outputPath)) {
      throw new IOException("The output path '" + outputPath + "' already exists.");
    }

    // Error if compression is set as there's mixed support in BigQuery.
    if (FileOutputFormat.getCompressOutput(job)) {
      throw new IOException("Compression isn't supported for this OutputFormat.");
    }

    // Error if unable to create a BigQuery helper.
    try {
      new BigQueryFactoryWithScopes(GCPUtils.BIGQUERY_SCOPES).getBigQueryHelper(conf);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    // Let delegate process its checks.
    getDelegate(conf).checkOutputSpecs(job);
  }

  /**
   * BigQuery Output committer.
   */
  public static class BigQueryOutputCommitter extends ForwardingBigQueryFileOutputCommitter {
    private BigQueryHelper bigQueryHelper;

    private Operation operation;
    private TableReference temporaryTableReference;
    private List<String> tableKeyList;
    private List<String> orderedByList;
    private List<String> tableFieldsList;
    private String partitionFilter;

    private boolean allowSchemaRelaxation;
    private boolean allowSchemaRelaxationOnEmptyOutput;

    private static final int BQ_IMPORT_MAX_BATCH_SIZE = 10000;

    BigQueryOutputCommitter(TaskAttemptContext context, OutputCommitter delegate) throws IOException {
      super(context, delegate);
      try {
        BigQueryFactory bigQueryFactory = new BigQueryFactoryWithScopes(GCPUtils.BIGQUERY_SCOPES);
        this.bigQueryHelper = bigQueryFactory.getBigQueryHelper(context.getConfiguration());
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to create Bigquery client.", e);
      }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      // CDAP-15289 - add specific error message in case of exception. This method is copied from
      // IndirectBigQueryOutputCommitter#commitJob.
      super.commitJob(jobContext);

      // Get the destination configuration information.
      Configuration conf = jobContext.getConfiguration();
      TableReference destTable = getTableReference(conf);
      String destProjectId = BigQueryOutputConfiguration.getJobProjectId(conf);
      String writeDisposition = BigQueryOutputConfiguration.getWriteDisposition(conf);
      Optional<TableSchema> destSchema = getTableSchema(conf);
      String kmsKeyName = BigQueryOutputConfiguration.getKmsKeyName(conf);
      BigQueryFileFormat outputFileFormat = BigQueryOutputConfiguration.getFileFormat(conf);
      List<String> sourceUris = getOutputFileURIs();

      allowSchemaRelaxation = conf.getBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION, false);
      allowSchemaRelaxationOnEmptyOutput =
        conf.getBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION_ON_EMPTY_OUTPUT, false);
      LOG.debug("Allow schema relaxation: '{}'", allowSchemaRelaxation);
      PartitionType partitionType = conf.getEnum(BigQueryConstants.CONFIG_PARTITION_TYPE, PartitionType.NONE);
      LOG.debug("Create Partitioned Table type: '{}'", partitionType);
      com.google.cloud.bigquery.TimePartitioning.Type timePartitioningType = conf.getEnum(
              BigQueryConstants.CONFIG_TIME_PARTITIONING_TYPE, com.google.cloud.bigquery.TimePartitioning.Type.DAY
      );
      Range range = partitionType == PartitionType.INTEGER ? createRangeForIntegerPartitioning(conf) : null;
      String partitionByField = conf.get(BigQueryConstants.CONFIG_PARTITION_BY_FIELD, null);
      LOG.debug("Partition Field: '{}'", partitionByField);
      boolean requirePartitionFilter = conf.getBoolean(BigQueryConstants.CONFIG_REQUIRE_PARTITION_FILTER, false);
      LOG.debug("Require partition filter: '{}'", requirePartitionFilter);
      operation = Operation.valueOf(conf.get(BigQueryConstants.CONFIG_OPERATION));
      String clusteringOrder = conf.get(BigQueryConstants.CONFIG_CLUSTERING_ORDER, null);
      List<String> clusteringOrderList = Arrays.stream(
        clusteringOrder != null ? clusteringOrder.split(",") : new String[0]).map(String::trim)
        .collect(Collectors.toList());
      String tableKey = conf.get(BigQueryConstants.CONFIG_TABLE_KEY, null);
      tableKeyList = Arrays.stream(tableKey != null ? tableKey.split(",") : new String[0]).map(String::trim)
        .collect(Collectors.toList());
      String dedupedBy = conf.get(BigQueryConstants.CONFIG_DEDUPE_BY, null);
      orderedByList = Arrays.stream(dedupedBy != null ? dedupedBy.split(",") : new String[0])
        .collect(Collectors.toList());
      String tableFields = conf.get(BigQueryConstants.CONFIG_TABLE_FIELDS, null);
      tableFieldsList = Arrays.stream(tableFields != null ? tableFields.split(",") : new String[0])
        .map(String::trim).collect(Collectors.toList());
      partitionFilter = conf.get(BigQueryConstants.CONFIG_PARTITION_FILTER, null);
      LOG.debug("Partition filter: '{}'", partitionFilter);
      boolean tableExists = conf.getBoolean(BigQueryConstants.CONFIG_DESTINATION_TABLE_EXISTS, false);

      try {
        importFromGcs(destProjectId, destTable, destSchema.orElse(null), kmsKeyName, outputFileFormat,
                      writeDisposition, sourceUris, partitionType, timePartitioningType, range, partitionByField,
                      requirePartitionFilter, clusteringOrderList, tableExists, conf);
      } catch (Exception e) {
        throw new IOException("Failed to import GCS into BigQuery. ", e);
      }

      cleanup(jobContext);
    }

    private String getJobIdForImportGCS(Configuration conf) {
      //If the operation is not INSERT then this is a write to a temporary table. No need to use saved JobId here.
      // Return a random UUID
      if (!Operation.INSERT.equals(operation)) {
        return UUID.randomUUID().toString();
      }
      //See if plugin specified a Job ID to be used.
      String savedJobId = conf.get(BigQueryConstants.CONFIG_JOB_ID);
      if (savedJobId == null || savedJobId.isEmpty()) {
        return UUID.randomUUID().toString();
      }
      return savedJobId;
    }

    private JobId getJobIdForUpdateUpsert(Configuration conf) {
      //See if plugin specified a Job ID to be used.
      String savedJobId = conf.get(BigQueryConstants.CONFIG_JOB_ID);
      if (savedJobId == null || savedJobId.isEmpty()) {
        return JobId.of(UUID.randomUUID().toString());
      }
      return JobId.of(savedJobId);
    }

    @Override
    public void abortJob(JobContext context, JobStatus.State state) throws IOException {
      //This method is copied from IndirectBigQueryOutputCommitter#abortJob.
      super.abortJob(context, state);
      cleanup(context);
    }

    /**
     * This method is copied from BigQueryHelper#importFromGcs.
     */
    private void importFromGcs(String projectId, TableReference tableRef, @Nullable TableSchema schema,
                               @Nullable String kmsKeyName, BigQueryFileFormat sourceFormat, String writeDisposition,
                               List<String> gcsPaths, PartitionType partitionType,
                               com.google.cloud.bigquery.TimePartitioning.Type timePartitioningType,
                               @Nullable Range range, @Nullable String partitionByField, boolean requirePartitionFilter,
                               List<String> clusteringOrderList, boolean tableExists, Configuration conf)
      throws IOException, InterruptedException {
      LOG.info("Importing into table '{}' from {} paths; path[0] is '{}'; awaitCompletion: {}",
               BigQueryStrings.toString(tableRef), gcsPaths.size(), gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0),
               true);

      String jobId = getJobIdForImportGCS(conf);
      if (gcsPaths.isEmpty()) {
        if (!bigQueryHelper.tableExists(tableRef)) {
          // If gcsPaths empty and destination table not exist - creating empty destination table.
          Table table = new Table();
          table.setSchema(schema);
          table.setTableReference(tableRef);
          bigQueryHelper.getRawBigquery().tables().insert(tableRef.getProjectId(), tableRef.getDatasetId(), table)
            .execute();
        } else if (allowSchemaRelaxationOnEmptyOutput) {
          // If the table requires a schema update, apply if even when there are no records to write.
          Table table = bigQueryHelper.getTable(tableRef);
          table.setSchema(schema);
          bigQueryHelper.getRawBigquery().tables()
            .update(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId(), table)
            .execute();
        }
        return;
      }
      // Create load conf with minimal requirements.
      JobConfigurationLoad loadConfig = new JobConfigurationLoad();
      loadConfig.setSourceFormat(sourceFormat.getFormatIdentifier());
      loadConfig.setUseAvroLogicalTypes(true);

      // If schema change is not allowed and if the destination table already exists, use the destination table schema
      // See PLUGIN-395
      if (!allowSchemaRelaxation && tableExists) {
        loadConfig.setSchema(bigQueryHelper.getTable(tableRef).getSchema());
      } else {
        loadConfig.setSchema(schema);
      }

      Map<String, String> fieldDescriptions = new HashMap<>();
      if (JobInfo.WriteDisposition.WRITE_TRUNCATE
        .equals(JobInfo.WriteDisposition.valueOf(writeDisposition)) && tableExists) {
          List<TableFieldSchema> tableFieldSchemas = Optional.ofNullable(bigQueryHelper.getTable(tableRef))
            .map(it -> it.getSchema())
            .map(it -> it.getFields())
            .orElse(Collections.emptyList());

          tableFieldSchemas
            .forEach(it -> {
              if (!Strings.isNullOrEmpty(it.getDescription())) {
                fieldDescriptions.put(it.getName(), it.getDescription());
              }
            });
      }

      if (!tableExists) {
        switch (partitionType) {
          case TIME:
            TimePartitioning timePartitioning = createTimePartitioning(partitionByField, requirePartitionFilter,
                    timePartitioningType);
            loadConfig.setTimePartitioning(timePartitioning);
            break;
          case INTEGER:
            RangePartitioning rangePartitioning = createRangePartitioning(partitionByField, range);
            if (requirePartitionFilter) {
              createTableWithRangePartitionAndRequirePartitionFilter(tableRef, schema, rangePartitioning);
            } else {
              loadConfig.setRangePartitioning(rangePartitioning);
            }
            break;
          case NONE:
            break;
        }
        if (PartitionType.NONE != partitionType && !clusteringOrderList.isEmpty()) {
          Clustering clustering = new Clustering();
          clustering.setFields(clusteringOrderList);
          loadConfig.setClustering(clustering);
        }
      }

      if (!tableExists && Operation.UPSERT.equals(operation)) {
        // If the table does not exist then an UPSERT operation becomes an INSERT operation
        operation = Operation.INSERT;
      }

      if (Operation.INSERT.equals(operation)) {
        // Schema update options should only be specified with WRITE_APPEND disposition,
        // or with WRITE_TRUNCATE disposition on a table partition - The logic below should change when we support
        // insertion into single partition
        if (allowSchemaRelaxation && !JobInfo.WriteDisposition.WRITE_TRUNCATE
          .equals(JobInfo.WriteDisposition.valueOf(writeDisposition))) {
          loadConfig.setSchemaUpdateOptions(Arrays.asList(
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION.name(),
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION.name()));
        }
      }

      if (!Strings.isNullOrEmpty(kmsKeyName)) {
        loadConfig.setDestinationEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKeyName));
      }

      // Auto detect the schema if we're not given one, otherwise use the set schema.
      if (loadConfig.getSchema() == null) {
        LOG.info("No import schema provided, auto detecting schema.");
        loadConfig.setAutodetect(true);
      } else {
        LOG.info("Using schema '{}' for the load job config.", loadConfig.getSchema());
      }

      // Get the dataset to determine the location
      Dataset dataset =
        bigQueryHelper.getRawBigquery().datasets().get(tableRef.getProjectId(), tableRef.getDatasetId()).execute();

      //Depending on Operation type and no of gcs paths present , trigger suitable BQ job.
      temporaryTableReference = null;
      if (operation.equals(Operation.INSERT) && gcsPaths.size() <= BQ_IMPORT_MAX_BATCH_SIZE) {
        // Directly load data into destination table when total no of input paths is loadable into BQ
        loadConfig.setSourceUris(gcsPaths);
        loadConfig.setWriteDisposition(writeDisposition);
        loadConfig.setDestinationTable(tableRef);

        JobConfiguration config = new JobConfiguration();
        config.setLoad(loadConfig);
        config.setLabels(BigQueryUtil.getJobTags(BigQueryUtil.BQ_JOB_TYPE_SINK_TAG));
        triggerBigqueryJob(projectId, jobId , dataset, config, tableRef);
      } else {
        // First load the data in a temp table.
        loadInBatchesInTempTable(tableRef, loadConfig, gcsPaths, projectId, jobId, dataset);

        if (operation.equals(Operation.INSERT)) { // For the case when gcs paths is more than 10000
          handleInsertOperation(tableRef, writeDisposition, loadConfig.getDestinationEncryptionConfiguration(),
                                projectId, jobId, dataset, tableExists);
        } else {
          handleUpdateUpsertOperation(tableRef, tableExists, kmsKeyName, getJobIdForUpdateUpsert(conf),
                                      projectId, dataset);
        }
      }

      setTemporaryTableExpiration();
      updateFieldDescriptions(writeDisposition, tableRef, fieldDescriptions);

      LOG.info("Imported into table '{}' from {} paths; path[0] is '{}'",
               BigQueryStrings.toString(tableRef), gcsPaths.size(), gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0));
    }

    private void triggerBigqueryJob(String projectId, String jobId, Dataset dataset, JobConfiguration jobConfiguration,
                                    TableReference tableRef) throws IOException, InterruptedException {

      JobReference jobReference =
        new JobReference().setProjectId(projectId)
          .setJobId(jobId)
          .setLocation(dataset.getLocation());

      Job job = new Job();
      job.setConfiguration(jobConfiguration);
      job.setJobReference(jobReference);
      // Insert and run job.
      bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
      // Poll until job is complete.
      waitForJobCompletion(bigQueryHelper, projectId, jobReference, tableRef, operation);
    }

    private void loadInBatchesInTempTable(TableReference tableRef, JobConfigurationLoad loadConfig,
                                          List<String> gcsPaths, String projectId, String jobId, Dataset dataset)
      throws IOException, InterruptedException {

      LOG.info(" Importing into a temporary table first in batches of 10000");

      String temporaryTableName = tableRef.getTableId() + "_"
        + UUID.randomUUID().toString().replaceAll("-", "_");
      temporaryTableReference = new TableReference()
        .setDatasetId(tableRef.getDatasetId())
        .setProjectId(tableRef.getProjectId())
        .setTableId(temporaryTableName);

      loadConfig.setDestinationTable(temporaryTableReference);
      loadConfig.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND.toString());

      // Split the list of files in batches 10000 (current bq load job limit) and import /append onto a temp table
      List<List<String>> gcsPathsInBatches = Lists.partition(gcsPaths, BQ_IMPORT_MAX_BATCH_SIZE);

      int jobcount = 1;
      for (List<String> gcsPathBatch :  gcsPathsInBatches) {
        LOG.debug(" Running for Batch {} with number of gcs paths : {}", jobcount, gcsPathBatch.size());
        loadConfig.setSourceUris(gcsPathBatch);
        JobConfiguration config = new JobConfiguration();
        config.setLoad(loadConfig);
        config.setLabels(BigQueryUtil.getJobTags(BigQueryUtil.BQ_JOB_TYPE_SINK_TAG));

        triggerBigqueryJob(projectId, jobId + "_" + jobcount, dataset, config, tableRef);
        jobcount++;
      }
    }

    private void setTemporaryTableExpiration() throws IOException {
      if (temporaryTableReference != null && bigQueryHelper.tableExists(temporaryTableReference)) {
        long expirationMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
        Table table = bigQueryHelper.getTable(temporaryTableReference).setExpirationTime(expirationMillis);
        bigQueryHelper.getRawBigquery().tables().update(temporaryTableReference.getProjectId(),
                                                        temporaryTableReference.getDatasetId(),
                                                        temporaryTableReference.getTableId(), table).execute();
      }
    }

    /**
     * This method is copied from BigQueryUtils#waitForJobCompletion for getting useful error message.
     */
    private static void waitForJobCompletion(BigQueryHelper bigQueryHelper, String projectId,
                                             JobReference jobReference, TableReference tableRef,
                                             @Nullable Operation operation) throws IOException, InterruptedException {

      Bigquery bigquery = bigQueryHelper.getRawBigquery();
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff pollBackOff =
        new ExponentialBackOff.Builder()
          .setMaxIntervalMillis(BigQueryUtils.POLL_WAIT_INTERVAL_MAX_MILLIS)
          .setInitialIntervalMillis(BigQueryUtils.POLL_WAIT_INITIAL_MILLIS)
          .setMaxElapsedTimeMillis(BigQueryUtils.POLL_WAIT_MAX_ELAPSED_MILLIS)
          .build();

      // Get starting time.
      long startTime = System.currentTimeMillis();
      long elapsedTime;
      boolean notDone = true;

      // While job is incomplete continue to poll.
      while (notDone) {
        BackOff operationBackOff = new ExponentialBackOff.Builder().build();
        Bigquery.Jobs.Get get = bigquery.jobs().get(projectId, jobReference.getJobId())
          .setLocation(jobReference.getLocation());

        Job pollJob = ResilientOperation.retry(
          get::execute,
          operationBackOff,
          RetryDeterminer.RATE_LIMIT_ERRORS,
          IOException.class,
          sleeper);

        elapsedTime = System.currentTimeMillis() - startTime;
        LOG.debug("Job status ({} ms) {}: {}", elapsedTime, jobReference.getJobId(),
                  pollJob.getStatus().getState());
        if (pollJob.getStatus().getState().equals("DONE")) {
          notDone = false;
          if (pollJob.getStatus().getErrorResult() != null) {
            if (Operation.UPDATE.equals(operation) && !bigQueryHelper.tableExists(tableRef)) {
              // ignore the failure. This is because we do not want to fail the pipeline as per below discussion
              // https://github.com/data-integrations/google-cloud/pull/290#discussion_r472405882
              LOG.warn("BigQuery Table {} does not exist. The operation update will not write any records to the table."
                , String.format("%s.%s.%s", tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()));
              return;
            }
            List<ErrorProto> errors = pollJob.getStatus().getErrors();
            int numOfErrors;
            String errorMessage;
            if (errors == null || errors.isEmpty()) {
              errorMessage = pollJob.getStatus().getErrorResult().getMessage();
              numOfErrors = 1;
            } else {
              errorMessage = errors.get(errors.size() - 1).getMessage();
              numOfErrors = errors.size();
            }
            // Only add first error message in the exception. For other errors user should look at BigQuery job logs.
            throw new IOException(String.format("Error occurred while importing data to BigQuery '%s'." +
                                                  " There are total %s error(s) for BigQuery job %s. Please look at " +
                                                  "BigQuery job logs for more information.",
                                                errorMessage, numOfErrors, jobReference.getJobId()));
          }
        } else {
          long millisToWait = pollBackOff.nextBackOffMillis();
          if (millisToWait == BackOff.STOP) {
            throw new IOException(
              String.format(
                "Job %s failed to complete after %s millis.",
                jobReference.getJobId(),
                elapsedTime));
          }
          // Pause execution for the configured duration before polling job status again.
          Thread.sleep(millisToWait);
          // Call progress to ensure task doesn't time out.
          Progressable progressable = () -> { };
          progressable.progress();
        }
      }
    }

    /**
     * This method is copied from BigQueryOutputConfiguration#getTableReference.
     */
    private static TableReference getTableReference(Configuration conf) throws IOException {
      // Ensure the BigQuery output information is valid.
      String projectId = BigQueryOutputConfiguration.getProjectId(conf);
      String datasetId = ConfigurationUtil.getMandatoryConfig(conf,
          BigQueryConfiguration.OUTPUT_DATASET_ID);
      String tableId = ConfigurationUtil.getMandatoryConfig(conf,
          BigQueryConfiguration.OUTPUT_TABLE_ID);

      return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
    }

    /**
     * This method is copied from BigQueryOutputConfiguration#getTableSchema. It is slightly modified to return
     * Optional<TableSchema> instead of Optional<BigQueryTableSchema>.
     */
    private static Optional<TableSchema> getTableSchema(Configuration conf) throws IOException {
      String fieldsJson = conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA.getKey());
      if (!Strings.isNullOrEmpty(fieldsJson)) {
        try {
          TableSchema tableSchema = createTableSchemaFromFields(fieldsJson);
          return Optional.of(tableSchema);
        } catch (IOException e) {
          throw new IOException(
            "Unable to parse key '" + BigQueryConfiguration.OUTPUT_TABLE_SCHEMA.getKey() + "'.", e);
        }
      }
      return Optional.empty();
    }

    private void handleInsertOperation(TableReference tableRef, String writeDisposition,
                                       EncryptionConfiguration encryptionConfiguration, String projectId, String jobId,
                                       Dataset dataset, boolean tableExists) throws IOException, InterruptedException {
      if (allowSchemaRelaxation && tableExists) {
        updateTableSchema(tableRef);
      }
      JobConfigurationTableCopy tableCopyConfig = new JobConfigurationTableCopy();
      tableCopyConfig.setDestinationTable(tableRef);
      tableCopyConfig.setSourceTable(temporaryTableReference);
      tableCopyConfig.setWriteDisposition(writeDisposition);
      tableCopyConfig.setDestinationEncryptionConfiguration(encryptionConfiguration);

      JobConfiguration config = new JobConfiguration();
      config.setCopy(tableCopyConfig);
      config.setLabels(BigQueryUtil.getJobTags(BigQueryUtil.BQ_JOB_TYPE_SINK_TAG));
      triggerBigqueryJob(projectId, jobId, dataset, config, tableRef);
    }

    private void handleUpdateUpsertOperation(TableReference tableRef,
                                             boolean tableExists,
                                             @Nullable String cmekKey,
                                             JobId jobId,
                                             String projectId,
                                             Dataset dataset) throws IOException, InterruptedException {
      if (allowSchemaRelaxation && tableExists) {
        updateTableSchema(tableRef);
      }
      TableId sourceTableId = TableId.of(temporaryTableReference.getProjectId(),
                                         temporaryTableReference.getDatasetId(),
                                         temporaryTableReference.getTableId());
      TableId destinationTableId = TableId.of(tableRef.getProjectId(),
                                              tableRef.getDatasetId(),
                                              tableRef.getTableId());
      String query = BigQuerySinkUtils.generateUpdateUpsertQuery(operation,
                                                                 sourceTableId,
                                                                 destinationTableId,
                                                                 tableFieldsList,
                                                                 tableKeyList,
                                                                 orderedByList,
                                                                 partitionFilter);
      LOG.info("Update/Upsert query: " + query);

      // Configure query execution
      JobConfigurationQuery jobConfigurationQuery = new JobConfigurationQuery();
      jobConfigurationQuery.setQuery(query);
      jobConfigurationQuery.setUseLegacySql(false);
      EncryptionConfiguration encryptionConfiguration = new EncryptionConfiguration();
      encryptionConfiguration.setKmsKeyName(cmekKey);
      jobConfigurationQuery.setDestinationEncryptionConfiguration(encryptionConfiguration);

      // Create Job Configuration and add job labels
      JobConfiguration jobConfiguration = new JobConfiguration();
      jobConfiguration.setLabels(BigQueryUtil.getJobTags(BigQueryUtil.BQ_JOB_TYPE_SINK_TAG));
      jobConfiguration.setQuery(jobConfigurationQuery);

      // Trigger job execution
      triggerBigqueryJob(projectId, jobId.getJob(), dataset, jobConfiguration, tableRef);
    }

    private void updateTableSchema(TableReference tableRef) {
      LOG.debug("Update/Upsert table schema update");
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      TableId sourceTableId = TableId.of(temporaryTableReference.getDatasetId(), temporaryTableReference.getTableId());
      TableId destinationTableId = TableId.of(tableRef.getDatasetId(), tableRef.getTableId());

      com.google.cloud.bigquery.Table sourceTable = bigquery.getTable(sourceTableId);
      com.google.cloud.bigquery.Table destinationTable = bigquery.getTable(destinationTableId);

      if (destinationTable == null) {
        LOG.warn("Unable to update schema for table {}.{}.{} , table does not exist.",
                 destinationTableId.getProject(),
                 destinationTableId.getDataset(),
                 destinationTableId.getTable());
        return;
      }

      FieldList sourceFields = sourceTable.getDefinition().getSchema().getFields();
      tableFieldsList = sourceFields.stream().map(Field::getName).collect(Collectors.toList());

      BigQuerySinkUtils.relaxTableSchema(bigquery, sourceTable, destinationTable);
    }

    private static TableSchema createTableSchemaFromFields(String fieldsJson) throws IOException {
      List<TableFieldSchema> fields = new ArrayList<>();
      JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(fieldsJson);
      parser.parseArrayAndClose(fields, TableFieldSchema.class);
      return new TableSchema().setFields(fields);
    }

    private void updateFieldDescriptions(String writeDisposition, TableReference tableRef,
                                         Map<String, String> fieldDescriptions) throws IOException {
      if (JobInfo.WriteDisposition.WRITE_TRUNCATE
        .equals(JobInfo.WriteDisposition.valueOf(writeDisposition))) {

        Table table = bigQueryHelper.getTable(tableRef);
        List<TableFieldSchema> tableFieldSchemas = Optional.ofNullable(table)
          .map(Table::getSchema)
          .map(TableSchema::getFields)
          .orElse(Collections.emptyList());

        tableFieldSchemas
          .forEach(it -> {
            Optional.ofNullable(fieldDescriptions.get(it.getName()))
              .ifPresent(it::setDescription);
          });

        bigQueryHelper
          .getRawBigquery()
          .tables()
          .update(tableRef.getProjectId(),
                  tableRef.getDatasetId(),
                  tableRef.getTableId(), table)
          .execute();
      }
    }

    @Override
    protected void cleanup(JobContext context) throws IOException {
      super.cleanup(context);
      if (temporaryTableReference != null && bigQueryHelper.tableExists(temporaryTableReference)) {
        bigQueryHelper.getRawBigquery().tables()
          .delete(temporaryTableReference.getProjectId(),
                  temporaryTableReference.getDatasetId(),
                  temporaryTableReference.getTableId())
          .execute();
      }
    }

    private Range createRangeForIntegerPartitioning(Configuration conf) {
      long rangeStart = conf.getLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_START, 0);
      long rangeEnd = conf.getLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_END, 0);
      long rangeInterval = conf.getLong(BigQueryConstants.CONFIG_PARTITION_INTEGER_RANGE_INTERVAL, 0);
      Range range = new Range();
      range.setStart(rangeStart);
      range.setEnd(rangeEnd);
      range.setInterval(rangeInterval);
      return range;
    }

    private TimePartitioning createTimePartitioning(
            @Nullable String partitionByField, boolean requirePartitionFilter,
      com.google.cloud.bigquery.TimePartitioning.Type timePartitioningType) {
      TimePartitioning timePartitioning = new TimePartitioning();
      timePartitioning.setType(timePartitioningType.name());
      if (partitionByField != null) {
        timePartitioning.setField(partitionByField);
      }
      timePartitioning.setRequirePartitionFilter(requirePartitionFilter);
      return timePartitioning;
    }

    private void createTableWithRangePartitionAndRequirePartitionFilter(TableReference tableRef,
                                                                        @Nullable TableSchema schema,
                                                                        RangePartitioning rangePartitioning)
      throws IOException {
      Table table = new Table();
      table.setSchema(schema);
      table.setTableReference(tableRef);
      table.setRequirePartitionFilter(true);
      table.setRangePartitioning(rangePartitioning);
      bigQueryHelper.getRawBigquery().tables()
        .insert(tableRef.getProjectId(), tableRef.getDatasetId(), table)
        .execute();
    }

    private RangePartitioning createRangePartitioning(@Nullable String partitionByField, @Nullable Range range) {
      RangePartitioning rangePartitioning = new RangePartitioning();
      rangePartitioning.setRange(range);
      if (partitionByField != null) {
        rangePartitioning.setField(partitionByField);
      }
      return rangePartitioning;
    }
  }

  private static BigQuery getBigQuery(Configuration config) throws IOException {
    String projectId = ConfigurationUtil.getMandatoryConfig(config,
        BigQueryConfiguration.PROJECT_ID);
    Credentials credentials = GCPUtils.loadCredentialsFromConf(config);
    return GCPUtils.getBigQuery(projectId, credentials);
  }
}
