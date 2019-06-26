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
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.JobInfo;
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
import com.google.gson.JsonObject;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * An Output Format which extends {@link ForwardingBigQueryFileOutputFormat}.
 * This is added to override BigQueryUtils.waitForJobCompletion error message with more useful error message.
 * See CDAP-15289 for more information.
 */
public class BigQueryOutputFormat extends ForwardingBigQueryFileOutputFormat<JsonObject, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryOutputFormat.class);

  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    return new BigQueryOutputCommitter(context, delegateCommitter);
  }

  /**
   * BigQuery Output committer.
   */
  public static class BigQueryOutputCommitter extends ForwardingBigQueryFileOutputCommitter {
    private BigQueryHelper bigQueryHelper;

    BigQueryOutputCommitter(TaskAttemptContext context, OutputCommitter delegate) throws IOException {
      super(context, delegate);
      try {
        BigQueryFactory bigQueryFactory = new BigQueryFactory();
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
      String destProjectId = BigQueryOutputConfiguration.getProjectId(conf);
      String writeDisposition = BigQueryOutputConfiguration.getWriteDisposition(conf);
      Optional<TableSchema> destSchema = getTableSchema(conf);
      String kmsKeyName = BigQueryOutputConfiguration.getKmsKeyName(conf);
      BigQueryFileFormat outputFileFormat = BigQueryOutputConfiguration.getFileFormat(conf);
      List<String> sourceUris = getOutputFileURIs();

      boolean allowSchemaRelaxation = conf.getBoolean(BigQueryConstants.CONFIG_ALLOW_SCHEMA_RELAXATION, false);
      LOG.debug("Allow schema relaxation: '{}'", allowSchemaRelaxation);
      boolean createPartitionedTable = conf.getBoolean(BigQueryConstants.CONFIG_CREATE_PARTITIONED_TABLE, false);
      LOG.debug("Create Partitioned Table: '{}'", createPartitionedTable);
      String partitionByField = conf.get(BigQueryConstants.CONFIG_PARTITION_BY_FIELD, null);
      LOG.debug("Partition Field: '{}'", partitionByField);
      boolean requirePartitionFilter = conf.getBoolean(BigQueryConstants.CONFIG_REQUIRE_PARTITION_FILTER, false);
      LOG.debug("Require partition filter: '{}'", requirePartitionFilter);

      try {
        importFromGcs(destProjectId, destTable, destSchema.orElse(null), kmsKeyName, outputFileFormat,
                      writeDisposition, sourceUris, allowSchemaRelaxation, createPartitionedTable, partitionByField,
                      requirePartitionFilter);
      } catch (InterruptedException e) {
        throw new IOException("Failed to import GCS into BigQuery", e);
      }

      cleanup(jobContext);
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
                               List<String> gcsPaths, boolean allowSchemaRelaxation, boolean createPartitionedTable,
                               @Nullable String partitionByField, boolean requirePartitionFilter)
      throws IOException, InterruptedException {
      LOG.info("Importing into table '{}' from {} paths; path[0] is '{}'; awaitCompletion: {}",
               BigQueryStrings.toString(tableRef), gcsPaths.size(), gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0),
               true);

      // Create load conf with minimal requirements.
      JobConfigurationLoad loadConfig = new JobConfigurationLoad();
      loadConfig.setSchema(schema);
      loadConfig.setSourceFormat(sourceFormat.getFormatIdentifier());
      loadConfig.setSourceUris(gcsPaths);
      loadConfig.setDestinationTable(tableRef);
      loadConfig.setWriteDisposition(writeDisposition);
      if (createPartitionedTable) {
        TimePartitioning timePartitioning = new TimePartitioning();
        timePartitioning.setType("DAY");
        if (partitionByField != null) {
          timePartitioning.setField(partitionByField);
        }
        timePartitioning.setRequirePartitionFilter(requirePartitionFilter);
        loadConfig.setTimePartitioning(timePartitioning);
      }

      if (!Strings.isNullOrEmpty(kmsKeyName)) {
        loadConfig.setDestinationEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKeyName));
      }

      if (allowSchemaRelaxation) {
        loadConfig.setSchemaUpdateOptions(Arrays.asList(
          JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION.name(),
          JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION.name()));
      }

      // Auto detect the schema if we're not given one, otherwise use the passed schema.
      if (schema == null) {
        LOG.info("No import schema provided, auto detecting schema.");
        loadConfig.setAutodetect(true);
      } else {
        LOG.info("Using provided import schema '{}'.", schema.toString());
      }

      JobConfiguration config = new JobConfiguration();
      config.setLoad(loadConfig);

      // Get the dataset to determine the location
      Dataset dataset =
        bigQueryHelper.getRawBigquery().datasets().get(tableRef.getProjectId(), tableRef.getDatasetId()).execute();

      JobReference jobReference =
        bigQueryHelper.createJobReference(projectId, "direct-bigqueryhelper-import", dataset.getLocation());
      Job job = new Job();
      job.setConfiguration(config);
      job.setJobReference(jobReference);

      // Insert and run job.
      bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);

      // Poll until job is complete.
      waitForJobCompletion(bigQueryHelper.getRawBigquery(), projectId, jobReference);
    }

    /**
     * This method is copied from BigQueryUtils#waitForJobCompletion for getting useful error message.
     */
    private static void waitForJobCompletion(Bigquery bigquery, String projectId,
                                             JobReference jobReference) throws IOException, InterruptedException {

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
          ResilientOperation.getGoogleRequestCallable(get),
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
      String datasetId =
        ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
      String tableId =
        ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);

      return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
    }

    /**
     * This method is copied from BigQueryOutputConfiguration#getTableSchema. It is slightly modified to return
     * Optional<TableSchema> instead of Optional<BigQueryTableSchema>.
     */
    private static Optional<TableSchema> getTableSchema(Configuration conf) throws IOException {
      String fieldsJson = conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY);
      if (!Strings.isNullOrEmpty(fieldsJson)) {
        try {
          TableSchema tableSchema = createTableSchemaFromFields(fieldsJson);
          return Optional.of(tableSchema);
        } catch (IOException e) {
          throw new IOException(
            "Unable to parse key '" + BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY + "'.", e);
        }
      }
      return Optional.empty();
    }

    private static TableSchema createTableSchemaFromFields(String fieldsJson) throws IOException {
      List<TableFieldSchema> fields = new ArrayList<>();
      JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(fieldsJson);
      parser.parseArrayAndClose(fields, TableFieldSchema.class);
      return new TableSchema().setFields(fields);
    }
  }
}
