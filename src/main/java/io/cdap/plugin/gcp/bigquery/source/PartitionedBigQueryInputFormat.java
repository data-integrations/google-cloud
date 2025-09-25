/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.AbstractBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.AvroRecordReader;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * BigQuery input format, splits query from the configuration into list of queries
 * in order to create input splits.
 */
public class PartitionedBigQueryInputFormat extends AbstractBigQueryInputFormat<LongWritable, GenericData.Record> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedBigQueryInputFormat.class);
  private InputFormat<LongWritable, GenericData.Record> delegateInputFormat =
    new AvroBigQueryInputFormatWithScopes();

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.AVRO;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    processQuery(context);

    return delegateInputFormat.getSplits(context);
  }


  @Override
  public RecordReader<LongWritable, GenericData.Record> createDelegateRecordReader(InputSplit split,
                                                                                   Configuration configuration)
    throws IOException, InterruptedException {
    Preconditions.checkState(
      split instanceof FileSplit, "AvroBigQueryInputFormat requires FileSplit input splits");
    return new AvroRecordReader();
  }

  /**
   * Override to support additonal scopes, useful when exporting from external tables
   *
   * @param config Hadoop config
   * @return BigQuery Helper instance
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on security exception.
   */
  @Override
  protected BigQueryHelper getBigQueryHelper(Configuration config) throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactoryWithScopes(GCPUtils.BIGQUERY_SCOPES);
    return factory.getBigQueryHelper(config);
  }

  private void processQuery(JobContext context) throws IOException, InterruptedException {
    final Configuration configuration = context.getConfiguration();
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        "Failed to create BigQuery client", gse.getMessage(), ErrorType.UNKNOWN, true, gse);
    }

    List<HadoopConfigurationProperty<?>> hadoopConfigurationProperties = new ArrayList<>(
        BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
      configuration, hadoopConfigurationProperties);
    String projectId = mandatoryConfig.get(BigQueryConfiguration.PROJECT_ID.getKey());
    String datasetProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID.getKey());
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID.getKey());
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID.getKey());
    String serviceAccount = configuration.get(BigQueryConstants.CONFIG_SERVICE_ACCOUNT, null);
    boolean isServiceAccountFilePath = configuration.getBoolean(BigQueryConstants.CONFIG_SERVICE_ACCOUNT_IS_FILE,
                                                                true);

    String partitionFromDate = configuration.get(BigQueryConstants.CONFIG_PARTITION_FROM_DATE, null);
    String partitionToDate = configuration.get(BigQueryConstants.CONFIG_PARTITION_TO_DATE, null);
    String filter = configuration.get(BigQueryConstants.CONFIG_FILTER, null);
    String limit = configuration.get(BigQueryConstants.CONFIG_LIMIT, null);
    String orderBy = configuration.get(BigQueryConstants.CONFIG_ORDER_BY, null);
    Integer readTimeout = configuration.getInt(BigQueryConstants.CONFIG_BQ_HTTP_READ_TIMEOUT,
        GCPUtils.BQ_DEFAULT_READ_TIMEOUT_SECONDS);

    com.google.cloud.bigquery.Table bigQueryTable = BigQueryUtil.getBigQueryTable(
        datasetProjectId, datasetId, tableName, serviceAccount, isServiceAccountFilePath, null,
        readTimeout);
    Type type = Objects.requireNonNull(bigQueryTable).getDefinition().getType();
    Boolean isPartitionFilterRequired = bigQueryTable.getRequirePartitionFilter();
    StandardTableDefinition tableDefinition = Objects.requireNonNull(bigQueryTable).getDefinition();

    String query;
    if (type == Type.VIEW || type == Type.MATERIALIZED_VIEW || type == Type.EXTERNAL) {
      query = generateQueryForMaterializingView(datasetProjectId, datasetId, tableName, filter,
          limit, orderBy);
    } else {
      query = generateQuery(partitionFromDate, partitionToDate, filter, datasetProjectId,
          datasetId,
          tableName, limit, orderBy,
          isPartitionFilterRequired, tableDefinition);
    }

    if (query != null) {
      TableReference sourceTable = new TableReference().setDatasetId(datasetId).setProjectId(datasetProjectId)
        .setTableId(tableName);
      String location = bigQueryHelper.getTable(sourceTable).getLocation();
      String temporaryTableName = configuration.get(BigQueryConstants.CONFIG_TEMPORARY_TABLE_NAME);
      TableReference exportTableReference = createExportTableReference(type, datasetProjectId, datasetId,
                                                                       temporaryTableName, configuration);
      runQuery(configuration, bigQueryHelper, projectId, exportTableReference, query, location);

      // Default values come from BigquerySource config, and can be overridden by config.
      configuration.set(BigQueryConfiguration.INPUT_PROJECT_ID.getKey(),
                        configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_PROJECT));
      configuration.set(BigQueryConfiguration.INPUT_DATASET_ID.getKey(),
                        configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_DATASET));
      configuration.set(BigQueryConfiguration.INPUT_TABLE_ID.getKey(), temporaryTableName);
    }
  }

  @VisibleForTesting
  String generateQuery(String partitionFromDate, String partitionToDate, String filter,
      String datasetProject, String dataset, String table, String limit, String orderBy,
      Boolean isPartitionFilterRequired, StandardTableDefinition tableDefinition) {

    RangePartitioning rangePartitioning = tableDefinition.getRangePartitioning();
    TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
    StringBuilder condition = new StringBuilder();
    String partitionCondition = null;

    if (timePartitioning != null) {
      if (partitionFromDate == null && partitionToDate == null
          && Objects.equals(isPartitionFilterRequired, Boolean.TRUE)) {
        partitionCondition = BigQueryUtil.generateDefaultTimePartitionCondition(tableDefinition);
      } else if (partitionFromDate != null || partitionToDate != null) {
        partitionCondition =
            BigQueryUtil.generateTimePartitionCondition(tableDefinition, partitionFromDate,
                partitionToDate);
      }
    } else if (rangePartitioning != null && Objects.equals(isPartitionFilterRequired,
        Boolean.TRUE)) {
      partitionCondition = BigQueryUtil.generateDefaultRangePartitionCondition(
          tableDefinition);
    }

    if (!Strings.isNullOrEmpty(partitionCondition)) {
      condition.append("(").append(partitionCondition).append(")");
    }

    if (!Strings.isNullOrEmpty(filter)) {
      if (condition.length() == 0) {
        condition.append(filter);
      } else {
        condition.append(" and (").append(filter).append(")");
      }
    }

    String tableName = datasetProject + "." + dataset + "." + table;
    StringBuilder query = new StringBuilder("select * from ").append(tableName);

    if (condition.length() > 0) {
      query.append(" where ").append(condition);
    }

    if (!Strings.isNullOrEmpty(orderBy)) {
      query.append(" order by ").append(orderBy);
    }

    if (!Strings.isNullOrEmpty(limit)) {
      query.append(" limit ").append(limit);
    }

    LOG.debug("Generated BigQuery query for job: {}", query);
    return query.toString();
  }

  @VisibleForTesting
  String generateQueryForMaterializingView(String datasetProject, String dataset, String table,
      String filter, String limit, String orderBy) {
    String tableName = String.format("`%s.%s.%s`", datasetProject, dataset, table);
    StringBuilder query = new StringBuilder("select * from ").append(tableName);
    if (!Strings.isNullOrEmpty(filter)) {
      query.append(" where ").append(filter);
    }

    if (!Strings.isNullOrEmpty(orderBy)) {
      query.append(" order by ").append(orderBy);
    }

    if (!Strings.isNullOrEmpty(limit)) {
      query.append(" limit ").append(limit);
    }

    return query.toString();
  }

  /**
   * Create {@link TableReference} to export Table or View
   *
   * @param type           BigQuery table type
   * @param datasetProjectId project id of source table
   * @param datasetId      dataset id of source table
   * @param tableId        The ID of the table
   * @param configuration  Configuration that contains View Materialization ProjectId and View
   *                       Materialization Dataset Id
   * @return {@link TableReference}
   */
  private TableReference createExportTableReference(
    Type type, String datasetProjectId,
    String datasetId,
    String tableId,
    Configuration configuration) {

    TableReference tableReference = new TableReference().setTableId(tableId);

    tableReference.setProjectId(configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_PROJECT));
    tableReference.setDatasetId(configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_DATASET));

    return tableReference;
  }

  private static void runQuery(Configuration configuration,
                               BigQueryHelper bigQueryHelper,
                               String projectId,
                               TableReference tableRef,
                               String query,
                               String location)
    throws IOException, InterruptedException {

    // Create a query statement and query request object.
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    queryConfig.setAllowLargeResults(true);
    queryConfig.setQuery(query);
    queryConfig.setUseLegacySql(false);

    // Set the table to put results into.
    queryConfig.setDestinationTable(tableRef);

    queryConfig.setCreateDisposition("CREATE_IF_NEEDED");

    // Require table to be empty.
    queryConfig.setWriteDisposition("WRITE_EMPTY");

    JobConfiguration config = new JobConfiguration();
    config.setQuery(queryConfig);
    config.setLabels(BigQueryUtil.getJobLabels(BigQueryUtil.BQ_JOB_TYPE_SOURCE_TAG));

    JobReference jobReference = getJobReference(configuration, bigQueryHelper, projectId, location);

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    // Poll until job is complete.
    BigQueryUtils.waitForJobCompletion(
      bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
    if (bigQueryHelper.tableExists(tableRef)) {
      long expirationMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
      Table table = bigQueryHelper.getTable(tableRef).setExpirationTime(expirationMillis);
      bigQueryHelper.getRawBigquery().tables().update(tableRef.getProjectId(), tableRef.getDatasetId(),
                                                      tableRef.getTableId(), table).execute();
    }
  }

  /**
   * Gets the Job Reference for the BQ job to execute.
   *
   * If a Job ID is pre-configured, use this value.
   *
   * Otherwise, a new Job ID is generated with the "querybasedexport" prefix.
   *
   * @param conf Hadoop configuration.
   * @param bigQueryHelper Big Query Helper instance
   * @param projectId Project ID
   * @param location Location
   * @return Job Reference for the job to execute.
   */
  private static JobReference getJobReference(Configuration conf, BigQueryHelper bigQueryHelper,
                                              String projectId, @Nullable String location) {
    String savedJobId = conf.get(BigQueryConstants.CONFIG_JOB_ID);
    if (savedJobId == null || savedJobId.isEmpty()) {
      return bigQueryHelper.createJobReference(projectId, "querybasedexport", location);
    }
    return new JobReference().setProjectId(projectId).setJobId(savedJobId).setLocation(location);
  }
}
