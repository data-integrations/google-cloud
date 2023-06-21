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

package io.cdap.plugin.gcp.bigquery.action;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class <code>BigQueryExecute</code> executes a single Cloud BigQuery SQL.
 * <p>
 * The plugin provides the ability different options like choosing interactive or batch execution of sql query, setting
 * of resulting dataset and table, enabling/disabling cache, specifying whether the query being executed is legacy or
 * standard and retry strategy.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryExecute.NAME)
@Description("Execute a Google BigQuery SQL.")
public final class BigQueryExecute extends AbstractBigQueryAction {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecute.class);
  public static final String NAME = "BigQueryExecute";
  private static final String RECORDS_PROCESSED = "records.processed";

  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());
    QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(config.getSql());
    // Run at batch priority, which won't count toward concurrent rate limit.
    if (config.getMode().equals(QueryJobConfiguration.Priority.BATCH)) {
      builder.setPriority(QueryJobConfiguration.Priority.BATCH);
    } else {
      builder.setPriority(QueryJobConfiguration.Priority.INTERACTIVE);
    }

    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(), collector);
    collector.getOrThrowException();

    // Save the results of the query to a permanent table.
    String datasetName = config.getDataset();
    String tableName = config.getTable();
    String datasetProjectId = config.getDatasetProject();
    if (config.getStoreResults() && datasetProjectId != null && datasetName != null && tableName != null) {
      builder.setDestinationTable(TableId.of(datasetProjectId, datasetName, tableName));
    }

    // Enable or Disable the query cache to force live query evaluation.
    if (config.shouldUseCache()) {
      builder.setUseQueryCache(true);
    }

    // Enable legacy SQL
    builder.setUseLegacySql(config.isLegacySQL());

    // Location must match that of the dataset(s) referenced in the query.
    JobId jobId = JobId.newBuilder().setRandomJob().setLocation(config.getLocation()).build();

    // API request - starts the query.
    Credentials credentials = config.getServiceAccount() == null ?
      null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(),
                                                    config.isServiceAccountFilePath());
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
    //create dataset to store the results if not exists
    if (config.getStoreResults() && !Strings.isNullOrEmpty(datasetName) &&
      !Strings.isNullOrEmpty(tableName)) {
      BigQuerySinkUtils.createDatasetIfNotExists(bigQuery, DatasetId.of(datasetProjectId, datasetName),
                                                 config.getLocation(), cmekKeyName,
                                                 () -> String.format("Unable to create BigQuery dataset '%s.%s'",
                                                                     datasetProjectId, datasetName));
      if (cmekKeyName != null) {
        builder.setDestinationEncryptionConfiguration(
          EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKeyName.toString()).build());
      }
    }

    // Add labels for the BigQuery Execute job.
    builder.setLabels(BigQueryUtil.getJobTags(BigQueryUtil.BQ_JOB_TYPE_EXECUTE_TAG));

    QueryJobConfiguration queryConfig = builder.build();

    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    LOG.info("Executing SQL as job {}.", jobId.getJob());
    LOG.debug("The BigQuery SQL is {}", config.getSql());

    // Wait for the query to complete
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getExecutionErrors().toString());
    }

    TableResult queryResults = queryJob.getQueryResults();
    long rows = queryResults.getTotalRows();

    if (config.shouldSetAsArguments()) {
      if (rows == 0 || queryResults.getSchema() == null) {
        LOG.warn("The query result does not contain any row or schema, will not save the results in the arguments");
      } else {
        Schema schema = queryResults.getSchema();
        FieldValueList firstRow = queryResults.iterateAll().iterator().next();
        for (int i = 0; i < schema.getFields().size(); i++) {
          Field field = schema.getFields().get(i);
          String name = field.getName();
          if (field.getMode().equals(Field.Mode.REPEATED)) {
            LOG.warn("Field {} is an array, will not save the value in the argument", name);
            continue;
          }
          if (field.getType().equals(LegacySQLTypeName.RECORD)) {
            LOG.warn("Field {} is a record type with nested schema, will not save the value in the argument", name);
            continue;
          }
          context.getArguments().set(name, firstRow.get(name).getStringValue());
        }
      }
    }

    long processedBytes =
        ((JobStatistics.QueryStatistics) queryJob.getStatistics()).getTotalBytesProcessed();
    LOG.info("Job {} processed {} bytes", queryJob.getJobId(), processedBytes);
    Map<String, String> tags = new ImmutableMap.Builder<String, String>()
        .put(Constants.Metrics.Tag.APP_ENTITY_TYPE, Action.PLUGIN_TYPE)
        .put(Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME, BigQueryExecute.NAME)
        .build();
    context.getMetrics().gauge(RECORDS_PROCESSED, rows);
    context.getMetrics().child(tags).countLong(BigQuerySinkUtils.BYTES_PROCESSED_METRIC,
        processedBytes);
  }

  @Override
  public AbstractBigQueryActionConfig getConfig() {
    return config;
  }

  /**
   * Config for the plugin.
   */
  public static final class Config extends AbstractBigQueryActionConfig {
    private static final String MODE = "mode";
    private static final String SQL = "sql";
    private static final String DATASET = "dataset";
    private static final String TABLE = "table";
    private static final String NAME_LOCATION = "location";
    private static final int ERROR_CODE_NOT_FOUND = 404;
    private static final String STORE_RESULTS = "storeResults";

    @Description("Dialect of the SQL command. The value must be 'legacy' or 'standard'. " +
      "If set to 'standard', the query will use BigQuery's standard SQL: " +
      "https://cloud.google.com/bigquery/sql-reference/. If set to 'legacy', BigQuery's legacy SQL " +
      "dialect will be used for this query.")
    @Macro
    private String dialect;

    @Name(SQL)
    @Description("SQL command to execute.")
    @Macro
    private String sql;

    @Name(MODE)
    @Description("Mode to execute the query in. The value must be 'batch' or 'interactive'. " +
      "An interactive query is executed as soon as possible and counts towards the concurrent rate " +
      "limit and the daily rate limit. A batch query is queued and started as soon as idle resources " +
      "are available, usually within a few minutes. If the query hasn't started within 3 hours, " +
      "its priority is changed to 'interactive'")
    @Macro
    private String mode;

    @Description("Use the cache when executing the query.")
    @Macro
    private String useCache;

    @Name(NAME_LOCATION)
    @Description("Location of the job. Must match the location of the dataset specified in the query. Defaults to 'US'")
    @Macro
    private String location;

    @Name(DATASET)
    @Description("Dataset to store the query results in. If not specified, the results will not be stored.")
    @Macro
    @Nullable
    private String dataset;

    @Name(TABLE)
    @Description("Table to store the query results in. If not specified, the results will not be stored.")
    @Macro
    @Nullable
    private String table;

    @Name(NAME_CMEK_KEY)
    @Macro
    @Nullable
    @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to the " +
      "dataset or table created by the plugin to store the query results. It is only applicable when users choose to " +
      "store the query results in a BigQuery table. More information can be found at " +
      "https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys")
    private String cmekKey;

    @Description("Row as arguments. For example, if the query is " +
      "'select min(id) as min_id, max(id) as max_id from my_dataset.my_table'," +
      "an arguments for 'min_id' and 'max_id' will be set based on the query results. " +
      "Plugins further down the pipeline can then" +
      "reference these values with macros ${min_id} and ${max_id}.")
    @Macro
    private String rowAsArguments;

    @Name(STORE_RESULTS)
    @Nullable
    @Description("Whether to store results in a BigQuery Table.")
    private Boolean storeResults;

    private Config(@Nullable String project, @Nullable String serviceAccountType, @Nullable String serviceFilePath,
                   @Nullable String serviceAccountJson, @Nullable String dataset, @Nullable String table,
                   @Nullable String location, @Nullable String cmekKey, @Nullable String dialect, @Nullable String sql,
                   @Nullable String mode, @Nullable Boolean storeResults) {
      this.project = project;
      this.serviceAccountType = serviceAccountType;
      this.serviceFilePath = serviceFilePath;
      this.serviceAccountJson = serviceAccountJson;
      this.dataset = dataset;
      this.table = table;
      this.location = location;
      this.cmekKey = cmekKey;
      this.dialect = dialect;
      this.sql = sql;
      this.mode = mode;
      this.storeResults = storeResults;
    }

    public boolean isLegacySQL() {
      return dialect.equalsIgnoreCase("legacy");
    }

    public boolean shouldUseCache() {
      return useCache.equalsIgnoreCase("true");
    }

    public boolean shouldSetAsArguments() {
      return rowAsArguments.equalsIgnoreCase("true");
    }

    public String getLocation() {
      return location;
    }

    public String getSql() {
      return sql;
    }

    public Boolean getStoreResults() {
      return storeResults == null || storeResults;
    }

    public QueryJobConfiguration.Priority getMode() {
      return QueryJobConfiguration.Priority.valueOf(mode.toUpperCase());
    }

    @Nullable
    public String getDataset() {
      return dataset;
    }

    @Nullable
    public String getTable() {
      return table;
    }

    @Override
    public void validate(FailureCollector failureCollector) {
      validate(failureCollector, Collections.emptyMap());
    }

    public void validate(FailureCollector failureCollector, Map<String, String> arguments) {
      // check the mode is valid
      if (!containsMacro(MODE)) {
        try {
          getMode();
        } catch (IllegalArgumentException e) {
          failureCollector.addFailure(e.getMessage(), "The mode must be 'batch' or 'interactive'.")
            .withConfigProperty(MODE);
        }
      }

      if (!containsMacro(SQL)) {
        if (Strings.isNullOrEmpty(sql)) {
          failureCollector.addFailure("SQL not specified.", "Please specify a SQL to execute")
            .withConfigProperty(SQL);
        } else {
          if (tryGetProject() != null && !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH)
            && !containsMacro(NAME_SERVICE_ACCOUNT_JSON)) {
            BigQuery bigquery = getBigQuery(failureCollector);
            validateSQLSyntax(failureCollector, bigquery);
          }
        }
      }

      // validates that either they are null together or not null together
      if ((!containsMacro(DATASET) && !containsMacro(TABLE)) &&
        (Strings.isNullOrEmpty(dataset) != Strings.isNullOrEmpty(table))) {
        failureCollector.addFailure("Dataset and table must be specified together.", null)
          .withConfigProperty(TABLE).withConfigProperty(DATASET);
      }

      if (!containsMacro(DATASET)) {
        BigQueryUtil.validateDataset(dataset, DATASET, failureCollector);
      }

      if (!containsMacro(TABLE)) {
        BigQueryUtil.validateTable(table, TABLE, failureCollector);
      }

      if (!containsMacro(NAME_CMEK_KEY)) {
        validateCmekKey(failureCollector, arguments);
      }

      failureCollector.getOrThrowException();
    }

    void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
      CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);
      //these fields are needed to check if bucket exists or not and for location validation
      if (cmekKeyName == null || containsMacro(DATASET) || containsMacro(NAME_LOCATION) || containsMacro(TABLE) ||
        projectOrServiceAccountContainsMacro() || Strings.isNullOrEmpty(dataset) || Strings.isNullOrEmpty(table) ||
        containsMacro(DATASET_PROJECT_ID)) {
        return;
      }
      String datasetProjectId = getDatasetProject();
      String datasetName = getDataset();
      DatasetId datasetId = DatasetId.of(datasetProjectId, datasetName);
      TableId tableId = TableId.of(datasetProjectId, datasetName, getTable());
      BigQuery bigQuery = getBigQuery(failureCollector);
      if (bigQuery == null) {
        return;
      }
      CmekUtils.validateCmekKeyAndDatasetOrTableLocation(bigQuery, datasetId, tableId, cmekKeyName, location,
                                                         failureCollector);
    }

    public void validateSQLSyntax(FailureCollector failureCollector, BigQuery bigQuery) {
      QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(sql).setDryRun(true).build();
      try {
        bigQuery.create(JobInfo.of(queryJobConfiguration));
      } catch (BigQueryException e) {
          final String errorMessage;
          if (e.getCode() == ERROR_CODE_NOT_FOUND)  {
            errorMessage = String.format("Resource was not found. Please verify the resource name. If the resource " +
              "will be created at runtime, then update to use a macro for the resource name. Error message received " +
              "was: %s", e.getMessage());
          } else {
               errorMessage = e.getMessage();
          }
          failureCollector.addFailure(String.format("%s.", errorMessage), "Please specify a valid query.")
                    .withConfigProperty(SQL);
      }
    }

    private BigQuery getBigQuery(FailureCollector failureCollector) {
      Credentials credentials = null;
      try {
        credentials = getServiceAccount() == null ?
          null : GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath());
      } catch (IOException e) {
        failureCollector.addFailure(e.getMessage(), null);
        failureCollector.getOrThrowException();
      }
      return GCPUtils.getBigQuery(getProject(), credentials);
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * BigQuery Execute configuration builder.
     */
    public static class Builder {
      private String serviceAccountType;
      private String serviceFilePath;
      private String serviceAccountJson;
      private String project;
      private String dataset;
      private String table;
      private String cmekKey;
      private String location;
      private String dialect;
      private String sql;
      private String mode;
      private Boolean storeResults;

      public Builder setProject(@Nullable String project) {
        this.project = project;
        return this;
      }

      public Builder setServiceAccountType(@Nullable String serviceAccountType) {
        this.serviceAccountType = serviceAccountType;
        return this;
      }

      public Builder setServiceFilePath(@Nullable String serviceFilePath) {
        this.serviceFilePath = serviceFilePath;
        return this;
      }

      public Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
        return this;
      }

      public Builder setDataset(@Nullable String dataset) {
        this.dataset = dataset;
        return this;
      }

      public Builder setTable(@Nullable String table) {
        this.table = table;
        return this;
      }

      public Builder setCmekKey(@Nullable String cmekKey) {
        this.cmekKey = cmekKey;
        return this;
      }

      public Builder setLocation(@Nullable String location) {
        this.location = location;
        return this;
      }

      public Builder setDialect(@Nullable String dialect) {
        this.dialect = dialect;
        return this;
      }

      public Builder setMode(@Nullable String mode) {
        this.mode = mode;
        return this;
      }

      public Builder setSql(@Nullable String sql) {
        this.sql = sql;
        return this;
      }

      public Config build() {
        return new Config(
          project,
          serviceAccountType,
          serviceFilePath,
          serviceAccountJson,
          dataset,
          table,
          location,
          cmekKey,
          dialect,
          sql,
          mode,
          storeResults
        );
      }

    }
  }
}
