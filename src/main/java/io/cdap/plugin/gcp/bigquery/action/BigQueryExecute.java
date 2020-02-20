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
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class BigQueryExecute extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecute.class);
  public static final String NAME = "BigQueryExecute";
  private static final String RECORDS_PROCESSED = "records.processed";

  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate(context.getFailureCollector());
    QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(config.getSql());
    // Run at batch priority, which won't count toward concurrent rate limit.
    if (config.getMode().equals(QueryJobConfiguration.Priority.BATCH)) {
      builder.setPriority(QueryJobConfiguration.Priority.BATCH);
    } else {
      builder.setPriority(QueryJobConfiguration.Priority.INTERACTIVE);
    }

    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    if (cmekKey != null) {
      builder.setDestinationEncryptionConfiguration(
        EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKey).build());
    }

    // Save the results of the query to a permanent table.
    if (config.getDataset() != null && config.getTable() != null) {
      builder.setDestinationTable(TableId.of(config.getDataset(), config.getTable()));
    }

    // Enable or Disable the query cache to force live query evaluation.
    if (config.shouldUseCache()) {
      builder.setUseQueryCache(true);
    }

    // Enable legacy SQL
    builder.setUseLegacySql(config.isLegacySQL());

    QueryJobConfiguration queryConfig = builder.build();

    // Location must match that of the dataset(s) referenced in the query.
    JobId jobId = JobId.newBuilder().setRandomJob().setLocation(config.getLocation()).build();

    // API request - starts the query.
    Credentials credentials = config.getServiceAccountFilePath() == null ?
                                null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    LOG.info("Executing SQL as job {}.", jobId.getJob());
    LOG.debug("The BigQuery SQL is {}", config.getSql());

    // Wait for the query to complete
    queryJob.waitFor();

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

    context.getMetrics().gauge(RECORDS_PROCESSED, rows);
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    config.validate(configurer.getStageConfigurer().getFailureCollector());
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    private static final String MODE = "mode";
    private static final String SQL = "sql";
    private static final String DATASET = "dataset";
    private static final String TABLE = "table";

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

    @Description("Row as arguments. For example, if the query is " +
                   "'select min(id) as min_id, max(id) as max_id from my_dataset.my_table'," +
                   "an arguments for 'min_id' and 'max_id' will be set based on the query results. " +
                   "Plugins further down the pipeline can then" +
                   "reference these values with macros ${min_id} and ${max_id}.")
    @Macro
    private String rowAsArguments;

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

    public void validate(FailureCollector failureCollector) {
      // check the mode is valid
      if (!containsMacro(MODE)) {
        try {
          getMode();
        } catch (IllegalArgumentException e) {
          failureCollector.addFailure(e.getMessage(), "The mode must be 'batch' or 'interactive'.")
            .withConfigProperty(MODE);
        }
      }

      if (!containsMacro(SQL) && Strings.isNullOrEmpty(sql)) {
        failureCollector.addFailure("SQL not specified. Please specify a SQL to execute", null).withConfigProperty(SQL);
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

      failureCollector.getOrThrowException();
    }
  }
}
