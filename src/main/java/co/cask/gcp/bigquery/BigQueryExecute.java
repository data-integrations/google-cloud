/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.common.GCPConfig;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

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
  private static final String RECORDS_OUT = "records.out";
  private static final String MODE_BATCH = "batch";

  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(config.getSql());
    // Run at batch priority, which won't count toward concurrent rate limit.
    if (config.getMode().equalsIgnoreCase(MODE_BATCH)) {
      builder.setPriority(QueryJobConfiguration.Priority.BATCH);
    } else {
      builder.setPriority(QueryJobConfiguration.Priority.INTERACTIVE);
    }

    // Save the results of the query to a permanent table.
    if (config.getDataset() != null  && config.getTable() != null) {
      builder.setDestinationTable(TableId.of(config.getDataset(), config.getTable()));
      context.getArguments().set(context.getStageName() + ".dataset", config.getDataset());
      context.getArguments().set(context.getStageName() + ".table", config.getTable());
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
    BigQuery bigQuery = BigQueryUtils.getBigQuery(config.getServiceAccountFilePath(), config.getProject());
    Job queryJob = bigQuery.create(
      JobInfo.newBuilder(queryConfig)
        .setJobId(jobId).build()
    );

    LOG.info("Executing query '%s'. The Google BigQuery job id is '%s'.", config.getSql(), jobId.getJob());

    // Wait for the query to complete
    RetryOption retryOption = RetryOption.totalTimeout(Duration.ofMinutes(config.getQueryTimeoutInMins()));
    queryJob.waitFor(retryOption);

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getExecutionErrors().toString());
    }

    TableResult queryResults = queryJob.getQueryResults();
    long rows = queryResults.getTotalRows();

    context.getMetrics().gauge(RECORDS_OUT, rows);
    context.getArguments().set(context.getStageName().concat(".query"), config.getSql());
    context.getArguments().set(context.getStageName().concat(".jobid"), jobId.getJob());
    context.getArguments().set(context.getStageName().concat(RECORDS_OUT), String.valueOf(rows));
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    config.validate();
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    @Description("Use Legacy SQL.")
    @Macro
    private String legacy;

    @Description("SQL query to execute.")
    @Macro
    private String sql;

    @Description("Mode to execute the query in. The value must be 'batch' or 'interactive'. " +
      "A batch query is executed as soon as possible and count towards the concurrent rate limit and the daily " +
      "rate limit. An interactive query is queued and started as soon as idle resources are available, usually " +
      "within a few minutes. If the query hasn't started within 3 hours, its priority is changed to 'INTERACTIVE'")
    @Macro
    private String mode;

    @Description("Use the cache when executing the query.")
    @Macro
    private String cache;

    @Description("Location of the job. Must match the location of the dataset specified in the query. Defaults to 'US'")
    @Macro
    private String location;

    @Description("The dataset to store the query results in. If not specified, the results will not be stored.")
    @Macro
    @Nullable
    private String dataset;

    @Description("The table to store the query results in. If not specified, the results will not be stored.")
    @Macro
    @Nullable
    private String table;

    @Name("timeout")
    @Description("Query timeout in minutes. Defaults to 10.")
    @Macro
    private Long queryTimeoutInMins;

    public Long getQueryTimeoutInMins() {
      return queryTimeoutInMins == null ? 10 : queryTimeoutInMins;
    }

    public boolean isLegacySQL() {
      return legacy.equalsIgnoreCase("true");
    }

    public boolean shouldUseCache() {
      return cache.equalsIgnoreCase("true");
    }

    public String getLocation() {
      return location;
    }

    public String getLegacy() {
      return legacy;
    }

    public String getSql() {
      return sql;
    }

    public String getMode() {
      return mode;
    }

    public String getCache() {
      return cache;
    }

    @Nullable
    public String getDataset() {
      return dataset;
    }

    @Nullable
    public String getTable() {
      return table;
    }

    public void validate() {
      if (!containsMacro("sql") && (sql == null || sql.isEmpty())) {
        throw new IllegalArgumentException("SQL not specified. Please specify a SQL to execute");
      }

      // validates that either they are null together or not null together
      if ((dataset == null && table != null) || (table == null && dataset != null)) {
        throw new IllegalArgumentException("Dataset and table must be specified together.");
      }

      if (dataset != null) {
        // if one is not null then we know another is not null either. Now validate they are empty or non-empty
        // together
        if ((dataset.isEmpty() && !table.isEmpty()) ||
          (table.isEmpty() && !dataset.isEmpty())) {
          throw new IllegalArgumentException("Dataset and table must be specified together.");
        }
      }
    }
  }
}
