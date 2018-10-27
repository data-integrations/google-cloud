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
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
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
 *
 * The plugin provides the ability different options like choosing interactive or batch execution
 * of sql query, setting of resulting dataset and table, enabling/disbaling cache, specifying whether
 * the query being executed is legacy or standard and reattempt strategy.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryExecute.NAME)
@Description("Execute a Google BigQuery SQL.")
public final class BigQueryExecute extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecute.class);
  public static final String NAME = "BigQueryExecute";
  private static final String RECORDS_OUT = "records.out";
  private static final String MODE_BATCH = "batch";
  private static final String CACHE_ENABLED = "enabled";
  private static final String LEGACY_ENABLED = "legacy-enabled";

  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    // Check if a SQL has been specified, if the SQL has not been specified,
    // then there is nothing for this class to do.
    if (config.sql == null || config.sql.isEmpty()) {
      throw new IllegalArgumentException(
        "SQL not specified. Please specify a SQL to execute"
      );
    }

    QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(config.sql);

    // Run at batch priority, which won't count toward concurrent rate limit.
    if (config.mode.equalsIgnoreCase(MODE_BATCH)) {
      builder.setPriority(QueryJobConfiguration.Priority.BATCH);
    } else {
      builder.setPriority(QueryJobConfiguration.Priority.INTERACTIVE);
    }

    // Save the results of the query to a permanent table.
    if (config.dataset != null && config.table != null
      && !config.dataset.isEmpty() && !config.table.isEmpty()) {
      builder.setDestinationTable(TableId.of(config.dataset, config.table));
      context.getArguments().set(context.getStageName() + ".dataset", config.dataset);
      context.getArguments().set(context.getStageName() + ".table", config.dataset);
    }

    // Enable or Disable the query cache to force live query evaluation.
    if (config.cache.equalsIgnoreCase(CACHE_ENABLED)) {
      builder.setUseQueryCache(true);
    }

    // Enable legacy SQL
    if (config.legacy.equalsIgnoreCase(LEGACY_ENABLED)) {
      builder.setUseLegacySql(true);
    } else {
      builder.setUseLegacySql(false);
    }

    QueryJobConfiguration queryConfig = builder.build();

    // Location must match that of the dataset(s) referenced in the query.
    JobId jobId = JobId.newBuilder().setRandomJob().setLocation("US").build();

    // API request - starts the query.
    BigQueryOptions.Builder bqBuilder = GCPUtils.getBigQuery(config.getProject(), config.getServiceAccountFilePath());
    BigQuery bigquery = bqBuilder.build().getService();
    Job queryJob = bigquery.create(
      JobInfo.newBuilder(queryConfig)
        .setJobId(jobId).build()
    );

    LOG.info("Executing query %s, Job id %s.", config.sql, jobId.getJob());

    // Wait for the query to complete
    RetryOption retryOption = RetryOption.totalTimeout(Duration.ofMinutes(10));
    queryJob.waitFor(retryOption);

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job " + jobId.getJob() + " no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getExecutionErrors().toString());
    }

    TableResult queryResults = queryJob.getQueryResults();
    long rows = queryResults.getTotalRows();

    context.getMetrics().gauge(RECORDS_OUT, rows);
    context.getArguments().set(context.getStageName() + ".query", config.sql);
    context.getArguments().set(context.getStageName() + ".jobid", jobId.getJob());
    context.getArguments().set(RECORDS_OUT, String.valueOf(rows));
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    if (!config.containsMacro("sql")) {
      if (config.sql == null || config.sql.isEmpty()) {
        throw new IllegalArgumentException("SQL not specified. Please specify a SQL to execute");
      }
    }
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    @Name("legacy")
    @Description("Use Legacy SQL")
    @Macro
    public String legacy;

    @Name("sql")
    @Description("SQL query to execuute.")
    @Macro
    public String sql;

    @Name("mode")
    @Description("Batch or Interactive.")
    @Macro
    public String mode;

    @Name("cache")
    @Description("Use Cache")
    @Macro
    public String cache;

    @Name("dataset")
    @Description("Permanent Dataset.")
    @Macro
    @Nullable
    public String dataset;

    @Name("table")
    @Description("Permanent Table.")
    @Macro
    @Nullable
    public String table;
  }
}
