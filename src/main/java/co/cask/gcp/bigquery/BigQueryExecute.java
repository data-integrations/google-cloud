/*
 * Copyright © 2018 Cask Data, Inc.
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
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * This class <code>BigQueryExecute</code> executes a Cloud BigQuery SQL query.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryExecute.NAME)
@Description("Execute a Google BigQuery SQL.")
public final class BigQueryExecute extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecute.class);
  public static final String NAME = "BigQueryExecute";
  private Config config;

  @Override
  public void run(ActionContext actionContext) throws Exception {
    LOG.info("Query %s", config.sql);
    // Check if a SQL has been specified, if the SQL has not been specified,
    // then there is nothing for this class to do.
    if (config.sql == null || config.sql.isEmpty()) {
      throw new IllegalArgumentException(
        "SQL not specified. Please specify a SQL to execute"
      );
    }

    QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(config.sql);

    // Run at batch priority, which won't count toward concurrent rate limit.
    if (config.mode.equalsIgnoreCase("batch")) {
      builder.setPriority(QueryJobConfiguration.Priority.BATCH);
    }

    // Save the results of the query to a permanent table.
    if (config.dataset != null && config.table != null
      && !config.dataset.isEmpty() && !config.table.isEmpty()) {
      builder.setDestinationTable(TableId.of(config.dataset, config.table));
    }

    // Disable the query cache to force live query evaluation.
    if (config.cache != null && config.cache.equalsIgnoreCase("true")) {
      builder.setUseQueryCache(true);
    } else {
      builder.setUseQueryCache(false);
    }

    QueryJobConfiguration queryConfig = builder.build();

    // Location must match that of the dataset(s) referenced in the query.
    JobId jobId = JobId.newBuilder().setRandomJob().setLocation("US").build();
    String jobIdString = jobId.getJob();

    // API request - starts the query.
    bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
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
    @Name("sql")
    @Description("SQL query to execuute.")
    @Macro
    public String sql;

    @Name("model")
    @Description("Batch or Interactive.")
    @Macro
    public String mode;

    @Name("cache")
    @Description("Use Cache")
    @Macro
    public Boolean cache;

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
