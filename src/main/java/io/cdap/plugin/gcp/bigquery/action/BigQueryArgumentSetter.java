/*
 * Copyright © 2020 AdaptiveScale, Inc.
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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class <code>BigQueryArgumentSetter</code> executes a single Cloud BigQuery SQL.
 * <p>
 * The plugin provides the ability to map columns name as pipeline arguments name and columns values
 * as pipeline arguments
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryArgumentSetter.NAME)
@Description("Argument setter for dynamically configuring pipeline from BiqQuery table.")
public final class BigQueryArgumentSetter extends Action {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryArgumentSetter.class);
  public static final String NAME = "BigQueryArgumentSetter";
  private BigQueryArgumentSetterConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate(context.getFailureCollector());

    QueryJobConfiguration queryConfig = config.getQueryJobConfiguration();
    JobId jobId = JobId.newBuilder().setRandomJob().build();

    // API request - starts the query.
    Credentials credentials = config.getServiceAccountFilePath() == null ?
        null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    LOG.info("Executing SQL as job {}.", jobId.getJob());
    LOG.debug("The BigQuery SQL  {}", queryConfig.getQuery());

    // Wait for the query to complete
    queryJob.waitFor();

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getExecutionErrors().toString());
    }

    TableResult queryResults = queryJob.getQueryResults();
    if (queryResults.getTotalRows() == 0 || queryResults.getTotalRows() > 1) {
      throw new RuntimeException(String
          .format("The query result total rows should be \"1\" but is \"%d\"",
              queryResults.getTotalRows()));
    }

    Schema schema = queryResults.getSchema();
    FieldValueList row = queryResults.iterateAll().iterator().next();

    for (int i = 0; i < schema.getFields().size(); i++) {
      Field field = schema.getFields().get(i);
      String name = field.getName();
      String stringValue = row.get(name).getStringValue();
      context.getArguments().set(name, stringValue);
    }
  }
}
