/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.cloud.StringEnumValue;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.bigquery.common.BigQueryErrorUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class <code>BigQueryArgumentSetter</code> executes a single Cloud BigQuery SQL.
 * <p>
 * The plugin provides the ability to map columns name as pipeline arguments name and columns values
 * as pipeline arguments
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryArgumentSetter.NAME)
@Description("Argument setter for dynamically configuring pipeline from BiqQuery table.")
public final class BigQueryArgumentSetter extends AbstractBigQueryAction {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryArgumentSetter.class);
  public static final String NAME = "BigQueryArgumentSetter";
  private static final Set<LegacySQLTypeName> SUPPORTED_SQL_TYPES = ImmutableSet.of(
    LegacySQLTypeName.BOOLEAN,
    LegacySQLTypeName.STRING,
    LegacySQLTypeName.FLOAT,
    LegacySQLTypeName.INTEGER,
    LegacySQLTypeName.NUMERIC,
    LegacySQLTypeName.TIMESTAMP);
  private BigQueryArgumentSetterConfig config;

  @Override
  public AbstractBigQueryActionConfig getConfig() {
    return config;
  }

  @Override
  public void run(ActionContext context) {
    config.validate(context.getFailureCollector());

    QueryJobConfiguration queryConfig = config.getQueryJobConfiguration(context.getFailureCollector());
    JobId jobId = JobId.newBuilder().setRandomJob().build();

    // API request - starts the query.
    Credentials credentials = null;
    try {
      credentials = config.getServiceAccount() == null ? null :
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
    } catch (Exception e) {
      context.getFailureCollector().addFailure(
          String.format("Failed to load service account credentials, %s: %s",
              e.getClass().getName(), e.getMessage()), null).withStacktrace(e.getStackTrace());
      context.getFailureCollector().getOrThrowException();
    }
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials, null);
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    LOG.info("Executing SQL as job {}.", jobId.getJob());
    LOG.debug("The BigQuery SQL  {}", queryConfig.getQuery());

    // Wait for the query to complete
    try {
      queryJob.waitFor();
    } catch (BigQueryException e) {
      String errorMessage = String.format("The bigquery query job failed, %s: %s",
          e.getClass().getName(), e.getMessage());
      throw BigQueryErrorUtil.getProgramFailureException(errorMessage, (e).getReason(), e);
    } catch (InterruptedException e) {
      String errorMessage = String.format("The bigquery query job interrupted, %s: %s",
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.UNKNOWN, true, e);
    }

    // Check for errors
    if (queryJob.getStatus().getError() != null) {
      String errorReason = String.format(
          "The bigquery job failed with reason: %s. For more details, see %s",
          queryJob.getStatus().getError().getReason(), GCPUtils.BQ_SUPPORTED_DOC_URL);
      ErrorType type = BigQueryErrorUtil.getErrorType(queryJob.getStatus().getError().getReason());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
          queryJob.getStatus().getExecutionErrors().toString(), type, true, null, null,
          GCPUtils.BQ_SUPPORTED_DOC_URL, null);
    }
    TableResult queryResults;
    try {
      queryResults = queryJob.getQueryResults();
    } catch (BigQueryException e) {
      String errorMessage = String.format("The bigquery query job failed, %s: %s",
          e.getClass().getName(), e.getMessage());
      throw BigQueryErrorUtil.getProgramFailureException(errorMessage, e.getReason(), e);
    } catch (InterruptedException e) {
      String errorMessage = String.format("The bigquery query job interrupted, %s: %s",
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.UNKNOWN, false, e);
    }
    if (queryResults.getTotalRows() == 0 || queryResults.getTotalRows() > 1) {
      String error = String.format("The query result total rows should be \"1\" but is \"%d\"",
        queryResults.getTotalRows());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        error, error, ErrorType.USER, false, null);
    }

    Schema schema = queryResults.getSchema();
    FieldValueList row = queryResults.iterateAll().iterator().next();

    for (int i = 0; i < schema.getFields().size(); i++) {
      Field field = schema.getFields().get(i);
      if (!SUPPORTED_SQL_TYPES.contains(field.getType())) {
        context.getFailureCollector().addFailure(
          String.format("Field '%s'  with type '%s' , is not supported.", field.getName(), field.getType().name()),
          String.format("Supported types are: %s", SUPPORTED_SQL_TYPES.stream().map(StringEnumValue::name)
            .collect(Collectors.joining(","))));
        context.getFailureCollector().getOrThrowException();
      }

      String name = field.getName();
      FieldValue fieldValue = row.get(name);
      // For type  LegacySQLTypeName.TIMESTAMP string value will be seconds since epoch
      // (e.g. 1408452095.22 == 2014-08-19 07:41:35.220 -05:00)
      context.getArguments().set(name, fieldValue.getStringValue());
    }
  }
}
