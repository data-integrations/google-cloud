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

package io.cdap.plugin.gcp.bigquery.action;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.action.ActionContext;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.exception.BigQueryJobExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class BigQueryExecuteTest {
  @Mock
  BigQuery bigQuery;
  @Mock
  Job queryJob;
  @Mock
  JobStatus jobStatus;
  @Mock
  BigQueryError bigQueryError;
  @Mock
  TableResult queryResults;
  @Mock
  JobStatistics.QueryStatistics queryStatistics;
  @Mock
  ActionContext context;
  @Mock
  StageMetrics stageMetrics;
  @Mock
  Metrics metrics;
  QueryJobConfiguration queryJobConfiguration;
  BigQueryExecute.Config config;
  JobInfo jobInfo;
  JobId jobId;
  BigQueryExecute bq;
  MockFailureCollector failureCollector;
  // Mock error message that will be returned by BigQuery when job fails to execute
  String mockErrorMessageNoRetry = "Job execution failed with error: $error";
  String errorMessageRetryExhausted = "Failed to execute BigQuery job. Reason: Retries exhausted.";

  @Before
  public void setUp() throws InterruptedException, NoSuchMethodException {
    MockitoAnnotations.initMocks(this);
    failureCollector = new MockFailureCollector();
    queryJobConfiguration = QueryJobConfiguration.newBuilder("select * from test").build();
    config = BigQueryExecute.Config.builder()
            .setLocation("US").setProject("testProject").setRowAsArguments("false")
            .setInitialRetryDuration(1L).setMaxRetryDuration(5L)
            .setMaxRetryCount(1).setRetryMultiplier(2.0).build();
    jobId = JobId.newBuilder().setRandomJob().setLocation(config.getLocation()).build();
    jobInfo = JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build();
    bq = new BigQueryExecute(config);

    // Mock Job Creation
    Mockito.when(bigQuery.create((JobInfo) Mockito.any())).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(jobStatus.getError()).thenReturn(bigQueryError);
    Mockito.when(bigQueryError.getMessage()).thenReturn(mockErrorMessageNoRetry);

    // Mock Successful Query
    Mockito.when(queryJob.getQueryResults()).thenReturn(queryResults);
    Mockito.when(queryResults.getTotalRows()).thenReturn(1L);
    Mockito.when(queryJob.getStatistics()).thenReturn(queryStatistics);
    Mockito.when(queryStatistics.getTotalBytesProcessed()).thenReturn(1L);

    // Mock context
    Mockito.when(context.getMetrics()).thenReturn(stageMetrics);
    Mockito.doNothing().when(stageMetrics).gauge(Mockito.anyString(), Mockito.anyLong());
    Mockito.when(stageMetrics.child(Mockito.any())).thenReturn(metrics);
    Mockito.doNothing().when(metrics).countLong(Mockito.anyString(), Mockito.anyLong());

  }

  @Test
  public void testExecuteQueryWithExponentialBackoffFailsWithNonRetryError() {
    Mockito.when(bigQueryError.getReason()).thenReturn("accessDenied");
    Exception exception = Assert.assertThrows(java.lang.RuntimeException.class, () -> {
      bq.executeQueryWithExponentialBackoff(bigQuery, queryJobConfiguration, context);
    });
    String actualMessage = exception.getMessage();
    Assert.assertEquals(mockErrorMessageNoRetry, actualMessage);
  }
  @Test
  public void testExecuteQueryWithExponentialBackoffFailsRetryError() {
    Mockito.when(bigQueryError.getReason()).thenReturn("jobBackendError");
    Mockito.when(bigQueryError.getMessage()).thenReturn(errorMessageRetryExhausted);
    Exception exception = Assert.assertThrows(BigQueryJobExecutionException.class, () -> {
      bq.executeQueryWithExponentialBackoff(bigQuery, queryJobConfiguration, context);
    });
    String actualMessage = exception.getMessage();
    Assert.assertEquals(errorMessageRetryExhausted, actualMessage);
  }

  @Test
  public void testExecuteQueryWithExponentialBackoffSuccess()
          throws Throwable {
    Mockito.when(jobStatus.getError()).thenReturn(null);
    Mockito.when(queryJob.getQueryResults()).thenReturn(queryResults);
    bq.executeQueryWithExponentialBackoff(bigQuery, queryJobConfiguration, context);
  }

  @Test
  public void testValidateRetryConfigurationWithDefaultValues() {
    config.validateRetryConfiguration(failureCollector,
            BigQueryExecute.Config.DEFAULT_INITIAL_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFULT_MAX_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT,
            BigQueryExecute.Config.DEFAULT_RETRY_MULTIPLIER);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidInitialRetryDuration() {
    config.validateRetryConfiguration(failureCollector, -1L,
            BigQueryExecute.Config.DEFULT_MAX_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT,
            BigQueryExecute.Config.DEFAULT_RETRY_MULTIPLIER);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Initial retry duration must be greater than 0.",
            failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidMaxRetryDuration() {
    config.validateRetryConfiguration(failureCollector,
            BigQueryExecute.Config.DEFAULT_INITIAL_RETRY_DURATION_SECONDS, -1L,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT,
            BigQueryExecute.Config.DEFAULT_RETRY_MULTIPLIER);
    Assert.assertEquals(2, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Max retry duration must be greater than 0.",
            failureCollector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("Max retry duration must be greater than initial retry duration.",
            failureCollector.getValidationFailures().get(1).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidRetryMultiplier() {
    config.validateRetryConfiguration(failureCollector,
            BigQueryExecute.Config.DEFAULT_INITIAL_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFULT_MAX_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT, -1.0);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Retry multiplier must be strictly greater than 1.",
            failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidRetryMultiplierAndMaxRetryCount() {
    config.validateRetryConfiguration(failureCollector,
            BigQueryExecute.Config.DEFAULT_INITIAL_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFULT_MAX_RETRY_DURATION_SECONDS, -1,
            BigQueryExecute.Config.DEFAULT_RETRY_MULTIPLIER);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Max retry count must be greater than 0.",
            failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithMultiplierOne() {
    config.validateRetryConfiguration(failureCollector,
            BigQueryExecute.Config.DEFAULT_INITIAL_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFULT_MAX_RETRY_DURATION_SECONDS,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT, 1.0);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Retry multiplier must be strictly greater than 1.",
            failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithMaxRetryLessThanInitialRetry() {
    config.validateRetryConfiguration(failureCollector, 10L, 5L,
            BigQueryExecute.Config.DEFAULT_MAX_RETRY_COUNT,
            BigQueryExecute.Config.DEFAULT_RETRY_MULTIPLIER);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Max retry duration must be greater than initial retry duration.",
            failureCollector.getValidationFailures().get(0).getMessage());
  }

}

