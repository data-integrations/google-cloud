/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mortbay.log.Log;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class BigQueryExecuteConfigTest {

  @Test
  public void testBigQueryExecuteValidSQL() throws Exception {
    BigQueryExecute.Config config = getConfig("select * from dataset.table where id=1");

    MockFailureCollector failureCollector = new MockFailureCollector();
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenReturn(ArgumentMatchers.any(Job.class));

    config.validateSQLSyntax(failureCollector, bigQuery);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }
  
  private BigQueryExecute.Config getConfig(String sql) throws NoSuchFieldException {
    BigQueryExecute.Config.Builder builder = BigQueryExecute.Config.builder();
    return builder
            .setDialect("standard")
            .setSql(sql)
            .setMode("batch")
            .setProject("test_project")
            .build();
  }

  @Test
  public void testBigQueryExecuteSQLWithNonExistentResource() throws Exception {
    String errorMessage = "Resource was not found. Please verify the resource name. If the resource will be created " +
      "at runtime, then update to use a macro for the resource name. Error message received was: ";
    BigQueryExecute.Config config = getConfig("select * from dataset.table where id=1");
    MockFailureCollector failureCollector = new MockFailureCollector();
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenThrow(new BigQueryException(404, ""));

    config.validateSQLSyntax(failureCollector, bigQuery);
    Log.warn("size : {}", failureCollector.getValidationFailures().size());
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals(String.format("%s.", errorMessage),
            failureCollector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("sql",
            failureCollector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
  }

  @Test
  public void testBigQueryExecuteInvalidSQL() throws Exception {
    String errorMessage = "Invalid sql";
    BigQueryExecute.Config config = getConfig("secelt * from dataset.table where id=1");
    MockFailureCollector failureCollector = new MockFailureCollector();
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenThrow(new BigQueryException(1, errorMessage));

    config.validateSQLSyntax(failureCollector, bigQuery);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals(String.format("%s.", errorMessage),
            failureCollector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("sql",
            failureCollector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
  }
}
