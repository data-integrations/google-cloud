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
import com.google.cloud.bigquery.JobInfo;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.internal.util.reflection.FieldSetter;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class BigQueryExecuteConfigTest {

  @Test
  public void testBigQueryExecuteValidSQL() throws Exception {
    BigQueryExecute.Config config = new BigQueryExecute.Config("standard",
                                                               "select * from dataset.table where id=1",
                                                               "batch");

    FieldSetter.setField(config, GCPConfig.class.getDeclaredField("project"), "test_project");

    MockFailureCollector failureCollector = new MockFailureCollector();
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenReturn(ArgumentMatchers.any());

    config.validateSQLSyntax(failureCollector, bigQuery);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testBigQueryExecuteInvalidSQL() throws Exception {
    String errorMessage = "Invalid sql";
    BigQueryExecute.Config config = new BigQueryExecute.Config("standard",
                                                               "selcet * fro$m dataset-table where id=1",
                                                               "batch");

    FieldSetter.setField(config, GCPConfig.class.getDeclaredField("project"), "test_project");

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
