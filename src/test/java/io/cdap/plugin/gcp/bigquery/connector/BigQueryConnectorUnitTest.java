/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.connector;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableResult;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.connector.SampleType;
import io.cdap.plugin.gcp.bigquery.util.BigQueryDataParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryConnectorUnitTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final BigQueryConnector CONNECTOR = new BigQueryConnector(null);
  private String tableName;
  private String sessionID;
  private int limit;
  private String strata;
  private Job queryJob;

  @Before
  public void setUp() {
    tableName = "`project.dataset.table`";
    sessionID = UUID.randomUUID().toString().replace('-', '_');
    limit = 100;
    strata = "strata";
  }

  /**
   * Unit tests for getTableQuery()
   */
  @Test
  public void getTableQueryTest() {
    // random query
    Assert.assertEquals(String.format("WITH table AS (\n" +
                                        "  SELECT *, RAND() AS r_%s\n" +
                                        "  FROM %s\n" +
                                        "  WHERE RAND() < 2*%d/(SELECT COUNT(*) FROM %s)\n" +
                                        ")\n" +
                                        "SELECT * EXCEPT (r_%s)\n" +
                                        "FROM table\n" +
                                        "ORDER BY r_%s\n" +
                                        "LIMIT %d",
                                      sessionID, tableName, limit, tableName, sessionID, sessionID, limit),
                        CONNECTOR.getTableQuery(tableName, limit, SampleType.RANDOM, null, sessionID));

    // stratified query
    Assert.assertEquals(String.format("SELECT * EXCEPT (`sqn_%s`, `c_%s`)\n" +
                                        "FROM (\n" +
                                        "SELECT *, row_number() OVER (ORDER BY %s, RAND()) AS sqn_%s,\n" +
                                        "COUNT(*) OVER () as c_%s,\n" +
                                        "FROM %s\n" +
                                        ") %s\n" +
                                        "WHERE MOD(sqn_%s, CAST(c_%s / %d AS INT64)) = 1\n" +
                                        "ORDER BY %s\n" +
                                        "LIMIT %d",
                                      sessionID, sessionID, strata, sessionID, sessionID, tableName, tableName,
                                      sessionID, sessionID, limit, strata, limit),
                        CONNECTOR.getTableQuery(tableName, limit, SampleType.STRATIFIED, strata, sessionID));

    // default query
    Assert.assertEquals(String.format("SELECT * FROM %s LIMIT %d", tableName, limit),
                        CONNECTOR.getTableQuery(tableName, limit, SampleType.DEFAULT, null, sessionID));
  }

  /**
   * Test for {@link IllegalArgumentException} from getTableQuery when attempting stratified query with null strata
   * @throws IllegalArgumentException expected
   */
  @Test
  public void getTableQueryNullStrataTest() throws IllegalArgumentException {
    expectedEx.expect(IllegalArgumentException.class);
    CONNECTOR.getTableQuery(tableName, limit, SampleType.STRATIFIED, null, sessionID);
  }

  /**
   * Test for {@link IOException} from getQueryResult() when attempting on null query job
   * @throws IOException expected
   */
  @Test
  public void getQueryResultNullJobTest() throws IOException {
    expectedEx.expect(IOException.class);
    CONNECTOR.getQueryResult(null, sessionID);
  }

  /**
   * Test for {@link IOException} from getQueryResult() if job timed out
   * @throws IOException expected
   */
  @Test
  public void getQueryResultTimedOutTest() throws IOException {
    expectedEx.expect(IOException.class);
    queryJob = mock(Job.class);
    doAnswer(invocation -> false).when(queryJob).isDone();
    CONNECTOR.getQueryResult(queryJob, sessionID);
  }

  /**
   * Test for {@link IOException} from getQueryResult() if query has error
   * @throws IOException expected
   */
  @Test
  public void getQueryResultErrorTest() throws IOException {
    expectedEx.expect(IOException.class);
    queryJob = mock(Job.class);
    doAnswer(invocation -> true).when(queryJob).isDone();
    JobStatus status = mock(JobStatus.class);
    doAnswer(invocation -> mock(BigQueryError.class)).when(status).getError();
    doAnswer(invocation -> status).when(queryJob).getStatus();
    CONNECTOR.getQueryResult(queryJob, sessionID);
  }

  /**
   * Test for {@link IOException} from getQueryResult() if query is interrupted
   * @throws InterruptedException to get IOException
   * @throws IOException expected
   */
  @Test
  public void getQueryResultInterruptedTest() throws InterruptedException, IOException {
    expectedEx.expect(IOException.class);
    queryJob = mock(Job.class);
    doAnswer(invocation -> true).when(queryJob).isDone();
    doAnswer(invocation -> mock(JobStatus.class)).when(queryJob).getStatus();
    doThrow(mock(InterruptedException.class)).when(queryJob).getQueryResults();
    CONNECTOR.getQueryResult(queryJob, sessionID);
  }

}
