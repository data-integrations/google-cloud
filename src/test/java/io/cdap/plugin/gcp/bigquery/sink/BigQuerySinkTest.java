/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.common.AbstractStageContext;
import io.cdap.cdap.etl.mock.common.MockStageMetrics;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link BigQuerySink}.
 */
public class BigQuerySinkTest {

  @Test
  public void testBigQuerySinkConfig() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    BigQuerySinkConfig config = new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString());
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBigQuerySinkInvalidConfig() {
    Schema invalidSchema = Schema.recordOf("record",
                                           Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    BigQuerySinkConfig config = new BigQuerySinkConfig("reference!!", "ds", "tb", "buck3t$$", invalidSchema.toString());
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
  }

  @Test
  public void testBigQuerySinkMetricInsert() throws Exception {
    Job mockJob = getMockLoadJob(10L);
    testMetric(mockJob, 10L, 1);
  }

  @Test
  public void testBigQuerySinkMetricInsertLargeCount1() throws Exception {
    Job mockJob = getMockLoadJob((long) Integer.MAX_VALUE + 1);
    testMetric(mockJob, -1, 2);
  }

  @Test
  public void testBigQuerySinkMetricInsertLargeCount2() throws Exception {
    Job mockLoadJob = getMockLoadJob((long) Integer.MAX_VALUE);
    testMetric(mockLoadJob, -1, 1);
  }

  @Test
  public void testBigQuerySinkMetricUpdate() throws Exception {
    Job mockJob = getMockQueryJob(20L);
    testMetric(mockJob, 20L, 1);
  }

  @Test
  public void testBigQuerySinkMetricUpsert() throws Exception {
    Job mockJob = getMockQueryJob(1000L);
    testMetric(mockJob, 1000L, 1);
  }

  private void testMetric(Job mockJob, long expectedCount, int invocations)
    throws NoSuchFieldException {
    BigQuerySink sink = getSinkToTest(mockJob);
    MockStageMetrics mockStageMetrics = Mockito.spy(new MockStageMetrics("test"));
    BatchSinkContext context = getContextWithMetrics(mockStageMetrics);
    sink.onRunFinish(true, context);
    if (expectedCount > -1) {
      Assert.assertEquals(expectedCount, mockStageMetrics.getCount(AbstractBigQuerySink.RECORDS_UPDATED_METRIC));
    }
    Mockito.verify(mockStageMetrics, times(invocations)).count(anyString(), anyInt());
  }

  private static BigQuerySink getSinkToTest(Job mockJob) throws NoSuchFieldException {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    BigQuerySinkConfig config =
      new BigQuerySinkConfig("testmetric", "ds", "tb", "bkt", schema.toString());
    BigQuery bigQueryMock = mock(BigQuery.class);
    BigQuerySink sink = new BigQuerySink(config);
    setBigQuery(sink, bigQueryMock);
    Dataset mockDataSet = mock(Dataset.class);
    Mockito.when(bigQueryMock.getDataset(anyString())).thenReturn(mockDataSet);
    Mockito.when(bigQueryMock.getJob(Mockito.any(JobId.class))).thenReturn(mockJob);
    return sink;
  }

  private static void setBigQuery(BigQuerySink sink, BigQuery bigQuery) throws NoSuchFieldException {
    FieldSetter.setField(sink, AbstractBigQuerySink.class.getDeclaredField("bigQuery"), bigQuery);
  }

  private BatchSinkContext getContextWithMetrics(MockStageMetrics mockStageMetrics) throws NoSuchFieldException {
    BatchSinkContext context = mock(SparkBatchSinkContext.class);
    FieldSetter.setField(context, AbstractStageContext.class.getDeclaredField("stageMetrics"), mockStageMetrics);
    return context;
  }

  private static Job getMockLoadJob(long count) {
    Job job = mock(Job.class);
    JobConfiguration jobConfiguration = mock(JobConfiguration.class);
    when(job.getConfiguration()).thenReturn(jobConfiguration);
    when(jobConfiguration.getType()).thenReturn(JobConfiguration.Type.LOAD);
    JobStatistics.LoadStatistics loadStatistics = mock(JobStatistics.LoadStatistics.class);
    when(job.getStatistics()).thenReturn(loadStatistics);
    when(loadStatistics.getOutputRows()).thenReturn(count);
    return job;
  }

  private static Job getMockQueryJob(long count) {
    Job job = mock(Job.class);
    JobConfiguration jobConfiguration = mock(JobConfiguration.class);
    when(job.getConfiguration()).thenReturn(jobConfiguration);
    when(jobConfiguration.getType()).thenReturn(JobConfiguration.Type.QUERY);
    JobStatistics.QueryStatistics queryStatistics = mock(JobStatistics.QueryStatistics.class);
    when(job.getStatistics()).thenReturn(queryStatistics);
    when(queryStatistics.getNumDmlAffectedRows()).thenReturn(count);
    return job;
  }

  @Test(expected = ValidationException.class)
  public void testSchemaValidationException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(false);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    sink.validateSchema(table, sink.getConfig().getSchema(collector), false, collector);
  }

  @Test
  public void testSchemaValidationNoException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(true);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    sink.validateSchema(table, sink.getConfig().getSchema(collector), false, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private Table getTestSchema() {
    Table table = mock(Table.class);
    TableId tableId = TableId.of("test", "testds", "testtab");
    when(table.getTableId()).thenReturn(tableId);
    TableDefinition mock = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(mock);
    Field field = Field.of("Field1", LegacySQLTypeName.STRING);
    when(mock.getSchema()).thenReturn(com.google.cloud.bigquery.Schema.of(field));
    return table;
  }

  private BigQuerySink getValidationTestSink(boolean truncateTable) throws NoSuchFieldException {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    BigQuerySinkConfig config =
      new BigQuerySinkConfig("testmetric", "ds", "tb", "bkt", schema.toString());
    FieldSetter.setField(config, AbstractBigQuerySinkConfig.class.getDeclaredField("truncateTable"),
                         truncateTable);
    BigQuery bigQueryMock = mock(BigQuery.class);
    return new BigQuerySink(config);
  }

}
