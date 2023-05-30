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
import com.google.cloud.bigquery.DatasetId;
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
import io.cdap.cdap.etl.mock.common.MockMetrics;
import io.cdap.cdap.etl.mock.common.MockStageMetrics;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    BigQuerySinkConfig config = new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString(),
                                                       "INTEGER", 0L, 100L, 10L, null);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBigQuerySinkInvalidConfig() {
    Schema invalidSchema = Schema.recordOf("record",
                                           Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    BigQuerySinkConfig config = new BigQuerySinkConfig("reference!!", "ds", "tb", "buck3t$$", invalidSchema.toString(),
                                                       "INTEGER", 0L, 100L, 10L, "200000");
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(3, failures.size());
  }

  @Test
  public void testBigQueryTimePartitionConfig() {
    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                      Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                      Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                      Schema.Field.of("timestamp",
                                      Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    BigQuerySinkConfig config =
      new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString(),
                             "TIME", 0L, 100L, 10L, null);
    config.partitionByField = "dt";

    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
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
    MockMetrics mockMetrics = Mockito.spy(new MockMetrics());
    MockStageMetrics mockStageMetrics = Mockito.spy(new MockStageMetrics("test"));
    BatchSinkContext context = getContextWithMetrics(mockStageMetrics);
    Mockito.when(mockStageMetrics.child(Mockito.any())).thenReturn(mockMetrics);
    sink.recordMetric(true, context);
    if (expectedCount > -1) {
      Assert.assertEquals(expectedCount, mockStageMetrics.getCount(AbstractBigQuerySink.RECORDS_UPDATED_METRIC));
      Assert.assertEquals(expectedCount, mockMetrics.getCount(BigQuerySinkUtils.BYTES_PROCESSED_METRIC));
    }
    Mockito.verify(mockStageMetrics, times(invocations)).count(anyString(), anyInt());
  }

  private static BigQuerySink getSinkToTest(Job mockJob) throws NoSuchFieldException {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    BigQuerySinkConfig config =
      new BigQuerySinkConfig("testmetric", "ds", "tb", "bkt", schema.toString(),
                             null, null, null, null, null);
    BigQuery bigQueryMock = mock(BigQuery.class);
    BigQuerySink sink = new BigQuerySink(config);
    setBigQuery(sink, bigQueryMock);
    Dataset mockDataSet = mock(Dataset.class);
    Mockito.when(mockDataSet.getLocation()).thenReturn("mock-location");
    Mockito.when(bigQueryMock.getDataset(any(DatasetId.class))).thenReturn(mockDataSet);
    Mockito.when(bigQueryMock.getDataset(anyString())).thenReturn(mockDataSet);
    Mockito.when(bigQueryMock.getJob(Mockito.any(JobId.class))).thenReturn(mockJob);
    return sink;
  }

  private static void setBigQuery(BigQuerySink sink, BigQuery bigQuery) throws NoSuchFieldException {
    FieldSetter.setField(sink, AbstractBigQuerySink.class.getDeclaredField("bigQuery"), bigQuery);
  }

  private BatchSinkContext getContextWithMetrics(MockStageMetrics mockStageMetrics) throws NoSuchFieldException {
    BatchSinkContext context = mock(SparkBatchSinkContext.class);
    Mockito.doReturn(mockStageMetrics).when(context).getMetrics();
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
    when(loadStatistics.getOutputBytes()).thenReturn(count);
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
    when(queryStatistics.getTotalBytesProcessed()).thenReturn(count);
    return job;
  }

  @Test(expected = ValidationException.class)
  public void testSchemaValidationNoTruncateNoSchemaRelaxationException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(false);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    BigQuerySinkUtils.validateSchema(
      table.getTableId().getTable(),
      table.getDefinition().getSchema(),
      sink.getConfig().getSchema(collector),
      false,
      false,
      "ds",
      collector);
  }

  @Test(expected = ValidationException.class)
  public void testSchemaValidationTruncateNoSchemaRelaxationException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(true);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    BigQuerySinkUtils.validateSchema(
      table.getTableId().getTable(),
      table.getDefinition().getSchema(),
      sink.getConfig().getSchema(collector),
      false,
      true,
      "ds",
      collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testSchemaValidationAllowSchemaRelaxationTruncateNoException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(true);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    BigQuerySinkUtils.validateSchema(
      table.getTableId().getTable(),
      table.getDefinition().getSchema(),
      sink.getConfig().getSchema(collector),
      true,
      true,
      "ds",
      collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test(expected = ValidationException.class)
  public void testSchemaValidationAllowSchemaRelaxationNoTruncateException() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(false);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    BigQuerySinkUtils.validateSchema(
      table.getTableId().getTable(),
      table.getDefinition().getSchema(),
      sink.getConfig().getSchema(collector),
      true,
      false,
      "ds",
      collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testSchemaValidationNullInputSchema() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(false);
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    Table table = getTestSchema();
    BigQuerySinkUtils.validateSchema(
      table.getTableId().getTable(),
      table.getDefinition().getSchema(),
      null,
      true,
      false,
      "ds",
      collector);
    BigQuerySinkUtils.validateInsertSchema(
      table,
      null,
      true,
      false,
      "ds",
      collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBigQuerySinkConfigValidChunkSize() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    BigQuerySinkConfig config = new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString(),
                                                       "INTEGER", 0L, 100L, 10L, "2097152");
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBigQuerySinkConfigInvalidChunkSize() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    BigQuerySinkConfig config = new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString(),
                                                       "INTEGER", 0L, 100L, 10L, "120000");
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }


  @Test
  public void testBigQueryDepthValidation() throws NoSuchFieldException {
    BigQuerySink sink = getValidationTestSink(false);

    // At maximum depth. There should be no error.
    Schema shouldPassSchema = getRecordOfDepth(BigQueryTypeSize.Struct.MAX_DEPTH);
    MockFailureCollector shouldPassCollector = new MockFailureCollector("bqsink");
    sink.validateRecordDepth(shouldPassSchema, shouldPassCollector);
    Assert.assertEquals(0, shouldPassCollector.getValidationFailures().size());

    // One deeper than Maximum depth. There should be one error.
    Schema shouldNotPass = getRecordOfDepth(BigQueryTypeSize.Struct.MAX_DEPTH + 1);
    MockFailureCollector shouldNotPassCollector = new MockFailureCollector("bqsink");
    sink.validateRecordDepth(shouldNotPass, shouldNotPassCollector);
    Assert.assertEquals(1, shouldNotPassCollector.getValidationFailures().size());
  }

  private Schema getRecordOfDepth(int n) {
    if (n <= 0) {
      return Schema.of(Schema.Type.STRING);
    }
    return Schema.recordOf("R" + String.valueOf(n),
                           Schema.Field.of("R" + String.valueOf(n - 1), getRecordOfDepth(n - 1)));
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
      new BigQuerySinkConfig("testmetric", "ds", "tb", "bkt", schema.toString(),
                             "INTEGER", 0L, 100L, 10L, null);
    FieldSetter.setField(config, AbstractBigQuerySinkConfig.class.getDeclaredField("truncateTable"),
                         truncateTable);
    return new BigQuerySink(config);
  }

  //Mocks used to configure testDatasetWithSpecialCharacters
  @Mock
  BigQueryMultiSinkConfig bigQueryMultiSinkConfig;

  @Test
  public void testDatasetWithSpecialCharacters() {
    BigQueryMultiSink multiSink = new BigQueryMultiSink(bigQueryMultiSinkConfig);
    Assert.assertEquals("table", multiSink.sanitizeOutputName("table"));
    Assert.assertEquals("sink_table", multiSink.sanitizeOutputName("sink_table"));
    Assert.assertEquals("table-2020", multiSink.sanitizeOutputName("table-2020"));
    Assert.assertEquals("table_2020", multiSink.sanitizeOutputName("table$2020"));
    Assert.assertEquals("new_table_2020", multiSink.sanitizeOutputName("new#table$2020"));
    Assert.assertEquals("new-table_2020", multiSink.sanitizeOutputName("new-table,2020"));
    Assert.assertEquals("new_table_2020", multiSink.sanitizeOutputName("new!table?2020"));
    Assert.assertEquals("new_table_2020", multiSink.sanitizeOutputName("new^table|2020"));
  }

  @Test
  public void testInitSQLEngineOutputDoesNotInitOutputWithNullSchema() throws Exception {
    BatchSinkContext sinkContext = mock(BatchSinkContext.class);
    MockFailureCollector collector = new MockFailureCollector("bqsink");

    BigQuerySinkConfig config =
      new BigQuerySinkConfig("testmetric", "ds", "tb", "bkt", null,
                             null, null, null, null, null);
    BigQuery bigQueryMock = mock(BigQuery.class);

    BigQuerySink sink = new BigQuerySink(config);
    sink.initSQLEngineOutput(sinkContext, bigQueryMock, "sink", "sink", "table", null, collector);
    verify(sinkContext, never()).addOutput(any());
  }
}
