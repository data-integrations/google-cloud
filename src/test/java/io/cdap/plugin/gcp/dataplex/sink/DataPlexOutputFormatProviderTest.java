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

package io.cdap.plugin.gcp.dataplex.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.gcs.sink.GCSOutputFormatProvider;

import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

/**
 * Tests for DataplexOutputFormatProvider
 */
@RunWith(MockitoJUnitRunner.class)
public class DataPlexOutputFormatProviderTest {

  public static final String DELEGATE_OUTPUTFORMAT_CLASSNAME = "gcssink.delegate.outputformat.classname";
  @Mock
  FileOutputCommitter fileOutputCommitter;
  @Mock
  JobContext mockJobContext;
  @Mock
  private RecordWriter mockWriter;
  @Mock
  private NullWritable mockWritable;
  @Mock
  private Configuration configuration;
  @Mock
  private Schema schema;
  @Mock
  private JobContext jobContext;
  @Mock
  private FormatContext context;
  @Mock
  private ValidatingOutputFormat delegate;
  @Mock
  private StructuredRecord mockRecord;
  @Mock
  private TaskAttemptContext mockContext;

  @Test
  public void testRecordWriter() throws IOException, InterruptedException {
    DataplexOutputFormatProvider.DataplexRecordWriter recordWriterToTest =
      new DataplexOutputFormatProvider.DataplexRecordWriter(mockWriter);
    recordWriterToTest.write(mockWritable, mockRecord);
    Configuration configuration = new Configuration();
    when(mockContext.getConfiguration()).thenReturn(configuration);
    recordWriterToTest.close(mockContext);

    //Verify that the delegate calls are being done as expected
    verify(mockWriter, times(1)).write(mockWritable, mockRecord);

    //Verify count is being recorded as expected
    Assert.assertEquals(configuration
      .getLong(String.format(GCSOutputFormatProvider.RECORD_COUNT_FORMAT,
        mockContext.getTaskAttemptID()), 0), 1);
  }

  @Test
  public void testGetClassName() {
    DataplexOutputFormatProvider dataplexOutputFormatProvider = new DataplexOutputFormatProvider
      (configuration, schema, delegate);
    dataplexOutputFormatProvider.validate(context);
    dataplexOutputFormatProvider.getOutputFormatConfiguration();
    Assert.assertEquals("io.cdap.plugin.gcp.dataplex.sink.DataplexOutputFormatProvider$DataplexOutputFormat",
      dataplexOutputFormatProvider.getOutputFormatClassName());
  }

  @Test
  public void testGetConfigurationWithDelegateNull() {
    Configuration configuration = new Configuration();
    configuration.set("name", "configName");
    configuration.set("blockSize", "240");
    DataplexOutputFormatProvider dataplexOutputFormatProvider = new DataplexOutputFormatProvider(configuration,
      schema, null);
    Assert.assertEquals("configName", dataplexOutputFormatProvider.getOutputFormatConfiguration().get("name"));
  }

  @Test
  public void testDataPexOutputCommitter() throws IOException, InterruptedException {

    JobID jobID = new JobID();
    TaskID taskID = new TaskID(jobID, TaskType.REDUCE, 1);
    TaskAttemptID taskAttemptID = new TaskAttemptID(taskID, 1);
    Configuration configuration = new Configuration();
    configuration.set(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE, DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE);

    DataplexOutputCommitter committer = new DataplexOutputCommitter(fileOutputCommitter);
    DataplexOutputCommitter committerToTest = spy(committer);
    JobStatus.State mockState = JobStatus.State.SUCCEEDED;

    //test all the delegation
    committerToTest.abortJob(mockJobContext, mockState);
    verify(fileOutputCommitter, times(1)).abortJob(mockJobContext, mockState);

    committerToTest.abortTask(mockContext);
    verify(fileOutputCommitter, times(1)).abortTask(mockContext);

    committerToTest.cleanupJob(mockJobContext);
    verify(fileOutputCommitter, times(1)).cleanupJob(mockJobContext);

    committerToTest.commitJob(mockJobContext);
    verify(fileOutputCommitter, times(1)).commitJob(mockJobContext);

    committerToTest.needsTaskCommit(mockContext);
    verify(fileOutputCommitter, times(1)).needsTaskCommit(mockContext);

    committerToTest.recoverTask(mockContext);
    verify(fileOutputCommitter, times(1)).recoverTask(mockContext);

    committerToTest.setupJob(mockJobContext);
    verify(fileOutputCommitter, times(1)).setupJob(mockJobContext);

    committerToTest.setupTask(mockContext);
    verify(fileOutputCommitter, times(1)).setupTask(mockContext);

    committerToTest.commitTask(mockContext);
    verify(fileOutputCommitter, times(1)).commitTask(mockContext);
  }

  /**
   * Exception is thrown as output path is not provided here.
   */
  @Test
  public void testDataPexOutputCommitterWDifferentFormat()
      throws IOException, InterruptedException {
    Configuration configuration = new Configuration();
    configuration.set(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE, "assetType");
    configuration.set("dataplexsink.assettype", "BIGQUERY_DATASET");
    configuration.set("mapred.bq.output.dataset.id", "id");
    configuration.set("mapred.bq.output.table.id", "tableId");
    configuration.set("mapred.bq.output.gcs.fileformat", "AVRO");
    configuration.set("mapred.bq.output.project.id", "projectId");
    configuration.set("mapred.bq.output.gcs.outputformatclass", AvroOutputFormat.class.getName());
    when(mockContext.getConfiguration()).thenReturn(configuration);
    DataplexOutputFormatProvider.DataplexOutputFormat dataplexOutputFormat =
        new DataplexOutputFormatProvider.DataplexOutputFormat();
    Assert.assertThrows(IOException.class, () -> {
      dataplexOutputFormat.getOutputCommitter(mockContext);
    });

  }

  /**
   * Exception is thrown as output path is not provided here.
   */
  @Test
  public void testOutputFormatWBigqueryDataset() throws IOException, InterruptedException {
    DataplexOutputFormatProvider.DataplexOutputFormat dataplexOutputFormat =
        new DataplexOutputFormatProvider.DataplexOutputFormat();
    Configuration configuration = new Configuration();
    configuration.set("name", "configName");
    configuration.set("blockSize", "240");
    configuration.set("dataplexsink.assettype", "BIGQUERY_DATASET");
    configuration.set("mapred.bq.output.dataset.id", "id");
    configuration.set("mapred.bq.output.table.id", "tableId");
    configuration.set("mapred.bq.output.gcs.fileformat", "AVRO");
    configuration.set("mapred.bq.output.project.id", "projectId");
    configuration.set("mapred.bq.output.gcs.outputformatclass", AvroOutputFormat.class.getName());
    when(mockContext.getConfiguration()).thenReturn(configuration);
    when(jobContext.getConfiguration()).thenReturn(configuration);
    Assert.assertThrows(IOException.class, () -> {
      dataplexOutputFormat.checkOutputSpecs(jobContext);
    });
    Assert.assertThrows(IOException.class, () -> {
      dataplexOutputFormat.getOutputCommitter(mockContext);
    });

  }

  @Test
  public void testGetRecordWriter() throws IOException, InterruptedException {
    DataplexOutputFormatProvider.DataplexOutputFormat dataplexOutputFormat =
      new DataplexOutputFormatProvider.DataplexOutputFormat();
    Configuration configuration = new Configuration();
    configuration.set("name", "configName");
    configuration.set("blockSize", "240");
    configuration.set("dataplexsink.assettype", "BIGQUERY_DATASET");
    configuration.set("mapred.bq.output.dataset.id", "id");
    configuration.set("mapred.bq.output.table.id", "tableId");
    configuration.set("mapred.bq.output.gcs.fileformat", "AVRO");
    when(mockContext.getConfiguration()).thenReturn(configuration);
    Assert.assertThrows(IOException.class, () -> {
      dataplexOutputFormat.getRecordWriter(mockContext);
    });
  }

  @Test
  public void testGetRecordWriterWithDifferentDataset() throws IOException, InterruptedException {
    DataplexOutputFormatProvider.DataplexOutputFormat dataplexOutputFormat =
      new DataplexOutputFormatProvider.DataplexOutputFormat();
    Configuration configuration = new Configuration();
    configuration.set("name", "configName");
    configuration.set("blockSize", "240");
    configuration.set("dataplexsink.assettype", "DATASET");
    configuration.set("mapred.bq.output.dataset.id", "id");
    configuration.set("mapred.bq.output.table.id", "tableId");
    configuration.set("mapred.bq.output.gcs.fileformat", "AVRO");
    configuration.set(DELEGATE_OUTPUTFORMAT_CLASSNAME, "class");
    when(mockContext.getConfiguration()).thenReturn(configuration);
    Assert.assertThrows(IOException.class, () -> {
      dataplexOutputFormat.getRecordWriter(mockContext);
    });
  }

}
