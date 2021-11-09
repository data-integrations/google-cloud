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

package io.cdap.plugin.gcp.dataplex.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;

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

import java.io.IOException;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for DataplexOutputFormatProvider
 */
@RunWith(MockitoJUnitRunner.class)
public class DataPlexOutputFormatProviderTest {

  @Mock
  FileOutputCommitter fileOutputCommitter;
  @Mock
  JobContext mockJobContext;
  @Mock
  private RecordWriter mockWriter;
  @Mock
  private NullWritable mockWritable;
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
      .getLong(String.format(DataplexOutputFormatProvider.RECORD_COUNT_FORMAT, mockContext.getTaskAttemptID()), 0), 1);
  }

  @Test
  public void testDataPexOutputCommitter() throws IOException, InterruptedException {
    DataplexOutputCommitter committer = new DataplexOutputCommitter();
    JobID jobID = new JobID();
    TaskID taskID = new TaskID(jobID, TaskType.REDUCE, 1);
    TaskAttemptID taskAttemptID = new TaskAttemptID(taskID, 1);
    Configuration configuration = new Configuration();
    configuration.set(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE, DataplexBatchSink.BIGQUERY_DATASET_ASSET_TYPE);

    when(mockContext.getConfiguration()).thenReturn(configuration);
    when(mockContext.getTaskAttemptID()).thenReturn(taskAttemptID);

    committer.addDataplexOutputCommitterFromOutputFormat(fileOutputCommitter, mockContext);
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
    verify(fileOutputCommitter, times(2)).setupTask(mockContext);

    committerToTest.commitTask(mockContext);
    verify(fileOutputCommitter, times(1)).commitTask(mockContext);
  }
}
