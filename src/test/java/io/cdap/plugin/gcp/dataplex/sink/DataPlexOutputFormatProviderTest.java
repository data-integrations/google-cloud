package io.cdap.plugin.gcp.dataplex.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
  public void testDataPexOutputCommitter() throws IOException {
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

    committerToTest.isCommitJobRepeatable(mockJobContext);
    verify(fileOutputCommitter, times(1)).isCommitJobRepeatable(mockJobContext);

    committerToTest.isRecoverySupported();
    verify(fileOutputCommitter, times(1)).isRecoverySupported();

    committerToTest.isRecoverySupported(mockJobContext);
    verify(fileOutputCommitter, times(1)).isRecoverySupported(mockJobContext);

    committerToTest.needsTaskCommit(mockContext);
    verify(fileOutputCommitter, times(1)).needsTaskCommit(mockContext);

    committerToTest.recoverTask(mockContext);
    verify(fileOutputCommitter, times(1)).recoverTask(mockContext);

    committerToTest.setupJob(mockJobContext);
    verify(fileOutputCommitter, times(1)).setupJob(mockJobContext);

    committerToTest.setupTask(mockContext);
    verify(fileOutputCommitter, times(1)).setupTask(mockContext);

    Configuration configuration = new Configuration();
    when(mockContext.getConfiguration()).thenReturn(configuration);
    committerToTest.commitTask(mockContext);
    verify(fileOutputCommitter, times(1)).commitTask(mockContext);
  }
}
