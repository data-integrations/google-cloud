package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.gcp.gcs.StorageClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Tests for GCSOutputformatProvider
 */
public class GCSOutputformatProviderTest {

  @Test
  public void testRecordWriter() throws IOException, InterruptedException {
    RecordWriter mockWriter = Mockito.mock(RecordWriter.class);
    GCSOutputFormatProvider.GCSRecordWriter recordWriterToTest = new GCSOutputFormatProvider.GCSRecordWriter(
      mockWriter);
    NullWritable mockWritable = Mockito.mock(NullWritable.class);
    StructuredRecord mockRecord = Mockito.mock(StructuredRecord.class);
    for (int i = 0; i < 5; i++) {
      recordWriterToTest.write(mockWritable, mockRecord);
    }
    TaskAttemptContext mockContext = Mockito.mock(TaskAttemptContext.class);
    Configuration configuration = new Configuration();
    Mockito.when(mockContext.getConfiguration()).thenReturn(configuration);
    recordWriterToTest.close(mockContext);
    //Verify that the delegate calls are being done as expected
    Mockito.verify(mockWriter, Mockito.times(5)).write(mockWritable, mockRecord);
    //verify that count is being recorded as expected
    Assert.assertTrue(configuration.getLong(
      String.format(GCSOutputFormatProvider.RECORD_COUNT_FORMAT, mockContext.getTaskAttemptID()), 0) == 5);
  }

  @Test
  public void testGCSOutputCommitter() throws IOException, URISyntaxException {
    FileOutputCommitter fileOutputCommitter = Mockito.mock(FileOutputCommitter.class);
    GCSOutputFormatProvider.GCSOutputCommitter committer = new GCSOutputFormatProvider.GCSOutputCommitter(
      fileOutputCommitter);
    GCSOutputFormatProvider.GCSOutputCommitter committerToTest = Mockito.spy(committer);
    JobContext mockJobContext = Mockito.mock(JobContext.class);
    JobStatus.State mockJobState = JobStatus.State.SUCCEEDED;
    TaskAttemptContext mockContext = Mockito.mock(TaskAttemptContext.class);
    //test all the delegation
    committerToTest.abortJob(mockJobContext, mockJobState);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).abortJob(mockJobContext, mockJobState);

    committerToTest.abortTask(mockContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).abortTask(mockContext);

    committerToTest.cleanupJob(mockJobContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).cleanupJob(mockJobContext);

    committerToTest.commitJob(mockJobContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).commitJob(mockJobContext);

    committerToTest.isCommitJobRepeatable(mockJobContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).isCommitJobRepeatable(mockJobContext);

    committerToTest.isRecoverySupported();
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).isRecoverySupported();

    committerToTest.isRecoverySupported(mockJobContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).isRecoverySupported(mockJobContext);

    committerToTest.needsTaskCommit(mockContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).needsTaskCommit(mockContext);

    committerToTest.recoverTask(mockContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).recoverTask(mockContext);

    committerToTest.setupJob(mockJobContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).setupJob(mockJobContext);

    committerToTest.setupTask(mockContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).setupTask(mockContext);

    Configuration configuration = new Configuration();
    Mockito.when(mockContext.getConfiguration()).thenReturn(configuration);
    Path committedTaskPathMock = Mockito.mock(Path.class);
    Mockito.when(fileOutputCommitter.getCommittedTaskPath(mockContext)).thenReturn(committedTaskPathMock);
    Mockito.when(committedTaskPathMock.toString()).thenReturn("gs://test");
    StorageClient mockStorage = Mockito.mock(StorageClient.class);
    Mockito.when(committerToTest.getStorageClient(configuration)).thenReturn(mockStorage);

    committerToTest.commitTask(mockContext);
    Mockito.verify(fileOutputCommitter, Mockito.times(1)).commitTask(mockContext);
  }
}
