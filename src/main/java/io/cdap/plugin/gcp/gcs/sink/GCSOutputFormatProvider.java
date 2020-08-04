package io.cdap.plugin.gcp.gcs.sink;

import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.StorageClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputFormatProvider for GCSSink
 */
public class GCSOutputFormatProvider implements ValidatingOutputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputFormatProvider.class);
  private static final String DELEGATE_OUTPUTFORMAT_CLASSNAME = "gcssink.delegate.outputformat.classname";
  private static final String OUTPUT_FOLDER = "gcssink.metric.output.folder";
  public static final String RECORD_COUNT_FORMAT = "recordcount.%s";
  private final ValidatingOutputFormat delegate;

  public GCSOutputFormatProvider(ValidatingOutputFormat delegate) {
    this.delegate = delegate;
  }

  @Override
  public void validate(FormatContext context) {
    delegate.validate(context);
  }

  @Override
  public String getOutputFormatClassName() {
    return GCSOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> outputFormatConfiguration = new HashMap<>(delegate.getOutputFormatConfiguration());
    outputFormatConfiguration.put(DELEGATE_OUTPUTFORMAT_CLASSNAME, delegate.getOutputFormatClassName());
    return outputFormatConfiguration;
  }

  /**
   * OutputFormat for GCS Sink
   */
  public static class GCSOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
    private OutputFormat delegateFormat;

    private OutputFormat getDelegateFormatInstance(Configuration configuration) throws IOException {
      if (delegateFormat != null) {
        return delegateFormat;
      }

      String delegateClassName = configuration.get(DELEGATE_OUTPUTFORMAT_CLASSNAME);
      try {
        delegateFormat = (OutputFormat) ReflectionUtils
          .newInstance(configuration.getClassByName(delegateClassName), configuration);
        return delegateFormat;
      } catch (ClassNotFoundException e) {
        throw new IOException(
          String.format("Unable to instantiate output format for class %s", delegateClassName),
          e);
      }
    }

    @Override
    public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext taskAttemptContext) throws
      IOException, InterruptedException {
      RecordWriter originalWriter = getDelegateFormatInstance(taskAttemptContext.getConfiguration())
        .getRecordWriter(taskAttemptContext);
      return new GCSRecordWriter(originalWriter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
      getDelegateFormatInstance(jobContext.getConfiguration()).checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
      InterruptedException {
      OutputCommitter delegateCommitter = getDelegateFormatInstance(taskAttemptContext.getConfiguration())
        .getOutputCommitter(taskAttemptContext);
      return new GCSOutputCommitter(delegateCommitter);
    }
  }

  /**
   * OutputCommitter for GCS
   */
  public static class GCSOutputCommitter extends OutputCommitter {

    private final OutputCommitter delegate;

    public GCSOutputCommitter(OutputCommitter delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      delegate.setupJob(jobContext);
    }

    @Override
    public void cleanupJob(JobContext jobContext) throws IOException {
      delegate.cleanupJob(jobContext);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      delegate.commitJob(jobContext);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
      delegate.abortJob(jobContext, state);
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
      delegate.setupTask(taskAttemptContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
      return delegate.needsTaskCommit(taskAttemptContext);
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
      /*On commit task, there seems to be some inconsistency across different hadoop implementations regarding the path
       where output file is stored. For some implementations it appears in the path returned by FileOutputCommitter
       getCommittedTaskPath and for some it does not.Before commit, the files appear to be consistently present in path
       returned by FileOutputCommitter getTaskAttemptPath. Hence, find the output file from taskAttemptPath and add
       metadata before commit happens. After commit, file would have been moved out of the taskAttemptPath. */
      try {
        updateMetricMetaData(taskAttemptContext);
      } catch (Exception exception) {
        LOG.warn("Unable to record metric for task. Metric emitted for the number of affected rows may be incorrect.",
                 exception);
      }

      delegate.commitTask(taskAttemptContext);
    }

    private void updateMetricMetaData(TaskAttemptContext taskAttemptContext) throws IOException {
      if (!(delegate instanceof FileOutputCommitter)) {
        return;
      }

      FileOutputCommitter fileOutputCommitter = (FileOutputCommitter) delegate;
      Configuration configuration = taskAttemptContext.getConfiguration();
      //Task is not yet committed, so should be available in attempt path
      Path taskAttemptPath = fileOutputCommitter.getTaskAttemptPath(taskAttemptContext);
      if (configuration == null || taskAttemptPath == null) {
        return;
      }

      //read the count from configuration
      String keyInConfig = String.format(RECORD_COUNT_FORMAT, taskAttemptContext.getTaskAttemptID());
      Map<String, String> metaData = new HashMap<>();
      metaData.put(GCSBatchSink.RECORD_COUNT, String.valueOf(configuration.getLong(keyInConfig, 0L)));
      StorageClient storageClient = getStorageClient(configuration);
      //update metadata on the output file present in the directory for this task
      Blob blob = storageClient.pickABlob(taskAttemptPath.toString());
      if (blob == null) {
        LOG.info("Could not find a file in path %s to apply count metadata.", taskAttemptPath.toString());
        return;
      }
      storageClient.setMetaData(blob, metaData);
    }

    @VisibleForTesting
    StorageClient getStorageClient(Configuration configuration) throws IOException {
      String project = configuration.get(GCPUtils.FS_GS_PROJECT_ID);
      String serviceAccountPath = configuration.get(GCPUtils.CLOUD_JSON_KEYFILE);
      return StorageClient.create(project, serviceAccountPath);
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
      delegate.abortTask(taskAttemptContext);
    }

    @Override
    public boolean isCommitJobRepeatable(JobContext jobContext) throws IOException {
      return delegate.isCommitJobRepeatable(jobContext);
    }

    @Override
    public boolean isRecoverySupported(JobContext jobContext) throws IOException {
      return delegate.isRecoverySupported(jobContext);
    }

    @Override
    public boolean isRecoverySupported() {
      return delegate.isRecoverySupported();
    }

    @Override
    public void recoverTask(TaskAttemptContext taskContext) throws IOException {
      delegate.recoverTask(taskContext);
    }
  }

  /**
   * RecordWriter for GCSSink
   */
  public static class GCSRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {

    private final RecordWriter originalWriter;
    private long recordCount;

    public GCSRecordWriter(RecordWriter originalWriter) {
      this.originalWriter = originalWriter;
    }

    @Override
    public void write(NullWritable nullWritable, StructuredRecord structuredRecord) throws IOException,
      InterruptedException {
      originalWriter.write(nullWritable, structuredRecord);
      recordCount++;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      originalWriter.close(taskAttemptContext);
      //Since the file details are not available here, pass the value on in configuration
      taskAttemptContext.getConfiguration()
        .setLong(String.format(RECORD_COUNT_FORMAT, taskAttemptContext.getTaskAttemptID()), recordCount);
    }
  }
}
