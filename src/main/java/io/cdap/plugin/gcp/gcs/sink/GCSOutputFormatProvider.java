/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.exception.ErrorDetailsProvider;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.gcp.common.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputFormatProvider for GCSSink
 */
public class GCSOutputFormatProvider implements ValidatingOutputFormat {
  public static final String DELEGATE_OUTPUTFORMAT_CLASSNAME = "gcssink.delegate.outputformat.classname";
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
  public static class GCSOutputFormat extends OutputFormat<NullWritable, StructuredRecord>
    implements ErrorDetailsProvider<Configuration> {
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

    @Override
    public RuntimeException getExceptionDetails(Throwable throwable, Configuration configuration) {
      return ExceptionUtils.getProgramFailureException(throwable);
    }
  }

  /**
   * RecordWriter for GCSSink
   */
  public static class GCSRecordWriter extends RecordWriter<NullWritable, StructuredRecord>
    implements ErrorDetailsProvider<Void> {

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

    @Override
    public RuntimeException getExceptionDetails(Throwable throwable, Void conf) {
      return ExceptionUtils.getProgramFailureException(throwable);
    }
  }
}
