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

package io.cdap.plugin.gcp.gcs.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ErrorDetailsProvider;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.InputFiles;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.gcp.common.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * InputFormatProvider for GCSSource.
 */
public class GCSInputFormatProvider implements ValidatingInputFormat {
  public static final String DELEGATE_INPUTFORMAT_CLASSNAME =
    "gcssource.delegate.inputformat.classname";
  private final ValidatingInputFormat delegate;

  public GCSInputFormatProvider(ValidatingInputFormat delegate) {
    this.delegate = delegate;
  }

  @Override
  public void validate(FormatContext context) {
    delegate.validate(context);
  }

  @Override
  public @Nullable Schema getSchema(FormatContext formatContext) {
    return delegate.getSchema(formatContext);
  }

  @Override
  public @Nullable Schema detectSchema(FormatContext context, InputFiles inputFiles)
    throws IOException {
    return ValidatingInputFormat.super.detectSchema(context, inputFiles);
  }

  @Override
  public String getInputFormatClassName() {
    return GCSInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Map<String, String> inputFormatConfiguration =
      new HashMap<>(delegate.getInputFormatConfiguration());
    inputFormatConfiguration.put(DELEGATE_INPUTFORMAT_CLASSNAME,
      delegate.getInputFormatClassName());
    return inputFormatConfiguration;
  }

  /**
   * InputFormat for GCSSource.
   */
  public static class GCSInputFormat extends InputFormat<NullWritable, StructuredRecord>
    implements ErrorDetailsProvider<Configuration> {
    private InputFormat delegateFormat;

    private InputFormat getDelegateFormatInstance(Configuration configuration) throws IOException {
      if (delegateFormat != null) {
        return delegateFormat;
      }

      String delegateClassName = configuration.get(DELEGATE_INPUTFORMAT_CLASSNAME);
      try {
        delegateFormat = (InputFormat) ReflectionUtils
          .newInstance(configuration.getClassByName(delegateClassName), configuration);
        return delegateFormat;
      } catch (ClassNotFoundException e) {
        throw new IOException(
          String.format("Unable to instantiate output format for class %s", delegateClassName),
          e);
      }
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
      return getDelegateFormatInstance(jobContext.getConfiguration()).getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      RecordReader originalReader =
        getDelegateFormatInstance(taskAttemptContext.getConfiguration())
        .createRecordReader(inputSplit, taskAttemptContext);
      return new GCSRecordReader(originalReader);
    }

    @Override
    public RuntimeException getExceptionDetails(Throwable throwable, Configuration configuration) {
      return ExceptionUtils.getProgramFailureException(throwable);
    }
  }

  /**
   * RecordReader for GCSSource.
   */
  public static class GCSRecordReader extends RecordReader<NullWritable, StructuredRecord>
    implements ErrorDetailsProvider<Void> {

    private final RecordReader<NullWritable, StructuredRecord> originalReader;

    public GCSRecordReader(RecordReader<NullWritable, StructuredRecord> originalReader) {
      this.originalReader = originalReader;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      originalReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return originalReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return originalReader.getCurrentKey();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      return originalReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return originalReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      originalReader.close();
    }

    @Override
    public RuntimeException getExceptionDetails(Throwable throwable, Void conf) {
      return ExceptionUtils.getProgramFailureException(throwable);
    }
  }
}
