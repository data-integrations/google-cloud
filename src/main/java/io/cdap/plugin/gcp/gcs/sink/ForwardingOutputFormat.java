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
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.plugin.gcp.common.ExceptionUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.IOException;

/**
 * ForwardingOutputFormat which delegates all operations to another OutputFormat.
 * <p>
 * This is used to wrap the delegate output format and
 * throw {@link io.cdap.cdap.api.exception.ProgramFailureException} from IOException.</p>
 */
public class ForwardingOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  private OutputFormat delegate;

  private OutputFormat getDelegateFormatInstance(Configuration configuration) {
    if (delegate != null) {
      return delegate;
    }

    String delegateClassName = configuration.get(
      GCPUtils.WRAPPED_OUTPUTFORMAT_CLASSNAME);
    try {
      delegate = (OutputFormat) ReflectionUtils
        .newInstance(configuration.getClassByName(delegateClassName), configuration);
      return delegate;
    } catch (ClassNotFoundException e) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Unable to instantiate output format for class '%s'.", delegateClassName),
        e.getMessage(), ErrorType.SYSTEM, false, e);
    }
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter (
    TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(
      () -> getDelegateFormatInstance(
        taskAttemptContext.getConfiguration()).getRecordWriter(taskAttemptContext));
  }

  @Override
  public void checkOutputSpecs (JobContext jobContext) throws IOException, InterruptedException {
    ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(
      () -> getDelegateFormatInstance(jobContext.getConfiguration()).checkOutputSpecs(jobContext));
  }

  @Override
  public OutputCommitter getOutputCommitter (TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(
      () -> getDelegateFormatInstance(
        taskAttemptContext.getConfiguration()).getOutputCommitter(taskAttemptContext));
  }
}
