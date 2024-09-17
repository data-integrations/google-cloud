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

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.plugin.gcp.common.ExceptionUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * ForwardingOutputCommitter which delegates all operations to another OutputCommitter.
 * <p>
 * This is used to wrap the OutputCommitter of the delegate format and
 * throw {@link io.cdap.cdap.api.exception.ProgramFailureException} from IOException.</p>
 */
public class ForwardingOutputCommitter extends OutputCommitter {
  private final OutputCommitter delegate;

  public ForwardingOutputCommitter(OutputCommitter delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    ExceptionUtils.invokeWithProgramFailureHandling(() -> delegate.setupJob(jobContext));
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    ExceptionUtils.invokeWithProgramFailureHandling(() -> delegate.setupTask(taskAttemptContext));
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return ExceptionUtils.invokeWithProgramFailureHandling(
      () -> delegate.needsTaskCommit(taskAttemptContext));
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    ExceptionUtils.invokeWithProgramFailureHandling(() -> delegate.commitTask(taskAttemptContext));
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    ExceptionUtils.invokeWithProgramFailureHandling(() -> delegate.abortTask(taskAttemptContext));
  }

  @SuppressWarnings("rawtypes")
  public void addGCSOutputCommitterFromOutputFormat(OutputFormat outputFormat, String tableName)
    throws InterruptedException, IOException {
    if (delegate instanceof DelegatingGCSOutputCommitter) {
      ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(() ->
        ((DelegatingGCSOutputCommitter) delegate).addGCSOutputCommitterFromOutputFormat(
          outputFormat, tableName));
    } else {
      throw ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Operation is not supported in the output committer: '%s'.",
          delegate.getClass().getName()), null, ErrorType.SYSTEM, false, null
      );
    }
  }
}
