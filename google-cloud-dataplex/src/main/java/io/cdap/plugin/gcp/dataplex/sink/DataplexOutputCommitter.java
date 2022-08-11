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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * OutputCommitter for Dataplex Sink Plugin
 */
public class DataplexOutputCommitter extends OutputCommitter {

  private final OutputCommitter delegate;

  public DataplexOutputCommitter(OutputCommitter delegate) {
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
    delegate.commitTask(taskAttemptContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    delegate.abortTask(taskAttemptContext);
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    delegate.recoverTask(taskContext);
  }
}
