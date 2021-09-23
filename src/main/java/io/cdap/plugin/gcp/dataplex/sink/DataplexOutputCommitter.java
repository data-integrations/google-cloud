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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.StorageClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OutputCommitter for Dataplex
 */
public class DataplexOutputCommitter extends OutputCommitter {

  public static final String RECORD_COUNT_FORMAT = "recordcount.%s";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexOutputFormatProvider.class);
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


  @VisibleForTesting
  StorageClient getStorageClient(Configuration configuration) throws IOException {
    String project = configuration.get(GCPUtils.FS_GS_PROJECT_ID);
    String serviceAccount = null;
    boolean isServiceAccountFile = GCPUtils.SERVICE_ACCOUNT_TYPE_FILE_PATH
      .equals(configuration.get(GCPUtils.SERVICE_ACCOUNT_TYPE));
    if (isServiceAccountFile) {
      serviceAccount = configuration.get(GCPUtils.CLOUD_JSON_KEYFILE, null);
    } else {
      serviceAccount = configuration.get(String.format("%s.%s", GCPUtils.CLOUD_JSON_KEYFILE_PREFIX,
        GCPUtils.CLOUD_ACCOUNT_JSON_SUFFIX));
    }
    return StorageClient.create(project, serviceAccount, isServiceAccountFile);
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

