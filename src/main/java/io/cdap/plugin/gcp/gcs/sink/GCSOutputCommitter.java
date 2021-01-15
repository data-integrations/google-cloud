/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.StorageClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputCommitter for GCS
 */
public class GCSOutputCommitter extends OutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputFormatProvider.class);
  public static final String RECORD_COUNT_FORMAT = "recordcount.%s";

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
      LOG.info("Could not find a file in path {} to apply count metadata.", taskAttemptPath.toString());
      return;
    }
    blob.toBuilder().setContentType(configuration.get(GCSBatchSink.CONTENT_TYPE)).setMetadata(metaData).build()
      .update();
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
