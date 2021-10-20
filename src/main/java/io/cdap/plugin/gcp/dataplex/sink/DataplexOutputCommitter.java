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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputCommitter for Dataplex
 */
public class DataplexOutputCommitter extends OutputCommitter {

  public static final String RECORD_COUNT_FORMAT = "recordcount.%s";
  // Creating committer map to commit every committer jo separately for hive style partitioning for GCS Bucket assets.
  private final Map<String, OutputCommitter> committerMap;

  public DataplexOutputCommitter() {
    committerMap = new HashMap<>();
  }

  public void addDataplexOutputCommitterFromOutputFormat(OutputCommitter outputCommitter,
                                                         TaskAttemptContext context)
    throws IOException, InterruptedException {
    outputCommitter.setupJob(context);
    outputCommitter.setupTask(context);
    String assetType = context.getConfiguration().get(DataplexOutputFormatProvider.DATAPLEX_ASSET_TYPE);
    // Only one committer is required for BigQuery otherwise it is trying to delete the temporary data twice while
    // clean up.
    if (assetType.equalsIgnoreCase(DataplexBatchSink.BIGQUERY_DATASET_ASSET_TYPE) && !committerMap.isEmpty()) {
      return;
    }
    committerMap.put(DataplexOutputFormatProvider.PARTITION_PREFIX + context.getTaskAttemptID().toString(),
      outputCommitter);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.setupJob(jobContext);
    }
  }

  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.cleanupJob(jobContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.commitJob(jobContext);
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.abortJob(jobContext, state);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.setupTask(taskAttemptContext);
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    if (committerMap.isEmpty()) {
      return false;
    }
    boolean needsTaskCommit = true;
    for (OutputCommitter committer : committerMap.values()) {
      needsTaskCommit = needsTaskCommit && committer.needsTaskCommit(taskAttemptContext);
    }
    return needsTaskCommit;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.commitTask(taskAttemptContext);
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.abortTask(taskAttemptContext);
    }
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.recoverTask(taskContext);
    }
  }
}

