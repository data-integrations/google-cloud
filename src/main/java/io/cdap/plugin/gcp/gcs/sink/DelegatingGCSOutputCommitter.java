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

package io.cdap.plugin.gcp.gcs.sink;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Output Committer which creates and delegates operations to other GCS Output Committer instances.
 *
 * Delegated instances are created based on a supplied Output Format and Destination Table Names.
 */
public class DelegatingGCSOutputCommitter extends OutputCommitter {
  private final Map<String, OutputCommitter> committerMap;

  public DelegatingGCSOutputCommitter() {
    committerMap = new HashMap<>();
  }

  /**
   * Add a new GCSOutputCommitter based on a supplied Output Format and Table Name.
   *
   * This GCS Output Committer gets initialized when created.
   */
  @SuppressWarnings("rawtypes")
  public void addGCSOutputCommitterFromOutputFormat(OutputFormat outputFormat,
                                                    TaskAttemptContext context,
                                                    String tableName) throws IOException, InterruptedException {
    //Set output directory
    context.getConfiguration().set(FileOutputFormat.OUTDIR,
                                   DelegatingGCSOutputUtils.buildOutputPath(context.getConfiguration(), tableName));

    //Wrap output committer into the GCS Output Committer.
    GCSOutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(context));

    //Initialize the new GCS Output Committer and add it to the Committer Map
    gcsOutputCommitter.setupJob(context);
    gcsOutputCommitter.setupTask(context);
    committerMap.put(tableName, gcsOutputCommitter);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    //no-op
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //no-op
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
  public void commitJob(JobContext jobContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.commitJob(jobContext);
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    IOException ioe = new IOException("Exception when aborting task.");

    for (OutputCommitter committer : committerMap.values()) {
      try {
        committer.abortTask(taskAttemptContext);
      } catch (IOException e) {
        ioe.addSuppressed(e);
      }
    }

    if (ioe.getSuppressed().length > 0) {
      throw ioe;
    }
  }
}
