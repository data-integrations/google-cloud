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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.DatasetId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Output Committer which creates and delegates operations to other Bigquery Output Committer instances.
 * <p>
 * Delegated instances are supplied along with a schema, which is used to configure the commit operation.
 */
public class DelegatingMultiSinkOutputCommitter extends OutputCommitter {
  private final Map<String, OutputCommitter> committerMap;
  private final Map<String, Schema> schemaMap;
  private final String projectName;
  private final String datasetName;
  private final String bucketName;
  private final String bucketPathUniqueId;

  public DelegatingMultiSinkOutputCommitter(String projectName,
                                            String datasetName,
                                            String bucketName,
                                            String bucketPathUniqueId) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.bucketName = bucketName;
    this.bucketPathUniqueId = bucketPathUniqueId;
    this.committerMap = new HashMap<>();
    this.schemaMap = new HashMap<>();
  }

  /**
   * Add a committer and schema to this instance.
   * <p>
   * The supplied committer and schema will be used when the commit operations are invoked.
   */
  public void addCommitterAndSchema(OutputCommitter committer,
                                    String tableName,
                                    Schema schema,
                                    TaskAttemptContext context) throws IOException, InterruptedException {
    committerMap.put(tableName, committer);
    schemaMap.put(tableName, schema);

    //Configure the supplied committer.
    committer.setupJob(context);
    committer.setupTask(context);
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
    for (String tableName : committerMap.keySet()) {
      configureContext(taskAttemptContext, tableName);

      committerMap.get(tableName).commitTask(taskAttemptContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (String tableName : committerMap.keySet()) {
      configureContext(jobContext, tableName);

      committerMap.get(tableName).commitJob(jobContext);
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    IOException ioe = null;

    for (OutputCommitter committer : committerMap.values()) {
      try {
        committer.abortTask(taskAttemptContext);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    IOException ioe = null;

    for (OutputCommitter committer : committerMap.values()) {
      try {
        committer.abortJob(jobContext, state);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }

  public void configureContext(JobContext context, String tableName) throws IOException {
    Schema schema = schemaMap.get(tableName);
    List<BigQueryTableFieldSchema> fields = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(schema);

    String gcsPath = BigQuerySinkUtils.getTemporaryGcsPath(bucketName, bucketPathUniqueId,  tableName);

    BigQuerySinkUtils.configureMultiSinkOutput(context.getConfiguration(),
                                               DatasetId.of(projectName, datasetName),
                                               tableName,
                                               gcsPath,
                                               fields);
  }
}
