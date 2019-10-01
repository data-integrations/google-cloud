/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.gcp.spanner.sink;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Spanner output format
 */
public class SpannerOutputFormat extends OutputFormat<NullWritable, Mutation> {

  /**
   * Get properties from SparkSinkConfig and store them as properties in Configuration
   *
   * @param configuration the Hadoop configuration to set the properties in
   * @param config        the spanner configuration
   * @param schema        schema for spanner table
   */
  public static void configure(Configuration configuration, SpannerSinkConfig config, Schema schema) {
    String projectId = config.getProject();
    configuration.set(SpannerConstants.PROJECT_ID, projectId);
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      configuration.set(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH, serviceAccountFilePath);
    }
    configuration.set(SpannerConstants.INSTANCE_ID, config.getInstance());
    configuration.set(SpannerConstants.DATABASE, config.getDatabase());
    configuration.set(SpannerConstants.TABLE_NAME, config.getTable());
    configuration.set(SpannerConstants.SPANNER_WRITE_BATCH_SIZE, String.valueOf(config.getBatchSize()));
    configuration.set(SpannerConstants.SCHEMA, schema.toString());
  }

  @Override
  public RecordWriter<NullWritable, Mutation> getRecordWriter(TaskAttemptContext context)
    throws IOException {
    Configuration configuration = context.getConfiguration();
    String projectId = configuration.get(SpannerConstants.PROJECT_ID);
    String instanceId = configuration.get(SpannerConstants.INSTANCE_ID);
    String database = configuration.get(SpannerConstants.DATABASE);
    String serviceFilePath = configuration.get(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH);
    Spanner spanner = SpannerUtil.getSpannerService(serviceFilePath, projectId);
    int batchSize = Integer.parseInt(configuration.get(SpannerConstants.SPANNER_WRITE_BATCH_SIZE));
    DatabaseId db = DatabaseId.of(projectId, instanceId, database);
    DatabaseClient client = spanner.getDatabaseClient(db);
    return new SpannerRecordWriter(spanner, client, batchSize);
  }

  /**
   * Spanner record writer that buffers mutations and writes to spanner
   */
  protected static class SpannerRecordWriter extends RecordWriter<NullWritable, Mutation> {
    private final Spanner spanner;
    private final DatabaseClient databaseClient;
    private final List<Mutation> mutations;
    private final int batchSize;

    public SpannerRecordWriter(Spanner spanner, DatabaseClient client, int batchSize) {
      this.spanner = spanner;
      this.databaseClient = client;
      this.mutations = new ArrayList<>();
      this.batchSize = batchSize;
    }

    @Override
    public void write(NullWritable nullWritable, Mutation mutation) {
      mutations.add(mutation);
      if (mutations.size() > batchSize) {
        databaseClient.write(mutations);
        mutations.clear();
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      try {
        if (mutations.size() > 0) {
          databaseClient.write(mutations);
          mutations.clear();
        }
      } finally {
        spanner.close();
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  /**
   * No op output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
