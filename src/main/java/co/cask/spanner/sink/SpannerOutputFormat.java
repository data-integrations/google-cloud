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

package co.cask.spanner.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcs.GCPUtil;
import co.cask.spanner.SpannerConstants;
import co.cask.spanner.common.SpannerUtil;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Spanner output format
 */
public class SpannerOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerOutputFormat.class);

  /**
   * Get properties from SparkSinkConfig and store them as properties in Configuration
   * @param configuration
   * @param config
   */
  public static void configure(Configuration configuration, SpannerSinkConfig config) {
    String projectId = GCPUtil.getProjectId(config.project);
    configuration.set(SpannerConstants.PROJECT_ID, projectId);
    if (config.serviceFilePath != null) {
      configuration.set(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH, config.serviceFilePath);
    }
    configuration.set(SpannerConstants.INSTANCE_ID, config.instance);
    configuration.set(SpannerConstants.DATABASE, config.database);
    configuration.set(SpannerConstants.TABLE_NAME, config.table);
    configuration.set(SpannerConstants.SPANNER_WRITE_BATCH_SIZE, String.valueOf(config.getBatchSize()));
    configuration.set(SpannerConstants.SCHEMA, config.getSchema().toString());
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    String projectId = configuration.get(SpannerConstants.PROJECT_ID);
    String instanceId = configuration.get(SpannerConstants.INSTANCE_ID);
    String database = configuration.get(SpannerConstants.DATABASE);
    String serviceFilePath = configuration.get(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH);
    String tableName = configuration.get(SpannerConstants.TABLE_NAME);
    Schema schema = Schema.parseJson(configuration.get(SpannerConstants.SCHEMA));
    Spanner spanner = SpannerUtil.getSpannerService(serviceFilePath, projectId);
    int batchSize = Integer.parseInt(configuration.get(SpannerConstants.SPANNER_WRITE_BATCH_SIZE));
    DatabaseId db = DatabaseId.of(projectId, instanceId, database);
    DatabaseClient client = spanner.getDatabaseClient(db);
    return new SpannerRecordWriter(spanner, tableName, client, batchSize, schema);
  }

  /**
   * Spanner record writer that buffers mutations and writes to spanner
   */
  protected static class SpannerRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final Spanner spanner;
    private final String tableName;
    private final DatabaseClient databaseClient;
    private final List<Mutation> mutations;
    private final int batchSize;
    private final Schema schema;

    public SpannerRecordWriter(Spanner spanner, String tableName, DatabaseClient client, int batchSize,
                               Schema schema) {
      this.spanner = spanner;
      this.tableName = tableName;
      this.databaseClient = client;
      this.mutations = new ArrayList<>();
      this.batchSize = batchSize;
      this.schema = schema;
    }

    @Override
    public void write(NullWritable nullWritable, StructuredRecord record) throws IOException, InterruptedException {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        Schema fieldSchema = field.getSchema();
        Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        String name = field.getName();
        switch(type) {
          case BOOLEAN:
            builder.set(name).to(record.<Boolean>get(name));
            break;
          case STRING:
            builder.set(name).to(record.<String>get(name));
            break;
          case LONG:
            builder.set(name).to(record.<Long>get(name));
            break;
          case DOUBLE:
            builder.set(name).to(record.<Double>get(name));
            break;
          case BYTES:
            byte[] byteArray = record.get(name);
            builder.set(name).to(ByteArray.copyFrom(byteArray));
            break;
          // todo CDAP-14233 - add support for array, date and timestamp
          default:
            throw new IOException(type.name() + " : Type currently not supported.");
        }
      }
      mutations.add(builder.build());
      if (mutations.size() > batchSize) {
        databaseClient.write(mutations);
        mutations.clear();
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      if (mutations.size() > 0) {
        databaseClient.write(mutations);
        mutations.clear();
      }
      spanner.close();
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
  }

  /**
   * No op output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }
    };
  }
}
