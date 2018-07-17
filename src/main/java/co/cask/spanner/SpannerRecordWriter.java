/*
 * Copyright © 2018 Google Inc.
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

package co.cask.spanner;

import co.cask.bigquery.BigQuerySource;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class <code>SpannerRecordWriter</code> is Hadoop output format for writing records
 * to Spanner.
 */
public class SpannerRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerRecordWriter.class);
  private String projectId;
  private String instanceId;
  private String database;
  private String serviceFilePath;
  private String tableName;
  private DatabaseClient client;
  private boolean initialized;

  public SpannerRecordWriter(Configuration configuration) throws IOException  {
    projectId = configuration.get(SpannerConstants.PROJECT_ID);
    instanceId = configuration.get(SpannerConstants.INSTANCE_ID);
    database = configuration.get(SpannerConstants.DATABASE);
    serviceFilePath = configuration.get(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH);
    tableName = configuration.get(SpannerConstants.TABLE_NAME);
  }

  /**
   * Reads the service credentials from the file and initialize the <code>Spanner</code>
   * @throws IOException if there is issue reading the file.
   */
  private void initialize() throws IOException {
    GoogleCredentials credentials = null;
    try {
      credentials = BigQuerySource.loadServiceAccountCredentials(serviceFilePath);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }

    SpannerOptions options = SpannerOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build();
    Spanner spanner = options.getService();
    DatabaseId db = DatabaseId.of(projectId, instanceId, database);
    client = spanner.getDatabaseClient(db);
  }

  /**
   * Invoked by the framework to write data to spanner converts from {@link StructuredRecord}
   * to a {@link Mutation}.
   *
   * @param nullWritable Null key
   * @param record {@link StructuredRecord} to be written to Spanner.
   * @throws IOException throw when issue writing records to spanner.
   */
  @Override
  public void write(NullWritable nullWritable, StructuredRecord record)
    throws IOException, InterruptedException {
    if (!initialized) {
      initialized = true;
      initialize();
    }

    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
    List<Schema.Field> fields = record.getSchema().getFields();
    List<Mutation> mutations = new ArrayList<>();
    for (Schema.Field field : fields) {
      Schema.Type type = field.getSchema().getNonNullable().getType();
      String name = field.getName();
      switch(type) {
        case INT:
          builder.set(name).to(record.<Integer>get(name));
          break;

        case FLOAT:
          builder.set(name).to(record.<Float>get(name));
          break;

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

        default:
          throw new IOException(type.name() + " : Type currently not supported.");
      }
    }
    mutations.add(builder.build());
    client.write(mutations);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    // nothing to close.
  }
}
