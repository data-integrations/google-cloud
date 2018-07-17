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
import com.google.cloud.spanner.ValueBinder;
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
 *
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

  @Override
  public void write(NullWritable nullWritable, StructuredRecord record)
    throws IOException, InterruptedException {
    if (!initialized) {
      initialized = true;
      initialize();
    }
    Mutation.WriteBuilder builder = Mutation.newInsertBuilder(tableName);
    List<Schema.Field> fields = record.getSchema().getFields();
    for (Schema.Field field : fields) {
      ValueBinder<Mutation.WriteBuilder> value = builder.set(field.getName());
      if (field.getSchema().getType().isSimpleType()) {
        Schema.Type type = field.getSchema().getType();
        switch(type) {
          case INT:
            value.to(record.<Integer>get(field.getName()));
            break;

          case FLOAT:
            value.to(record.<Float>get(field.getName()));
            break;

          case BOOLEAN:
            value.to(record.<Boolean>get(field.getName()));
            break;

          case STRING:
            value.to(record.<String>get(field.getName()));
            break;

          case LONG:
            value.to(record.<Long>get(field.getName()));
            break;

          case DOUBLE:
            value.to(record.<Double>get(field.getName()));
            break;

          default:
            throw new IOException("Type currently not supported.");
        }

      }
    }
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(builder.build());
    client.write(mutations);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    // nothing to close.
  }
}
