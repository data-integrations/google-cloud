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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Record Writer which delegates writes to other Record Writers based on the record's Table name.
 * <p>
 * This Record Writer will initialize record writes and Output Committers as needed.
 */
public class DelegatingMultiSinkRecordWriter extends RecordWriter<StructuredRecord, NullWritable> {

  private final TaskAttemptContext initialContext;
  private final String tableNameField;
  private final String bucketName;
  private final String bucketPathUniqueId;
  private final DatasetId datasetId;
  private final Map<String, RecordWriter<StructuredRecord, NullWritable>> delegateMap;
  private final DelegatingMultiSinkOutputCommitter delegatingOutputCommitter;

  public DelegatingMultiSinkRecordWriter(TaskAttemptContext initialContext,
                                         String tableNameField,
                                         String bucketName,
                                         String bucketPathUniqueId,
                                         DatasetId datasetId,
                                         DelegatingMultiSinkOutputCommitter delegatingMultiSinkOutputCommitter) {
    this.initialContext = initialContext;
    this.tableNameField = tableNameField;
    this.bucketName = bucketName;
    this.bucketPathUniqueId = bucketPathUniqueId;
    this.datasetId = datasetId;
    this.delegateMap = new HashMap<>();
    this.delegatingOutputCommitter = delegatingMultiSinkOutputCommitter;
  }

  @Override
  public void write(StructuredRecord key, NullWritable value) throws IOException, InterruptedException {
    String tableName = key.get(tableNameField);

    RecordWriter<StructuredRecord, NullWritable> delegate;

    if (delegateMap.containsKey(tableName)) {
      delegate = delegateMap.get(tableName);
    } else {
      delegate = getRecordWriterDelegate(tableName, key.getSchema());
    }

    delegate.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    for (RecordWriter<StructuredRecord, NullWritable> delegate : delegateMap.values()) {
      delegate.close(context);
    }

    // The task attempt context at this stage doesn't have all of the configuration properties we need to properly
    // execute the commit job step. For this reason, we use the original context instance that was used when
    // creating this record writer.
    delegatingOutputCommitter.commitTask(initialContext);
    delegatingOutputCommitter.commitJob(initialContext);
  }

  /**
   * Gets a new Record Writer Delegate instance for the specified table name and record schema.
   */
  public RecordWriter<StructuredRecord, NullWritable> getRecordWriterDelegate(String tableName, Schema schema)
    throws IOException, InterruptedException {
    // Configure output.
    List<BigQueryTableFieldSchema> fields = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(schema);

    String gcsPath = BigQuerySinkUtils.getTemporaryGcsPath(bucketName, bucketPathUniqueId,  tableName);

    BigQuerySinkUtils.configureMultiSinkOutput(initialContext.getConfiguration(),
                                               datasetId,
                                               tableName,
                                               gcsPath,
                                               fields);

    BigQueryOutputFormat bqOutputFormat = new BigQueryOutputFormat();

    // Get output committer instance for the current table and add it to the delegating Output Committer.
    OutputCommitter bqOutputCommitter = bqOutputFormat.getOutputCommitter(initialContext);
    delegatingOutputCommitter.addCommitterAndSchema(bqOutputCommitter, tableName, schema, initialContext);

    // Get record writer instance and add it to the delegate map.
    RecordWriter<StructuredRecord, NullWritable> delegate = bqOutputFormat.getRecordWriter(initialContext, schema);
    delegateMap.put(tableName, delegate);

    return delegate;
  }
}
