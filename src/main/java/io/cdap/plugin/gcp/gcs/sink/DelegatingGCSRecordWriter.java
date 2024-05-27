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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Record Writer which delegates writes to other Record Writers based on the record's Table name.
 * <p>
 * This Record Writer will initialize record writes and Output Committers as needed.
 */
public class DelegatingGCSRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private final TaskAttemptContext context;
  private final String partitionField;
  private final Map<String, RecordWriter<NullWritable, StructuredRecord>> delegateMap;
  private final DelegatingGCSOutputCommitter delegatingGCSOutputCommitter;

  DelegatingGCSRecordWriter(TaskAttemptContext context,
                            String partitionField,
                            DelegatingGCSOutputCommitter delegatingGCSOutputCommitter) {
    this.context = context;
    this.partitionField = partitionField;
    this.delegateMap = new HashMap<>();
    this.delegatingGCSOutputCommitter = delegatingGCSOutputCommitter;
  }

  @Override
  public void write(NullWritable key, StructuredRecord record) throws IOException, InterruptedException {
    String tableName = record.get(partitionField);

    RecordWriter<NullWritable, StructuredRecord> delegate;

    if (delegateMap.containsKey(tableName)) {
      delegate = delegateMap.get(tableName);
    } else {
      //Get output format from configuration.
      OutputFormat<NullWritable, StructuredRecord> format =
        DelegatingGCSOutputUtils.getDelegateFormat(context.getConfiguration());

      //Initialize GCS Output Committer for this format.
      delegatingGCSOutputCommitter.addGCSOutputCommitterFromOutputFormat(format, tableName);

      //Add record writer to delegate map.
      delegate = format.getRecordWriter(context);
      delegateMap.put(tableName, delegate);
    }

    delegate.write(key, record);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    //Close all delegates
    for (RecordWriter<NullWritable, StructuredRecord> delegate : delegateMap.values()) {
      delegate.close(context);
    }
  }

}
