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

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * An OutputFormat that filters records before sending them to a delegate
 */
public class RecordFilterOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  public static final String FILTER_FIELD = "record.filter.field";
  public static final String PASS_VALUE = "record.filter.val";
  public static final String ORIGINAL_SCHEMA = "record.original.schema";
  private static final String DELEGATE_CLASS = "filter.delegate";

  /**
   * Get the configuration required to filter out all records where the specified record field is not equal to the
   * specified pass through value.
   *
   * @param delegateClassName the class name of the delegate output format responsible for doing the actual write
   * @param filterField the record field to check
   * @param passThroughValue the value that the record field must have in order to be written
   * @param schema schema of the data to write
   */
  public static Map<String, String> configure(String delegateClassName, String filterField,
                                              String passThroughValue, Schema schema) {
    Map<String, String> config = new HashMap<>();
    config.put(DELEGATE_CLASS, delegateClassName);
    config.put(FILTER_FIELD, filterField);
    config.put(PASS_VALUE, passThroughValue);
    config.put(ORIGINAL_SCHEMA, schema.toString());
    return config;
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration hConf = context.getConfiguration();
    RecordWriter<NullWritable, StructuredRecord> delegate = getDelegateFormat(hConf).getRecordWriter(context);
    String filterField = hConf.get(FILTER_FIELD);
    String passthroughVal = hConf.get(PASS_VALUE);
    Schema schema = Schema.parseJson(hConf.get(ORIGINAL_SCHEMA));

    return new FilterRecordWriter(delegate, filterField, passthroughVal, schema);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    getDelegateFormat(context.getConfiguration()).checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return getDelegateFormat(context.getConfiguration()).getOutputCommitter(context);
  }

  private OutputFormat getDelegateFormat(Configuration hConf) throws IOException {
    String delegateClassName = hConf.get(DELEGATE_CLASS);
    try {
      Class<OutputFormat<NullWritable, StructuredRecord>> delegateClass =
        (Class<OutputFormat<NullWritable, StructuredRecord>>) hConf.getClassByName(delegateClassName);
      return delegateClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Unable to instantiate output format for class " + delegateClassName, e);
    }
  }

  /**
   * Filters records before writing them out using a delegate.
   */
  public static class FilterRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final String filterField;
    private final String passthroughValue;
    private final RecordWriter<NullWritable, StructuredRecord> delegate;
    private final Schema schema;

    FilterRecordWriter(RecordWriter<NullWritable, StructuredRecord> delegate, String filterField,
                       String passthroughValue, Schema schema) {
      this.filterField = filterField;
      this.passthroughValue = passthroughValue;
      this.delegate = delegate;
      this.schema = schema;
    }

    @Override
    public void write(NullWritable key, StructuredRecord record) throws IOException, InterruptedException {
      String val = record.get(filterField);
      if (!passthroughValue.equalsIgnoreCase(val)) {
        return;
      }

      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      for (Schema.Field field : record.getSchema().getFields()) {
        String fieldName = field.getName();
        if (filterField.equals(fieldName)) {
          continue;
        }
        recordBuilder.set(fieldName, record.get(fieldName));
      }
      delegate.write(key, recordBuilder.build());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.close(context);
    }
  }
}
