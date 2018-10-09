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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Filters out records unless the value of a specific field is a specific value.
 */
public class FilterOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  private static final String FILTER_FIELD = "record.filter.field";
  private static final String PASS_VALUE = "record.filter.val";
  private IndirectBigQueryOutputFormat<JsonObject, NullWritable> delegateFormat;

  static void configure(Configuration conf, String fieldName, String requiredVal) {
    conf.set(FILTER_FIELD, fieldName);
    conf.set(PASS_VALUE, requiredVal);
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String fieldName = conf.get(FILTER_FIELD);
    String requiredVal = conf.get(PASS_VALUE);
    RecordWriter<JsonObject, NullWritable> delegate = getDelegateFormat().getRecordWriter(taskAttemptContext);
    return new FilterRecordWriter(delegate, fieldName, requiredVal);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException {
    getDelegateFormat().checkOutputSpecs(jobContext);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException {
    return getDelegateFormat().getOutputCommitter(taskAttemptContext);
  }

  private IndirectBigQueryOutputFormat<JsonObject, NullWritable> getDelegateFormat() {
    if (delegateFormat != null) {
      return delegateFormat;
    }
    delegateFormat = new IndirectBigQueryOutputFormat<>();
    return delegateFormat;
  }

  /**
   * Record writer that ignores records unless the field value matches the required value. Also transforms
   * StructuredRecords into JsonObjects as required by the underlying record writer.
   */
  public static class FilterRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final RecordWriter<JsonObject, NullWritable> delegate;
    private final StructuredRecordToJsonTransformer transformer;
    private final String field;
    private final String val;

    FilterRecordWriter(RecordWriter<JsonObject, NullWritable> delegate, String field, String val) {
      this.delegate = delegate;
      this.transformer = new StructuredRecordToJsonTransformer(field);
      this.field = field;
      this.val = val;
    }

    @Override
    public void write(NullWritable nullWritable, StructuredRecord structuredRecord)
      throws IOException, InterruptedException {
      if (!val.equals(structuredRecord.get(field))) {
        return;
      }

      JsonObject object = transformer.transform(structuredRecord);
      delegate.write(object, NullWritable.get());
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      delegate.close(taskAttemptContext);
    }
  }
}
