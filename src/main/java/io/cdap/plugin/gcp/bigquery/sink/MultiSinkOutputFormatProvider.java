/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Provides {@link MultiSinkOutputFormatDelegate} to output values for multiple tables.
 */
public class MultiSinkOutputFormatProvider implements OutputFormatProvider {

  private static final String FILTER_FIELD = "bq.multi.record.filter.field";
  private static final String FILTER_VALUE = "bq.multi.record.filter.value";

  private final Configuration config;

  public MultiSinkOutputFormatProvider(Configuration config,
                                       String tableName,
                                       Schema tableSchema,
                                       String filterField) {
    this.config = new Configuration(config);
    this.config.set(FILTER_VALUE, tableName);
    this.config.set(FILTER_FIELD, filterField);
    this.config.set(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA, tableSchema.toString());
  }

  @Override
  public String getOutputFormatClassName() {
    return MultiSinkOutputFormatDelegate.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> map = BigQueryUtil.configToMap(config);
    map.put(org.apache.hadoop.mapred.JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
    return map;
  }

  /**
   * Uses {@link BigQueryOutputFormat} as delegate and creates {@link FilterRecordWriter}
   * to output values based on filter and its value and schema.
   */
  public static class MultiSinkOutputFormatDelegate extends OutputFormat<StructuredRecord, NullWritable> {

    private final OutputFormat<StructuredRecord, NullWritable> delegate;

    public MultiSinkOutputFormatDelegate() {
      this.delegate = new BigQueryOutputFormat();
    }

    @Override
    public RecordWriter<StructuredRecord, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      Configuration conf = taskAttemptContext.getConfiguration();
      String filterField = conf.get(FILTER_FIELD);
      String filterValue = conf.get(FILTER_VALUE);
      Schema schema = Schema.parseJson(conf.get(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA));
      @SuppressWarnings("unchecked")
      RecordWriter<StructuredRecord, NullWritable> recordWriter = delegate.getRecordWriter(taskAttemptContext);
      return new FilterRecordWriter(filterField, filterValue, schema, recordWriter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
      delegate.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      return delegate.getOutputCommitter(taskAttemptContext);
    }
  }

  /**
   * Filters records before writing them out using a delegate based on filter and its value and given schema.
   */
  public static class FilterRecordWriter extends RecordWriter<StructuredRecord, NullWritable> {

    private final String filterField;
    private final String filterValue;
    private final Schema schema;
    private final RecordWriter<StructuredRecord, NullWritable> delegate;


    public FilterRecordWriter(String filterField,
                              String filterValue,
                              Schema schema,
                              RecordWriter<StructuredRecord, NullWritable> delegate) {
      this.filterField = filterField;
      this.filterValue = filterValue;
      this.schema = schema;
      this.delegate = delegate;
    }

    @Override
    public void write(StructuredRecord key, NullWritable value) throws IOException, InterruptedException {
      Object objectValue = key.get(filterField);
      if (objectValue == null) {
        return;
      }
      String name = (String) objectValue;
      String[] split = name.split("\\.");
      if (split.length == 2) {
        name = split[1];
      }
      if (!filterValue.equalsIgnoreCase(name)) {
        return;
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);

      key.getSchema().getFields().stream()
        .filter(entry -> !filterField.equals(entry.getName()))
        .filter(entry -> schema.getField(entry.getName()) != null)
        .forEach(entry -> builder.set(entry.getName(), key.get(entry.getName())));

      delegate.write(builder.build(), value);
    }

    private org.apache.avro.Schema getAvroSchema(Schema cdapSchema) {
      return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.close(context);
    }
  }
}
