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

import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.RecordConverter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * BigQueryRecordWriter picks the  {@link RecordConverter} based on output file format
 */
public class BigQueryRecordWriter extends RecordWriter<StructuredRecord, NullWritable> {

  private final RecordWriter delegate;
  private final BigQueryFileFormat fileFormat;
  private final Schema outputSchema;
  private RecordConverter recordConverter;
  private Set<String> jsonStringFieldsPaths;

  public BigQueryRecordWriter(RecordWriter delegate, BigQueryFileFormat fileFormat, @Nullable Schema outputSchema,
                              Set<String> jsonStringFieldsPaths) {
    this.delegate = delegate;
    this.fileFormat = fileFormat;
    this.outputSchema = outputSchema;
    this.jsonStringFieldsPaths = jsonStringFieldsPaths;
    initRecordConverter();
  }

  private void initRecordConverter() {
    if (this.fileFormat == BigQueryFileFormat.NEWLINE_DELIMITED_JSON) {
      recordConverter = new BigQueryJsonConverter(jsonStringFieldsPaths);
      return;
    }
    recordConverter = new BigQueryAvroConverter();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(StructuredRecord structuredRecord, NullWritable nullWriter) throws IOException,
    InterruptedException {
    delegate.write(recordConverter.transform(structuredRecord, outputSchema), nullWriter);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    delegate.close(taskAttemptContext);
  }
}
