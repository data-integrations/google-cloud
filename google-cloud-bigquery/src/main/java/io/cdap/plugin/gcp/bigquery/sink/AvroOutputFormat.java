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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * avro output format.
 */
public class AvroOutputFormat extends AvroKeyOutputFormat<GenericRecord> {
  public AvroOutputFormat() {
    super();
  }

  public AvroOutputFormat(RecordWriterFactory recordWriterFactory) {
    super(recordWriterFactory);
  }

  /**
   *
   * @param context
   * @return
   * @throws IOException
   */
  public RecordWriter<AvroKey<GenericRecord>, NullWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException {
    Configuration conf = context.getConfiguration();

    // Get the writer schema.
    Schema writerSchema = AvroJob.getOutputKeySchema(conf);
    boolean isMapOnly = context.getNumReduceTasks() == 0;
    if (isMapOnly) {
      Schema mapOutputSchema = AvroJob.getMapOutputKeySchema(conf);
      if (mapOutputSchema != null) {
        writerSchema = mapOutputSchema;
      }
    }

    GenericData dataModel = AvroSerialization.createDataModel(conf);
    return create(writerSchema, dataModel, getCompressionCodec(context),
                  getAvroFileOutputStream(context), getSyncInterval(context));
  }

  /**
   * Creates a new record writer instance.
   *
   * @param writerSchema The writer schema for the records to write.
   * @param compressionCodec The compression type for the writer file.
   * @param outputStream The target output stream for the records.
   * @param syncInterval The sync interval for the writer file.
   */
  private RecordWriter<AvroKey<GenericRecord>, NullWritable> create(
    Schema writerSchema, GenericData dataModel, CodecFactory compressionCodec,
    OutputStream outputStream, int syncInterval) throws IOException {
    return new AvroRecordWriter(writerSchema, dataModel, compressionCodec, outputStream, syncInterval);
  }
}
