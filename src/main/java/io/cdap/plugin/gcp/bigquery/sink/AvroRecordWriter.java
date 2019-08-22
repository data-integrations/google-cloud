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
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.Syncable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/**
 * avro record writer
 */
public class AvroRecordWriter extends RecordWriter<AvroKey<GenericRecord>, NullWritable> implements Syncable {
  /** A writer for the Avro container file. */
  private DataFileWriter<GenericRecord> mAvroFileWriter;
  private Schema prevSchema;
  private GenericData dataModel;
  private CodecFactory compressionCodec;
  private OutputStream outputStream;
  private int syncInterval;

  /**
   * Constructor.
   *
   * @param writerSchema The writer schema for the records in the Avro container file.
   * @param compressionCodec A compression codec factory for the Avro container file.
   * @param outputStream The output stream to write the Avro container file to.
   * @param syncInterval The sync interval for the Avro container file.
   * @throws IOException If the record writer cannot be opened.
   */
  public AvroRecordWriter(Schema writerSchema, GenericData dataModel, CodecFactory compressionCodec,
                          OutputStream outputStream, int syncInterval) throws IOException {
    this.dataModel = dataModel;
    this.compressionCodec = compressionCodec;
    this.outputStream = outputStream;
    this.syncInterval = syncInterval;
  }
  /**
   * Constructor.
   *
   * @param writerSchema The writer schema for the records in the Avro container file.
   * @param compressionCodec A compression codec factory for the Avro container file.
   * @param outputStream The output stream to write the Avro container file to.
   * @throws IOException If the record writer cannot be opened.
   */
  public AvroRecordWriter(Schema writerSchema, GenericData dataModel,
                          CodecFactory compressionCodec, OutputStream outputStream) throws IOException {
    this(writerSchema, dataModel, compressionCodec, outputStream,
         DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  /** {@inheritDoc} */
  @Override
  public void write(AvroKey<GenericRecord> record, NullWritable ignore) throws IOException {
    // Create an Avro container file and a writer to it.
    Schema writerSchema = record.datum().getSchema();
    if (mAvroFileWriter == null) {
      createFileWriter(writerSchema);
    }

    if (prevSchema != null && !prevSchema.equals(writerSchema)) {
      mAvroFileWriter.sync();
      mAvroFileWriter.close();
      createFileWriter(writerSchema);
    }
    mAvroFileWriter.append(record.datum());
  }

  private void createFileWriter(Schema writerSchema) throws IOException {
    mAvroFileWriter = new DataFileWriter<GenericRecord>(dataModel.createDatumWriter(writerSchema));
    mAvroFileWriter.setCodec(compressionCodec);
    mAvroFileWriter.setSyncInterval(syncInterval);
    mAvroFileWriter.create(writerSchema, outputStream);
    prevSchema = writerSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    mAvroFileWriter.close();
  }

  /** {@inheritDoc} */
  @Override
  public long sync() throws IOException {
    return mAvroFileWriter.sync();
  }
}
